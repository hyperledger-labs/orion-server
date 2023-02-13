// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"fmt"

	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	interrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/mptrie"
	"github.com/hyperledger-labs/orion-server/internal/mtree"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/marshal"
	"github.com/hyperledger-labs/orion-server/pkg/state"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

type ledgerQueryProcessor struct {
	db              worldstate.DB
	blockStore      *blockstore.Store
	trieStore       mptrie.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

type ledgerQueryProcessorConfig struct {
	db              worldstate.DB
	blockStore      *blockstore.Store
	trieStore       mptrie.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

func newLedgerQueryProcessor(conf *ledgerQueryProcessorConfig) *ledgerQueryProcessor {
	return &ledgerQueryProcessor{
		db:              conf.db,
		blockStore:      conf.blockStore,
		trieStore:       conf.trieStore,
		identityQuerier: conf.identityQuerier,
		logger:          conf.logger,
	}
}

func (p *ledgerQueryProcessor) getBlockHeader(userId string, blockNum uint64) (*types.GetBlockResponse, error) {
	hasAccess, err := p.identityQuerier.HasLedgerAccess(userId)
	if err != nil {
		return nil, err
	}

	if !hasAccess {
		return nil, &interrors.PermissionErr{ErrMsg: fmt.Sprintf("user %s has no permission to access the ledger", userId)}
	}
	data, err := p.blockStore.GetHeader(blockNum)
	if err != nil {
		return nil, err
	}

	return &types.GetBlockResponse{
		BlockHeader: data,
	}, nil
}

func (p *ledgerQueryProcessor) getAugmentedBlockHeader(userId string, blockNum uint64) (*types.GetAugmentedBlockHeaderResponse, error) {
	hasAccess, err := p.identityQuerier.HasLedgerAccess(userId)
	if err != nil {
		return nil, err
	}

	if !hasAccess {
		return nil, &interrors.PermissionErr{ErrMsg: fmt.Sprintf("user %s has no permission to access the ledger", userId)}
	}
	data, err := p.blockStore.GetAugmentedHeader(blockNum)
	if err != nil {
		return nil, err
	}

	return &types.GetAugmentedBlockHeaderResponse{
		BlockHeader: data,
	}, nil
}

func (p *ledgerQueryProcessor) getPath(userId string, startBlockIdx, endBlockIdx uint64) (*types.GetLedgerPathResponse, error) {
	if startBlockIdx < 1 {
		return nil, &interrors.BadRequestError{ErrMsg: "start block number must be >=1"}
	}

	if endBlockIdx < startBlockIdx {
		return nil, &interrors.BadRequestError{ErrMsg: fmt.Sprintf("can't find path from start block %d to end block %d, start must be <= end", startBlockIdx, endBlockIdx)}
	}

	hasAccess, err := p.identityQuerier.HasLedgerAccess(userId)
	if err != nil {
		return nil, err
	}

	if !hasAccess {
		return nil, &interrors.PermissionErr{ErrMsg: fmt.Sprintf("user %s has no permission to access the ledger", userId)}
	}

	endBlock, err := p.blockStore.GetHeader(endBlockIdx)
	if err != nil {
		switch e := err.(type) {
		case *interrors.NotFoundErr:
			e.Message = fmt.Sprintf("can't find path in blocks skip list between %d %d: %s", endBlockIdx, startBlockIdx, e.Message)
			return nil, e
		default:
			return nil, err
		}
	}

	headers, err := p.findPath(endBlock, startBlockIdx)
	if err != nil {
		return nil, err
	}
	return &types.GetLedgerPathResponse{
		BlockHeaders: headers,
	}, nil
}

func (p *ledgerQueryProcessor) getTxProof(userId string, blockNum uint64, txIdx uint64) (*types.GetTxProofResponse, error) {
	hasAccess, err := p.identityQuerier.HasLedgerAccess(userId)
	if err != nil {
		return nil, err
	}

	if !hasAccess {
		return nil, &interrors.PermissionErr{ErrMsg: fmt.Sprintf("user %s has no permission to access the ledger", userId)}
	}
	block, err := p.blockStore.Get(blockNum)
	if err != nil {
		return nil, err
	}

	path, err := p.calculateProof(block, txIdx)
	if err != nil {
		return nil, err
	}
	return &types.GetTxProofResponse{
		Hashes: path,
	}, nil
}

// getTx allows a user to get a TX
func (p *ledgerQueryProcessor) getTx(userId string, blockNum uint64, txIndex uint64) (*types.GetTxResponse, error) {
	block, err := p.blockStore.Get(blockNum)
	if err != nil {
		return nil, err
	}

	response := &types.GetTxResponse{
		Version: &types.Version{
			BlockNum: blockNum,
			TxNum:    txIndex,
		},
	}

	switch block.Payload.(type) {
	case *types.Block_DataTxEnvelopes:
		dataTxEnvs := block.GetDataTxEnvelopes().Envelopes
		if int(txIndex) >= len(dataTxEnvs) {
			return nil, &interrors.BadRequestError{ErrMsg: fmt.Sprintf("transaction index out of range: %d", txIndex)}
		}
		dataTxEnv := dataTxEnvs[txIndex]
		hasAccess, err := p.hasDataTxAccess(userId, dataTxEnv)
		if err != nil {
			return nil, err
		}
		if !hasAccess {
			return nil, &interrors.PermissionErr{ErrMsg: fmt.Sprintf("user %s has no permission to access the tx", userId)}
		}
		response.TxEnvelope = &types.GetTxResponse_DataTxEnvelope{
			DataTxEnvelope: dataTxEnv,
		}

	case *types.Block_UserAdministrationTxEnvelope:
		if err := p.checkAdminTxAccess(userId, txIndex); err != nil {
			return nil, err
		}
		response.TxEnvelope = &types.GetTxResponse_UserAdministrationTxEnvelope{
			UserAdministrationTxEnvelope: block.GetUserAdministrationTxEnvelope(),
		}

	case *types.Block_DbAdministrationTxEnvelope:
		if err := p.checkAdminTxAccess(userId, txIndex); err != nil {
			return nil, err
		}
		response.TxEnvelope = &types.GetTxResponse_DbAdministrationTxEnvelope{
			DbAdministrationTxEnvelope: block.GetDbAdministrationTxEnvelope(),
		}

	case *types.Block_ConfigTxEnvelope:
		if err := p.checkAdminTxAccess(userId, txIndex); err != nil {
			return nil, err
		}
		response.TxEnvelope = &types.GetTxResponse_ConfigTxEnvelope{
			ConfigTxEnvelope: block.GetConfigTxEnvelope(),
		}

	default:
		return nil, errors.Errorf("unexpected transaction envelope in the block")
	}

	response.ValidationInfo = block.Header.ValidationInfo[txIndex]

	return response, nil

}

// either the user is in the must-sign set, or it has signed the TX correctly
func (p *ledgerQueryProcessor) hasDataTxAccess(userId string, env *types.DataTxEnvelope) (bool, error) {
	dataTx := env.GetPayload()

	for _, mustSignUser := range dataTx.GetMustSignUserIds() {
		if mustSignUser == userId { // must-sign user always have valid sig, no need to check
			return true, nil
		}
	}

	for signedUser, sig := range env.GetSignatures() {
		if userId != signedUser {
			continue
		}

		dataTxBytes, err := marshal.DefaultMarshaller().Marshal(dataTx)
		if err != nil {
			p.logger.Errorf("Error during Marshal Tx: %s, error: %s", dataTx, err)
			return false, errors.Wrap(err, "failed to Marshal Tx")
		}
		// TODO This may fail if the user changed his certificate after signing the TX - use certificate history
		sigVerifier := cryptoservice.NewVerifier(p.identityQuerier, p.logger)
		if err = sigVerifier.Verify(userId, sig, dataTxBytes); err == nil {
			return true, nil
		}
	}

	return false, nil
}

func (p *ledgerQueryProcessor) checkAdminTxAccess(userId string, txIndex uint64) error {
	if txIndex > 0 {
		return &interrors.BadRequestError{ErrMsg: fmt.Sprintf("transaction index out of range: %d", txIndex)}
	}

	hasAccess, err := p.identityQuerier.HasAdministrationPrivilege(userId)
	if err != nil {
		return err
	}
	if !hasAccess {
		return &interrors.PermissionErr{ErrMsg: fmt.Sprintf("user %s has no permission to access the tx", userId)}
	}

	return nil
}

func (p *ledgerQueryProcessor) getDataProof(userId string, blockNum uint64, dbname string, key string, isDeleted bool) (*types.GetDataProofResponse, error) {
	hasAccess, err := p.identityQuerier.HasLedgerAccess(userId)
	if err != nil {
		return nil, err
	}

	if !hasAccess {
		return nil, &interrors.PermissionErr{ErrMsg: fmt.Sprintf("user %s has no permission to access the ledger", userId)}
	}

	if p.trieStore.IsDisabled() {
		return nil, &interrors.ServerRestrictionError{ErrMsg: "State Merkle Patricia Trie is disabled"}
	}

	blockHeader, err := p.blockStore.GetHeader(blockNum)
	if err != nil {
		return nil, err
	}

	trie, err := mptrie.NewTrie(blockHeader.StateMerkleTreeRootHash, p.trieStore)
	if err != nil {
		return nil, err
	}
	trieKey, err := state.ConstructCompositeKey(dbname, key)
	if err != nil {
		return nil, err
	}

	proof, err := trie.GetProof(trieKey, isDeleted)
	if err != nil {
		return nil, err
	}

	if proof == nil {
		return nil, &interrors.NotFoundErr{Message: fmt.Sprintf("no proof for block %d, db %s, key %s, isDeleted %t found", blockNum, dbname, key, isDeleted)}
	}

	resp := &types.GetDataProofResponse{
		Path: proof.GetPath(),
	}

	return resp, nil
}

func (p *ledgerQueryProcessor) getTxReceipt(userId string, txId string) (*types.TxReceiptResponse, error) {
	hasAccess, err := p.identityQuerier.HasLedgerAccess(userId)
	if err != nil {
		return nil, err
	}

	if !hasAccess {
		return nil, &interrors.PermissionErr{ErrMsg: fmt.Sprintf("user %s has no permission to access the ledger", userId)}
	}

	txInfo, err := p.blockStore.GetTxInfo(txId)
	if err != nil {
		return nil, err
	}

	blockHeader, err := p.blockStore.GetHeader(txInfo.GetBlockNumber())
	if err != nil {
		return nil, err
	}

	return &types.TxReceiptResponse{
		Receipt: &types.TxReceipt{
			Header:  blockHeader,
			TxIndex: txInfo.GetTxIndex(),
		},
	}, nil
}

func (p *ledgerQueryProcessor) calculateProof(block *types.Block, txIdx uint64) ([][]byte, error) {
	root, err := mtree.BuildTreeForBlockTx(block)
	if err != nil {
		return nil, err
	}
	path, err := root.Proof(int(txIdx))
	if err != nil {
		return nil, err
	}
	return path, nil
}

func (p *ledgerQueryProcessor) findPath(endBlock *types.BlockHeader, startIndex uint64) ([]*types.BlockHeader, error) {
	headers := make([]*types.BlockHeader, 0)
	headers = append(headers, endBlock)
	if endBlock.GetBaseHeader().GetNumber() == startIndex {
		return headers, nil
	}
	for currentBlock := endBlock; currentBlock.GetBaseHeader().GetNumber() > startIndex; {
		blockSkipIndexes := blockstore.CalculateSkipListLinks(currentBlock.GetBaseHeader().GetNumber())
		for i := len(blockSkipIndexes) - 1; i >= 0; i-- {
			if blockSkipIndexes[i] >= startIndex {
				var err error
				currentBlock, err = p.blockStore.GetHeader(blockSkipIndexes[i])
				if err != nil {
					return nil, err
				}
				headers = append(headers, currentBlock)
				if blockSkipIndexes[i] > startIndex {
					break
				}

				return headers, nil
			}
		}
	}
	return nil, errors.Errorf("can't find path in blocks skip list between %d %d", endBlock.GetBaseHeader().GetNumber(), startIndex)
}
