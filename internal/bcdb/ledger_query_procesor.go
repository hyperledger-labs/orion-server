// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"fmt"

	"github.com/hyperledger-labs/orion-server/pkg/state"

	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	interrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/mptrie"
	"github.com/hyperledger-labs/orion-server/internal/mtree"
	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

type ledgerQueryProcessor struct {
	db              worldstate.DB
	blockStore      *blockstore.Store
	provenanceStore *provenance.Store
	trieStore       mptrie.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

type ledgerQueryProcessorConfig struct {
	db              worldstate.DB
	blockStore      *blockstore.Store
	provenanceStore *provenance.Store
	trieStore       mptrie.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

func newLedgerQueryProcessor(conf *ledgerQueryProcessorConfig) *ledgerQueryProcessor {
	return &ledgerQueryProcessor{
		db:              conf.db,
		blockStore:      conf.blockStore,
		provenanceStore: conf.provenanceStore,
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

func (p *ledgerQueryProcessor) getPath(userId string, startBlockIdx, endBlockIdx uint64) (*types.GetLedgerPathResponse, error) {
	if endBlockIdx < startBlockIdx {
		return nil, errors.Errorf("can't find path from smaller block %d to bigger %d", endBlockIdx, startBlockIdx)
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

func (p *ledgerQueryProcessor) getDataProof(userId string, blockNum uint64, dbname string, key string, isDeleted bool) (*types.GetDataProofResponse, error) {
	hasAccess, err := p.identityQuerier.HasLedgerAccess(userId)
	if err != nil {
		return nil, err
	}

	if !hasAccess {
		return nil, &interrors.PermissionErr{ErrMsg: fmt.Sprintf("user %s has no permission to access the ledger", userId)}
	}
	blockHeader, err := p.blockStore.GetHeader(blockNum)
	if err != nil {
		return nil, err
	}

	trie, err := mptrie.NewTrie(blockHeader.StateMerkelTreeRootHash, p.trieStore)
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
	txLoc, err := p.provenanceStore.GetTxIDLocation(txId)
	if err != nil {
		return nil, err
	}

	blockHeader, err := p.blockStore.GetHeader(txLoc.BlockNum)
	if err != nil {
		return nil, err
	}

	return &types.TxReceiptResponse{
		Receipt: &types.TxReceipt{
			Header:  blockHeader,
			TxIndex: uint64(txLoc.TxIndex),
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
