package bcdb

import (
	"fmt"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/internal/blockstore"
	interrors "github.ibm.com/blockchaindb/server/internal/errors"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/mtree"
	"github.ibm.com/blockchaindb/server/internal/provenance"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type ledgerQueryProcessor struct {
	nodeID          string
	signer          crypto.Signer
	db              worldstate.DB
	blockStore      *blockstore.Store
	provenanceStore *provenance.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

type ledgerQueryProcessorConfig struct {
	nodeID          string
	signer          crypto.Signer
	db              worldstate.DB
	blockStore      *blockstore.Store
	provenanceStore *provenance.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

func newLedgerQueryProcessor(conf *ledgerQueryProcessorConfig) *ledgerQueryProcessor {
	return &ledgerQueryProcessor{
		nodeID:          conf.nodeID,
		signer:          conf.signer,
		db:              conf.db,
		blockStore:      conf.blockStore,
		provenanceStore: conf.provenanceStore,
		identityQuerier: conf.identityQuerier,
		logger:          conf.logger,
	}
}

func (p *ledgerQueryProcessor) getBlockHeader(userId string, blockNum uint64) (*types.GetBlockResponseEnvelope, error) {
	hasAccess, err := p.identityQuerier.HasLedgerAccess(userId)
	if err != nil {
		return nil, err
	}

	if !hasAccess {
		return nil, &interrors.PermissionErr{ErrMsg: fmt.Sprintf("user %s doesn't has permission to access ledger", userId)}
	}
	data, err := p.blockStore.GetHeader(blockNum)
	if err != nil {
		return nil, err
	}

	result := &types.GetBlockResponseEnvelope{
		Payload: &types.GetBlockResponse{
			Header: &types.ResponseHeader{
				NodeID: p.nodeID,
			},
			BlockHeader: data,
		},
		Signature: nil,
	}

	if result.Signature, err = cryptoservice.SignQueryResponse(p.signer, result.Payload); err != nil {
		return nil, err
	}

	return result, nil
}

func (p *ledgerQueryProcessor) getPath(userId string, startBlockIdx, endBlockIdx uint64) (*types.GetLedgerPathResponseEnvelope, error) {
	if endBlockIdx < startBlockIdx {
		return nil, errors.Errorf("can't find path from smaller block %d to bigger %d", endBlockIdx, startBlockIdx)
	}

	hasAccess, err := p.identityQuerier.HasLedgerAccess(userId)
	if err != nil {
		return nil, err
	}

	if !hasAccess {
		return nil, &interrors.PermissionErr{ErrMsg: fmt.Sprintf("user %s doesn't has permission to access ledger", userId)}
	}

	endBlock, err := p.blockStore.GetHeader(endBlockIdx)
	if err != nil {
		switch err.(type)  {
		case *interrors.NotFoundErr:
			return nil, errors.Wrapf(err, "can't find path in blocks skip list between %d %d", endBlockIdx, startBlockIdx)
		default:
			return nil, err
		}
	}

	headers, err := p.findPath(endBlock, startBlockIdx)
	if err != nil {
		return nil, err
	}
	result := &types.GetLedgerPathResponseEnvelope{
		Payload: &types.GetLedgerPathResponse{
			Header: &types.ResponseHeader{
				NodeID: p.nodeID,
			},
			BlockHeaders: headers,
		},
		Signature: nil,
	}

	if result.Signature, err = cryptoservice.SignQueryResponse(p.signer, result.Payload); err != nil {
		return nil, err
	}
	return result, nil
}

func (p *ledgerQueryProcessor) getProof(userId string, blockNum uint64, txIdx uint64) (*types.GetTxProofResponseEnvelope, error) {
	hasAccess, err := p.identityQuerier.HasLedgerAccess(userId)
	if err != nil {
		return nil, err
	}

	if !hasAccess {
		return nil, &interrors.PermissionErr{ErrMsg: fmt.Sprintf("user %s doesn't has permission to access ledger", userId)}
	}
	block, err := p.blockStore.Get(blockNum)
	if err != nil {
		return nil, err
	}

	path, err := p.calculateProof(block, txIdx)
	if err != nil {
		return nil, err
	}
	result := &types.GetTxProofResponseEnvelope{
		Payload: &types.GetTxProofResponse{
			Header: &types.ResponseHeader{
				NodeID: p.nodeID,
			},
			Hashes: path,
		},
		Signature: nil,
	}

	if result.Signature, err = cryptoservice.SignQueryResponse(p.signer, result.Payload); err != nil {
		return nil, err
	}
	return result, nil
}

func (p *ledgerQueryProcessor) getTxReceipt(userId string, txId string) (*types.GetTxReceiptResponseEnvelope, error) {
	hasAccess, err := p.identityQuerier.HasLedgerAccess(userId)
	if err != nil {
		return nil, err
	}

	if !hasAccess {
		return nil, &interrors.PermissionErr{fmt.Sprintf("user %s doesn't has permission to access ledger", userId)}
	}
	txLoc, err := p.provenanceStore.GetTxIDLocation(txId)
	if err != nil {
		return nil, err
	}

	blockHeader, err := p.blockStore.GetHeader(txLoc.BlockNum)
	if err != nil {
		return nil, err
	}

	result := &types.GetTxReceiptResponseEnvelope{
		Payload: &types.GetTxReceiptResponse{
			Header: &types.ResponseHeader{
				NodeID: p.nodeID,
			},
			Receipt: &types.TxReceipt{
				Header:  blockHeader,
				TxIndex: uint64(txLoc.TxIndex),
			},
		},
		Signature: nil,
	}

	if result.Signature, err = cryptoservice.SignQueryResponse(p.signer, result.Payload); err != nil {
		return nil, err
	}
	return result, nil
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
