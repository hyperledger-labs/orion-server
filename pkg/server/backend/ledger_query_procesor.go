package backend

import (
	"fmt"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
	"github.ibm.com/blockchaindb/server/pkg/common/logger"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type ledgerQueryProcessor struct {
	nodeID          string
	db              worldstate.DB
	blockStore      *blockstore.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

type ledgerQueryProcessorConfig struct {
	nodeID          string
	db              worldstate.DB
	blockStore      *blockstore.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

func newLedgerQueryProcessor(conf *ledgerQueryProcessorConfig) *ledgerQueryProcessor {
	return &ledgerQueryProcessor{
		nodeID:          conf.nodeID,
		db:              conf.db,
		blockStore:      conf.blockStore,
		identityQuerier: conf.identityQuerier,
	}
}

func (p *ledgerQueryProcessor) getBlockHeader(userId string, blockNum uint64) (*types.GetBlockResponseEnvelope, error) {
	hasAccess, err := p.identityQuerier.HasLedgerAccess(userId)
	if err != nil {
		return nil, err
	}

	if !hasAccess {
		return nil, &PermissionErr{fmt.Sprintf("user %s doesn't has permision to access ledger", userId)}
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

	if result.Signature, err = cryptoservice.Sign(result.Payload); err != nil {
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
		return nil, &PermissionErr{fmt.Sprintf("user %s doesn't has permision to access ledger", userId)}
	}

	endBlock, err := p.blockStore.GetHeader(endBlockIdx)
	if err != nil {
		return nil, err
	}

	if endBlock == nil {
		return nil, errors.Errorf("can't find path in blocks skip list between %d %d, end block not exist", endBlockIdx, startBlockIdx)
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

	if result.Signature, err = cryptoservice.Sign(result.Payload); err != nil {
		return nil, err
	}
	return result, nil
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
