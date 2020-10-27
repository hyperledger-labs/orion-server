package blockcreator

import (
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

// BlockCreator uses transactions batch queue to construct the
// block and stores the created block in the block queue
type BlockCreator struct {
	txBatchQueue                *queue.Queue
	blockQueue                  *queue.Queue
	nextBlockNumber             uint64
	previousBlockHeaderBaseHash []byte
	blockStore                  *blockstore.Store
	logger                      *logger.SugarLogger
}

// Config holds the configuration information required to initialize the
// block creator
type Config struct {
	TxBatchQueue    *queue.Queue
	BlockQueue      *queue.Queue
	NextBlockNumber uint64
	BlockStore      *blockstore.Store
	Logger          *logger.SugarLogger
}

// New creates a new block assembler
func New(conf *Config) (*BlockCreator, error) {
	lastBlockBaseHash, err := conf.BlockStore.GetBaseHeaderHash(conf.NextBlockNumber - 1)
	if err != nil {
		return nil, err
	}
	return &BlockCreator{
		txBatchQueue:                conf.TxBatchQueue,
		blockQueue:                  conf.BlockQueue,
		nextBlockNumber:             conf.NextBlockNumber,
		logger:                      conf.Logger,
		blockStore:                  conf.BlockStore,
		previousBlockHeaderBaseHash: lastBlockBaseHash,
	}, nil
}

// Run runs the block assembler in an infinite loop
func (b *BlockCreator) Run() {
	for {
		txBatch := b.txBatchQueue.Dequeue()

		blkNum := b.nextBlockNumber

		baseHeader, err := b.createBaseHeader(blkNum)
		if err != nil {
			b.logger.Panicf("Error while filling block header, possible problems with block indexes {%v}", err)
		}
		block := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: baseHeader,
			},
		}

		switch txBatch.(type) {
		case *types.Block_DataTxEnvelopes:
			block.Payload = txBatch.(*types.Block_DataTxEnvelopes)
			b.logger.Debugf("created block %d with %d data transactions\n",
				blkNum,
				len(txBatch.(*types.Block_DataTxEnvelopes).DataTxEnvelopes.Envelopes),
			)

		case *types.Block_UserAdministrationTxEnvelope:
			block.Payload = txBatch.(*types.Block_UserAdministrationTxEnvelope)
			b.logger.Debugf("created block %d with an user administrative transaction", blkNum)

		case *types.Block_ConfigTxEnvelope:
			block.Payload = txBatch.(*types.Block_ConfigTxEnvelope)
			b.logger.Debugf("created block %d with a cluster config administrative transaction", blkNum)

		case *types.Block_DBAdministrationTxEnvelope:
			block.Payload = txBatch.(*types.Block_DBAdministrationTxEnvelope)
			b.logger.Debugf("created block %d with a DB administrative transaction", blkNum)
		}

		b.blockQueue.Enqueue(block)
		h, err := blockstore.ComputeBlockBaseHash(block)
		if err != nil {
			b.logger.Panicf("Error calculating block hash {%v}", err)
		}
		b.previousBlockHeaderBaseHash = h
		b.nextBlockNumber++
	}
}

func (b *BlockCreator) createBaseHeader(blockNum uint64) (*types.BlockHeaderBase, error) {
	baseHeader := &types.BlockHeaderBase{
			Number:                 blockNum,
			PreviousBaseHeaderHash: b.previousBlockHeaderBaseHash,
			TxMerkelTreeRootHash:   nil,
		}

	if blockNum > 1 {
		lastCommittedBlockNum, err := b.blockStore.Height()
		if err != nil {
			return nil, err
		}
		lastCommittedBlockHash, err := b.blockStore.GetHash(lastCommittedBlockNum)
		if err != nil {
			return nil, err
		}
		if lastCommittedBlockHash == nil && !(lastCommittedBlockNum == 0) {
			return nil, errors.Errorf("can't get hash of last committed block {%d}", lastCommittedBlockNum)
		}
		baseHeader.LastCommittedBlockHash = lastCommittedBlockHash
		baseHeader.LastCommittedBlockNum = lastCommittedBlockNum
	}
	return baseHeader, nil
}
