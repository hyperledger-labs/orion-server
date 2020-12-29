package blockcreator

import (
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/internal/blockstore"
	"github.ibm.com/blockchaindb/server/internal/queue"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// BlockCreator uses transactions batch queue to construct the
// block and stores the created block in the block queue
type BlockCreator struct {
	txBatchQueue                *queue.Queue
	blockQueue                  *queue.Queue
	nextBlockNumber             uint64
	previousBlockHeaderBaseHash []byte
	blockStore                  *blockstore.Store
	started                     chan struct{}
	stop                        chan struct{}
	stopped                     chan struct{}
	logger                      *logger.SugarLogger
}

// Config holds the configuration information required to initialize the
// block creator
type Config struct {
	TxBatchQueue *queue.Queue
	BlockQueue   *queue.Queue
	BlockStore   *blockstore.Store
	Logger       *logger.SugarLogger
}

// New creates a new block assembler
func New(conf *Config) (*BlockCreator, error) {
	height, err := conf.BlockStore.Height()
	if err != nil {
		return nil, err
	}

	lastBlockBaseHash, err := conf.BlockStore.GetBaseHeaderHash(height)
	if err != nil {
		return nil, err
	}
	return &BlockCreator{
		txBatchQueue:                conf.TxBatchQueue,
		blockQueue:                  conf.BlockQueue,
		nextBlockNumber:             height + 1,
		logger:                      conf.Logger,
		blockStore:                  conf.BlockStore,
		started:                     make(chan struct{}),
		stop:                        make(chan struct{}),
		stopped:                     make(chan struct{}),
		previousBlockHeaderBaseHash: lastBlockBaseHash,
	}, nil
}

// Start runs the block assembler in an infinite loop
func (b *BlockCreator) Start() {
	defer close(b.stopped)

	b.logger.Info("starting the block creator")
	close(b.started)
	for {
		select {
		case <-b.stop:
			b.logger.Info("stopping the block creator")
			return

		default:
			txBatch := b.txBatchQueue.Dequeue()
			if txBatch == nil {
				// when the queue is closed during the teardown/cleanup,
				// the dequeued txBatch would be nil.
				continue
			}

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

			switch batch := txBatch.(type) {
			case *types.Block_DataTxEnvelopes:
				block.Payload = batch
				b.logger.Debugf("created block %d with %d data transactions\n",
					blkNum,
					len(batch.DataTxEnvelopes.Envelopes),
				)

			case *types.Block_UserAdministrationTxEnvelope:
				block.Payload = batch
				b.logger.Debugf("created block %d with an user administrative transaction", blkNum)

			case *types.Block_ConfigTxEnvelope:
				block.Payload = batch
				b.logger.Debugf("created block %d with a cluster config administrative transaction", blkNum)

			case *types.Block_DBAdministrationTxEnvelope:
				block.Payload = batch
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
}

// WaitTillStart waits till the block creator is started
func (b *BlockCreator) WaitTillStart() {
	<-b.started
}

// Stop stops the block creator
func (b *BlockCreator) Stop() {
	b.txBatchQueue.Close()
	close(b.stop)
	<-b.stopped
}

func (b *BlockCreator) createBaseHeader(blockNum uint64) (*types.BlockHeaderBase, error) {
	baseHeader := &types.BlockHeaderBase{
		Number:                 blockNum,
		PreviousBaseHeaderHash: b.previousBlockHeaderBaseHash,
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
