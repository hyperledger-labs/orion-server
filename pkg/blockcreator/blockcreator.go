package blockcreator

import (
	"log"

	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

// BlockCreator uses transactions batch queue to construct the
// block and stores the created block in the block queue
type BlockCreator struct {
	txBatchQueue    *queue.Queue
	blockQueue      *queue.Queue
	lastBlockNumber uint64
}

// Config holds the configuration information required to initialize the
// block creator
type Config struct {
	TxBatchQueue    *queue.Queue
	BlockQueue      *queue.Queue
	LastBlockNumber uint64
}

// New creates a new block assembler
func New(conf *Config) *BlockCreator {
	return &BlockCreator{
		txBatchQueue:    conf.TxBatchQueue,
		blockQueue:      conf.BlockQueue,
		lastBlockNumber: conf.LastBlockNumber,
	}
}

// Run runs the block assembler in an infinte loop
func (b *BlockCreator) Run() {
	for {
		log.Printf("waiting for the block")
		txBatch := b.txBatchQueue.Dequeue().([]*types.TransactionEnvelope)

		block := &types.Block{
			Header: &types.BlockHeader{
				Number: b.lastBlockNumber + 1,
			},
			TransactionEnvelopes: txBatch,
		}

		log.Printf("created block %d with %d transactions\n", b.lastBlockNumber, len(txBatch))
		b.blockQueue.Enqueue(block)
		b.lastBlockNumber++
	}
}
