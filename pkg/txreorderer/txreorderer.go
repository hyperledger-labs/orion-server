package txreorderer

import (
	"log"

	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

var (
	// TODO: Need to make the txBatchSize or blockSize configurable
	txBatchSize = 1
)

// BatchCreator holds queue and other components needed to reorder
// transactions before creating a next batch of transactions to be
// included in the block
type BatchCreator struct {
	txQueue      *queue.Queue
	txBatchQueue *queue.Queue
	// TODO:
	// tx merkle tree
	// dependency graph
	// early abort and reorder
}

// NewBatchCreator creates a BatchCreator
func NewBatchCreator(txQueue, txBatchQueue *queue.Queue) *BatchCreator {
	return &BatchCreator{
		txQueue:      txQueue,
		txBatchQueue: txBatchQueue,
	}
}

// Run runs the transactions batch creator
func (b *BatchCreator) Run() {
	var txBatch []*types.TransactionEnvelope
	for {
		tx := b.txQueue.Dequeue().(*types.TransactionEnvelope)
		txBatch = append(txBatch, tx)
		if len(txBatch) < txBatchSize {
			continue
		}

		b.txBatchQueue.Enqueue(txBatch)
		log.Println("created a transaction batch")
		txBatch = nil
	}
}
