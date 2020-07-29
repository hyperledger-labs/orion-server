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

		if tx.Payload.Type == types.Transaction_CONFIG {
			b.enqueueTxBatch(txBatch)
			b.enqueueTxBatch([]*types.TransactionEnvelope{tx})
			txBatch = nil
			continue
		}

		txBatch = append(txBatch, tx)
		if len(txBatch) < txBatchSize {
			continue
		}

		b.enqueueTxBatch(txBatch)
		txBatch = nil
	}
}

func (b *BatchCreator) enqueueTxBatch(txBatch []*types.TransactionEnvelope) {
	if len(txBatch) == 0 {
		return
	}
	b.txBatchQueue.Enqueue(txBatch)
	log.Printf("created a transaction batch with %d transactions", len(txBatch))
}
