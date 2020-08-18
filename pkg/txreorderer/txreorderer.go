package txreorderer

import (
	"log"
	"time"

	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

// TxReorderer holds queue and other components needed to reorder
// transactions before creating a next batch of transactions to be
// included in the block
type TxReorderer struct {
	txQueue            *queue.Queue
	txBatchQueue       *queue.Queue
	MaxTxCountPerBatch uint32
	batchTimeout       time.Duration
	// TODO:
	// tx merkle tree
	// dependency graph
	// early abort and reorder
}

// Config holds the configuration information need to start the transaction
// reorderer
type Config struct {
	TxQueue            *queue.Queue
	TxBatchQueue       *queue.Queue
	MaxTxCountPerBatch uint32
	BatchTimeout       time.Duration
}

// New creates a transaction reorderer
func New(conf *Config) *TxReorderer {
	return &TxReorderer{
		txQueue:            conf.TxQueue,
		txBatchQueue:       conf.TxBatchQueue,
		MaxTxCountPerBatch: conf.MaxTxCountPerBatch,
		batchTimeout:       conf.BatchTimeout,
	}
}

// Run runs the transactions batch creator
func (b *TxReorderer) Run() {
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
		if uint32(len(txBatch)) < b.MaxTxCountPerBatch {
			continue
		}

		log.Printf("added %d transactions to the tx batch queue", len(txBatch))
		b.enqueueTxBatch(txBatch)
		txBatch = nil
	}
}

func (b *TxReorderer) enqueueTxBatch(txBatch []*types.TransactionEnvelope) {
	if len(txBatch) == 0 {
		return
	}
	b.txBatchQueue.Enqueue(txBatch)
	log.Printf("created a transaction batch with %d transactions", len(txBatch))
}
