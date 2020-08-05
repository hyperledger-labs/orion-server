package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/blockcreator"
	"github.ibm.com/blockchaindb/server/pkg/blockprocessor"
	"github.ibm.com/blockchaindb/server/pkg/queue"
	"github.ibm.com/blockchaindb/server/pkg/txreorderer"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type transactionProcessor struct {
	txQueue        *queue.Queue
	txBatchQueue   *queue.Queue
	blockQueue     *queue.Queue
	txReorderer    *txreorderer.TxReorderer
	blockCreator   *blockcreator.BlockCreator
	blockProcessor *blockprocessor.BlockProcessor
}

type txProcessorConfig struct {
	db                 worldstate.DB
	txQueueLength      uint32
	txBatchQueueLength uint32
	blockQueueLength   uint32
	MaxTxCountPerBatch uint32
	batchTimeout       time.Duration
}

func newTransactionProcessor(conf *txProcessorConfig) *transactionProcessor {
	p := &transactionProcessor{}

	p.txQueue = queue.New(conf.txQueueLength)
	p.txBatchQueue = queue.New(conf.txBatchQueueLength)
	p.blockQueue = queue.New(conf.blockQueueLength)

	p.txReorderer = txreorderer.New(p.txQueue, p.txBatchQueue, conf.MaxTxCountPerBatch, conf.batchTimeout)
	p.blockCreator = blockcreator.New(p.txBatchQueue, p.blockQueue)
	p.blockProcessor = blockprocessor.New(p.blockQueue, conf.db)

	go p.txReorderer.Run()
	go p.blockCreator.Run()
	go p.blockProcessor.Run()

	return p
}

// SubmitTransaction enqueue the transaction to the transaction queue
func (t *transactionProcessor) SubmitTransaction(_ context.Context, tx *types.TransactionEnvelope) error {
	if t.txQueue.IsFull() {
		return fmt.Errorf("transaction queue is full. It means the server load is high. Try after sometime")
	}

	t.txQueue.Enqueue(tx)
	jsonBytes, err := json.MarshalIndent(tx, "", "\t")
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %v", err)
	}

	log.Printf("enqueued transaction %s\n", string(jsonBytes))
	return nil
}
