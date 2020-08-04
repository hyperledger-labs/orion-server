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
	txQueue *queue.Queue
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
	txQueue := queue.New(conf.txQueueLength)
	txBatchQueue := queue.New(conf.txBatchQueueLength)
	blockQueue := queue.New(conf.blockQueueLength)

	go txreorderer.New(txQueue, txBatchQueue, conf.MaxTxCountPerBatch, conf.batchTimeout).Run()
	go blockcreator.New(txBatchQueue, blockQueue).Run()
	go blockprocessor.New(blockQueue, conf.db).Run()

	return &transactionProcessor{
		txQueue: txQueue,
	}
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
