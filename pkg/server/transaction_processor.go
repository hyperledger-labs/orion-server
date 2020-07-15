package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/blockcreator"
	"github.ibm.com/blockchaindb/server/pkg/blockprocessor"
	"github.ibm.com/blockchaindb/server/pkg/queue"
	"github.ibm.com/blockchaindb/server/pkg/txreorderer"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type transactionProcessor struct {
	txQueue        *queue.Queue
	txBatchCreator *txreorderer.BatchCreator
	blockCreator   *blockcreator.Assembler
	blockProcessor *blockprocessor.ValidatorAndCommitter
}

func newTransactionProcessor(db worldstate.DB) *transactionProcessor {
	// TODO: make the queue size configurable
	txQueue := queue.NewQueue(100)
	txBatchQueue := queue.NewQueue(100)
	blockQueue := queue.NewQueue(100)

	t := &transactionProcessor{
		txQueue:        txQueue,
		txBatchCreator: txreorderer.NewBatchCreator(txQueue, txBatchQueue),
		blockCreator:   blockcreator.NewAssembler(txBatchQueue, blockQueue),
		blockProcessor: blockprocessor.NewValidatorAndCommitter(blockQueue, db),
	}

	go t.txBatchCreator.Run()
	go t.blockCreator.Run()
	go t.blockProcessor.Run()

	return t
}

func (t *transactionProcessor) SubmitTransaction(ctx context.Context, tx *api.TransactionEnvelope) error {
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
