package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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

func newTransactionProcessor(db worldstate.DB) *transactionProcessor {
	// TODO: make the queue size configurable
	txQueue := queue.New(100)
	txBatchQueue := queue.New(100)
	blockQueue := queue.New(100)

	go txreorderer.NewBatchCreator(txQueue, txBatchQueue).Run()
	go blockcreator.NewAssembler(txBatchQueue, blockQueue).Run()
	go blockprocessor.NewValidatorAndCommitter(blockQueue, db).Run()

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
