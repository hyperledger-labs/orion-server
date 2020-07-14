package server

import (
	"context"
	"fmt"

	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

type transactionProcessor struct {
	txQueue *queue.Queue
}

func newTransactionProcessor() (*transactionProcessor, error) {
	return &transactionProcessor{
		// TODO: make the queue size configurable
		txQueue: queue.NewQueue(2000),
	}, nil
}

func (tp *transactionProcessor) SubmitTransaction(ctx context.Context, tx *api.TransactionEnvelope) error {
	if tp.txQueue.IsFull() {
		return fmt.Errorf("transaction queue is full. It means the server load is high. Try after sometime")
	}
	tp.txQueue.Enqueue(tx)
	return nil
}
