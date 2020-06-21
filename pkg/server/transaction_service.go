package server

import (
	"fmt"

	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

type TransactionServer struct {
	txQueue *queue.Queue
}

func newTransactionServer() (*TransactionServer, error) {
	return &TransactionServer{
		// TODO: make the queue size configurable
		txQueue: queue.NewQueue(2000),
	}, nil
}

func (ts *TransactionServer) SubmitTransaction(srv api.TransactionSvc_SubmitTransactionServer) error {
	tx, err := srv.Recv()
	if err != nil {
		return fmt.Errorf("unable to recevie the transaction envelope")
	}
	if ts.txQueue.IsFull() {
		return fmt.Errorf("transaction queue is full. It means the server load is high. Try after sometime")
	}
	ts.txQueue.Enqueue(tx)
	return nil
}
