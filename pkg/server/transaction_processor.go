package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.ibm.com/blockchaindb/server/pkg/blockcreator"
	"github.ibm.com/blockchaindb/server/pkg/blockprocessor"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
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
	sync.Mutex
}

type txProcessorConfig struct {
	db                 worldstate.DB
	blockStore         *blockstore.Store
	blockHeight        uint64
	txQueueLength      uint32
	txBatchQueueLength uint32
	blockQueueLength   uint32
	maxTxCountPerBatch uint32
	batchTimeout       time.Duration
}

func newTransactionProcessor(conf *txProcessorConfig) *transactionProcessor {
	p := &transactionProcessor{}

	p.txQueue = queue.New(conf.txQueueLength)
	p.txBatchQueue = queue.New(conf.txBatchQueueLength)
	p.blockQueue = queue.New(conf.blockQueueLength)

	p.txReorderer = txreorderer.New(
		&txreorderer.Config{
			TxQueue:            p.txQueue,
			TxBatchQueue:       p.txBatchQueue,
			MaxTxCountPerBatch: conf.maxTxCountPerBatch,
			BatchTimeout:       conf.batchTimeout,
		},
	)

	p.blockCreator = blockcreator.New(
		&blockcreator.Config{
			TxBatchQueue:    p.txBatchQueue,
			BlockQueue:      p.blockQueue,
			NextBlockNumber: conf.blockHeight + 1,
		},
	)

	p.blockProcessor = blockprocessor.New(
		&blockprocessor.Config{
			BlockQueue: p.blockQueue,
			BlockStore: conf.blockStore,
			DB:         conf.db,
		},
	)

	go p.txReorderer.Run()
	go p.blockCreator.Run()
	go p.blockProcessor.Run()

	return p
}

// submitTransaction enqueue the transaction to the transaction queue
func (t *transactionProcessor) submitTransaction(_ context.Context, tx interface{}) error {
	t.Lock()
	defer t.Unlock()

	if t.txQueue.IsFull() {
		return fmt.Errorf("transaction queue is full. It means the server load is high. Try after sometime")
	}

	jsonBytes, err := json.MarshalIndent(tx, "", "\t")
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %v", err)
	}
	log.Printf("enqueing transaction %s\n", string(jsonBytes))

	t.txQueue.Enqueue(tx)

	return nil
}

func (t *transactionProcessor) close() error {
	// TODO: need to signal goroutine to stop
	return nil
}
