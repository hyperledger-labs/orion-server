package bcdb

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.ibm.com/blockchaindb/server/internal/blockcreator"
	"github.ibm.com/blockchaindb/server/internal/blockprocessor"
	"github.ibm.com/blockchaindb/server/internal/blockstore"
	"github.ibm.com/blockchaindb/server/internal/provenance"
	"github.ibm.com/blockchaindb/server/internal/queue"
	"github.ibm.com/blockchaindb/server/internal/txreorderer"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

const (
	commitListenerName = "transactionProcessor"
)

type transactionProcessor struct {
	txQueue        *queue.Queue
	txBatchQueue   *queue.Queue
	blockQueue     *queue.Queue
	txReorderer    *txreorderer.TxReorderer
	blockCreator   *blockcreator.BlockCreator
	blockProcessor *blockprocessor.BlockProcessor
	logger         *logger.SugarLogger
	sync.Mutex
}

type txProcessorConfig struct {
	db                 worldstate.DB
	blockStore         *blockstore.Store
	provenanceStore    *provenance.Store
	txQueueLength      uint32
	txBatchQueueLength uint32
	blockQueueLength   uint32
	maxTxCountPerBatch uint32
	batchTimeout       time.Duration
	logger             *logger.SugarLogger
}

func newTransactionProcessor(conf *txProcessorConfig) (*transactionProcessor, error) {
	p := &transactionProcessor{}

	p.logger = conf.logger
	p.txQueue = queue.New(conf.txQueueLength)
	p.txBatchQueue = queue.New(conf.txBatchQueueLength)
	p.blockQueue = queue.New(conf.blockQueueLength)

	p.txReorderer = txreorderer.New(
		&txreorderer.Config{
			TxQueue:            p.txQueue,
			TxBatchQueue:       p.txBatchQueue,
			MaxTxCountPerBatch: conf.maxTxCountPerBatch,
			BatchTimeout:       conf.batchTimeout,
			Logger:             conf.logger,
		},
	)

	var err error
	if p.blockCreator, err = blockcreator.New(
		&blockcreator.Config{
			TxBatchQueue: p.txBatchQueue,
			BlockQueue:   p.blockQueue,
			Logger:       conf.logger,
			BlockStore:   conf.blockStore,
		},
	); err != nil {
		return nil, err
	}

	p.blockProcessor = blockprocessor.New(
		&blockprocessor.Config{
			BlockQueue:      p.blockQueue,
			BlockStore:      conf.blockStore,
			ProvenanceStore: conf.provenanceStore,
			DB:              conf.db,
			Logger:          conf.logger,
		},
	)

	p.blockProcessor.RegisterBlockCommitListener(commitListenerName, p)

	go p.txReorderer.Start()
	p.txReorderer.WaitTillStart()
	go p.blockCreator.Start()
	p.blockCreator.WaitTillStart()
	go p.blockProcessor.Start()
	p.blockProcessor.WaitTillStart()

	return p, nil
}

// submitTransaction enqueue the transaction to the transaction queue
func (t *transactionProcessor) submitTransaction(tx interface{}) error {
	t.Lock()
	defer t.Unlock()

	if t.txQueue.IsFull() {
		return fmt.Errorf("transaction queue is full. It means the server load is high. Try after sometime")
	}

	jsonBytes, err := json.MarshalIndent(tx, "", "\t")
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %v", err)
	}
	t.logger.Debugf("enqueuing transaction %s\n", string(jsonBytes))

	t.txQueue.Enqueue(tx)
	t.logger.Debug("transaction is enqueued for re-ordering")

	return nil
}

func (t *transactionProcessor) PostBlockCommitProcessing(block *types.Block) error {
	t.logger.Debugf("received commit event for block[%d]", block.GetHeader().GetBaseHeader().GetNumber())
	// TODO: in the subsequent PR, we will process the block to facilitate the detection of
	// duplicate txID
	return nil
}

func (t *transactionProcessor) close() error {
	t.Lock()
	defer t.Unlock()

	t.txReorderer.Stop()
	t.blockCreator.Stop()
	t.blockProcessor.Stop()

	return nil
}
