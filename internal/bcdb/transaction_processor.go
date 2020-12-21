package bcdb

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
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
	blockStore     *blockstore.Store
	pendingTxs     *pendingTxs
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

	p.pendingTxs = &pendingTxs{
		txIDs: make(map[string]bool),
	}
	p.blockStore = conf.blockStore

	return p, nil
}

// submitTransaction enqueue the transaction to the transaction queue
func (t *transactionProcessor) submitTransaction(tx interface{}) error {
	var txID string
	switch tx.(type) {
	case *types.DataTxEnvelope:
		txID = tx.(*types.DataTxEnvelope).Payload.TxID
	case *types.UserAdministrationTxEnvelope:
		txID = tx.(*types.UserAdministrationTxEnvelope).Payload.TxID
	case *types.DBAdministrationTxEnvelope:
		txID = tx.(*types.DBAdministrationTxEnvelope).Payload.TxID
	case *types.ConfigTxEnvelope:
		txID = tx.(*types.ConfigTxEnvelope).Payload.TxID
	default:
		return errors.Errorf("unexpected transaction type")
	}

	t.Lock()
	defer t.Unlock()

	duplicate, err := t.isTxIDDuplicate(txID)
	if err != nil {
		return err
	}
	if duplicate {
		return &DuplicateTxIDError{txID}
	}

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

	t.pendingTxs.add(txID)

	return nil
}

func (t *transactionProcessor) PostBlockCommitProcessing(block *types.Block) error {
	t.logger.Debugf("received commit event for block[%d]", block.GetHeader().GetBaseHeader().GetNumber())

	var txIDs []string

	switch block.Payload.(type) {
	case *types.Block_DataTxEnvelopes:
		dataTxEnvs := block.GetDataTxEnvelopes().Envelopes
		for _, tx := range dataTxEnvs {
			txIDs = append(txIDs, tx.Payload.TxID)
		}

	case *types.Block_UserAdministrationTxEnvelope:
		userTxEnv := block.GetUserAdministrationTxEnvelope()
		txIDs = append(txIDs, userTxEnv.Payload.TxID)

	case *types.Block_DBAdministrationTxEnvelope:
		dbTxEnv := block.GetDBAdministrationTxEnvelope()
		txIDs = append(txIDs, dbTxEnv.Payload.TxID)

	case *types.Block_ConfigTxEnvelope:
		configTxEnv := block.GetConfigTxEnvelope()
		txIDs = append(txIDs, configTxEnv.Payload.TxID)

	default:
		return errors.Errorf("unexpected transaction envelope in the block")
	}

	t.pendingTxs.remove(txIDs)

	return nil
}

func (t *transactionProcessor) isTxIDDuplicate(txID string) (bool, error) {
	if t.pendingTxs.has(txID) {
		return true, nil
	}

	isTxIDAlreadyCommitted, err := t.blockStore.DoesTxIDExist(txID)
	if err != nil {
		return false, err
	}
	return isTxIDAlreadyCommitted, nil
}

func (t *transactionProcessor) close() error {
	t.Lock()
	defer t.Unlock()

	t.txReorderer.Stop()
	t.blockCreator.Stop()
	t.blockProcessor.Stop()

	return nil
}

type pendingTxs struct {
	txIDs map[string]bool
	sync.RWMutex
}

func (p *pendingTxs) add(txID string) {
	p.Lock()
	defer p.Unlock()

	p.txIDs[txID] = true
}

func (p *pendingTxs) remove(txIDs []string) {
	p.Lock()
	defer p.Unlock()

	for _, txID := range txIDs {
		delete(p.txIDs, txID)
	}
}

func (p *pendingTxs) has(txID string) bool {
	p.RLock()
	defer p.RUnlock()

	return p.txIDs[txID]
}

func (p *pendingTxs) isEmpty() bool {
	p.RLock()
	defer p.RUnlock()

	return len(p.txIDs) == 0
}
