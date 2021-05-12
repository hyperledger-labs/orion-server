// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/internal/blockcreator"
	"github.com/IBM-Blockchain/bcdb-server/internal/blockprocessor"
	"github.com/IBM-Blockchain/bcdb-server/internal/blockstore"
	internalerror "github.com/IBM-Blockchain/bcdb-server/internal/errors"
	"github.com/IBM-Blockchain/bcdb-server/internal/provenance"
	"github.com/IBM-Blockchain/bcdb-server/internal/queue"
	"github.com/IBM-Blockchain/bcdb-server/internal/replication"
	"github.com/IBM-Blockchain/bcdb-server/internal/txreorderer"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/pkg/errors"
)

const (
	commitListenerName = "transactionProcessor"
)

type transactionProcessor struct {
	nodeID               string
	txQueue              *queue.Queue
	txBatchQueue         *queue.Queue
	blockOneQueueBarrier *queue.OneQueueBarrier
	txReorderer          *txreorderer.TxReorderer
	blockCreator         *blockcreator.BlockCreator
	blockReplicator      *replication.BlockReplicator
	blockProcessor       *blockprocessor.BlockProcessor
	blockStore           *blockstore.Store
	pendingTxs           *pendingTxs
	logger               *logger.SugarLogger
	sync.Mutex
}

type txProcessorConfig struct {
	nodeID             string
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

	p.nodeID = conf.nodeID
	p.logger = conf.logger
	p.txQueue = queue.New(conf.txQueueLength)
	p.txBatchQueue = queue.New(conf.txBatchQueueLength)
	p.blockOneQueueBarrier = queue.NewOneQueueBarrier(conf.logger)

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
			Logger:       conf.logger,
			BlockStore:   conf.blockStore,
		},
	); err != nil {
		return nil, err
	}

	p.blockReplicator = replication.NewBlockReplicator(
		&replication.Config{
			BlockOneQueueBarrier: p.blockOneQueueBarrier,
			Logger:               conf.logger,
		},
	)

	p.blockCreator.RegisterReplicator(p.blockReplicator)

	p.blockProcessor = blockprocessor.New(
		&blockprocessor.Config{
			BlockOneQueueBarrier: p.blockOneQueueBarrier,
			BlockStore:           conf.blockStore,
			ProvenanceStore:      conf.provenanceStore,
			DB:                   conf.db,
			Logger:               conf.logger,
		},
	)

	_ = p.blockProcessor.RegisterBlockCommitListener(commitListenerName, p)

	go p.txReorderer.Start()
	p.txReorderer.WaitTillStart()

	go p.blockCreator.Start()
	p.blockCreator.WaitTillStart()

	p.blockReplicator.Start()

	go p.blockProcessor.Start()
	p.blockProcessor.WaitTillStart()

	p.pendingTxs = &pendingTxs{
		txs: make(map[string]*promise),
	}
	p.blockStore = conf.blockStore

	return p, nil
}

// submitTransaction enqueue the transaction to the transaction queue
// If the timeout is set to 0, the submission would be treated as async while
// a non-zero timeout would be treated as a sync submission. When a timeout
// occurs with the sync submission, a timeout error will be returned
func (t *transactionProcessor) submitTransaction(tx interface{}, timeout time.Duration) (*types.TxResponse, error) {
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
		return nil, errors.Errorf("unexpected transaction type")
	}

	t.Lock()
	duplicate, err := t.isTxIDDuplicate(txID)
	if err != nil {
		t.Unlock()
		return nil, err
	}
	if duplicate {
		t.Unlock()
		return nil, &internalerror.DuplicateTxIDError{TxID: txID}
	}

	if t.txQueue.IsFull() {
		t.Unlock()
		return nil, fmt.Errorf("transaction queue is full. It means the server load is high. Try after sometime")
	}

	jsonBytes, err := json.MarshalIndent(tx, "", "\t")
	if err != nil {
		t.Unlock()
		return nil, fmt.Errorf("failed to marshal transaction: %v", err)
	}
	t.logger.Debugf("enqueuing transaction %s\n", string(jsonBytes))

	t.txQueue.Enqueue(tx)
	t.logger.Debug("transaction is enqueued for re-ordering")

	var p *promise
	if timeout > 0 {
		p = &promise{
			receipt: make(chan *types.TxReceipt),
			timeout: timeout,
		}
	}

	// TODO: add limit on the number of pending sync tx
	t.pendingTxs.add(txID, p)
	t.Unlock()

	receipt, err := p.wait()

	if err != nil {
		return nil, err
	}

	return &types.TxResponse{
		Receipt: receipt,
	}, nil
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

	t.pendingTxs.removeAndSendReceipt(txIDs, block.Header)

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
	t.blockReplicator.Close()
	t.blockProcessor.Stop()

	return nil
}

type pendingTxs struct {
	txs map[string]*promise
	sync.RWMutex
}

func (p *pendingTxs) add(txID string, subMethod *promise) {
	p.Lock()
	defer p.Unlock()

	p.txs[txID] = subMethod
}

func (p *pendingTxs) removeAndSendReceipt(txIDs []string, blockHeader *types.BlockHeader) {
	p.Lock()
	defer p.Unlock()

	for txIndex, txID := range txIDs {
		p.txs[txID].done(
			&types.TxReceipt{
				Header:  blockHeader,
				TxIndex: uint64(txIndex),
			},
		)

		delete(p.txs, txID)
	}
}

func (p *pendingTxs) has(txID string) bool {
	p.RLock()
	defer p.RUnlock()

	_, ok := p.txs[txID]
	return ok
}

func (p *pendingTxs) isEmpty() bool {
	p.RLock()
	defer p.RUnlock()

	return len(p.txs) == 0
}

type promise struct {
	receipt chan *types.TxReceipt
	timeout time.Duration
}

func (s *promise) wait() (*types.TxReceipt, error) {
	if s == nil {
		return nil, nil
	}

	ticker := time.NewTicker(s.timeout)
	select {
	case <-ticker.C:
		s.close()
		return nil, &internalerror.TimeoutErr{
			ErrMsg: "timeout has occurred while waiting for the transaction receipt",
		}
	case r := <-s.receipt:
		ticker.Stop()
		s.close()
		return r, nil
	}
}

func (s *promise) done(r *types.TxReceipt) {
	if s == nil {
		return
	}

	s.receipt <- r
}

func (s *promise) close() {
	if s == nil {
		return
	}

	close(s.receipt)
	s = nil
}
