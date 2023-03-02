// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package txreorderer

import (
	"time"

	"github.com/hyperledger-labs/orion-server/internal/queue"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

// TxReorderer holds queue and other components needed to reorder
// transactions before creating a next batch of transactions to be
// included in the block
type TxReorderer struct {
	txQueue            *queue.Queue
	txBatchQueue       *queue.Queue
	maxTxCountPerBatch uint32
	batchTimeout       time.Duration
	started            chan struct{}
	stop               chan struct{}
	stopped            chan struct{}
	pendingDataTxs     *types.DataTxEnvelopes
	logger             *logger.SugarLogger
	metrics            *utils.TxProcessingMetrics
	// TODO:
	// tx merkle tree
	// dependency graph
	// early abort and reorder
}

// Config holds the configuration information need to start the transaction
// reorderer
type Config struct {
	TxQueue            *queue.Queue
	TxBatchQueue       *queue.Queue
	MaxTxCountPerBatch uint32
	BatchTimeout       time.Duration
	Logger             *logger.SugarLogger
	Metrics            *utils.TxProcessingMetrics
}

// New creates a transaction reorderer
func New(conf *Config) *TxReorderer {
	return &TxReorderer{
		txQueue:            conf.TxQueue,
		txBatchQueue:       conf.TxBatchQueue,
		maxTxCountPerBatch: conf.MaxTxCountPerBatch,
		batchTimeout:       conf.BatchTimeout,
		started:            make(chan struct{}),
		stop:               make(chan struct{}),
		stopped:            make(chan struct{}),
		logger:             conf.Logger,
		metrics:            conf.Metrics,
	}
}

// Start starts the transactions batch creator
func (r *TxReorderer) Start() {
	defer close(r.stopped)
	r.logger.Info("starting the transactions reorderer")
	close(r.started)

	ticker := time.NewTicker(r.batchTimeout)
	defer ticker.Stop()

	r.pendingDataTxs = &types.DataTxEnvelopes{}

	for {
		select {
		case <-r.stop:
			r.logger.Info("stopping the transaction reorderer")
			return

		case <-ticker.C:
			r.logger.Debug("block timeout has occurred")
			r.enqueueAndResetPendingDataTxBatch()

		default:
			tx := r.txQueue.DequeueWithWaitLimit(r.batchTimeout)
			if tx == nil {
				continue
			}

			switch env := tx.(type) {
			case *types.DataTxEnvelope:
				r.pendingDataTxs.Envelopes = append(r.pendingDataTxs.Envelopes, env)

				if uint32(len(r.pendingDataTxs.Envelopes)) == r.maxTxCountPerBatch {
					r.enqueueAndResetPendingDataTxBatch()
					ticker.Reset(r.batchTimeout)
				}

			case *types.UserAdministrationTxEnvelope:
				r.enqueueAndResetPendingDataTxBatch()

				r.logger.Debug("enqueueing user administrative transaction")
				r.txBatchQueue.Enqueue(
					&types.Block_UserAdministrationTxEnvelope{
						UserAdministrationTxEnvelope: env,
					},
				)
				ticker.Reset(r.batchTimeout)

			case *types.DBAdministrationTxEnvelope:
				r.enqueueAndResetPendingDataTxBatch()

				r.logger.Debug("enqueueing db administrative transaction")
				r.txBatchQueue.Enqueue(
					&types.Block_DbAdministrationTxEnvelope{
						DbAdministrationTxEnvelope: env,
					},
				)
				ticker.Reset(r.batchTimeout)

			case *types.ConfigTxEnvelope:
				r.enqueueAndResetPendingDataTxBatch()

				r.logger.Debug("enqueueing cluster config transaction")
				r.txBatchQueue.Enqueue(
					&types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: env,
					},
				)
				ticker.Reset(r.batchTimeout)
			}
		}
	}
}

// WaitTillStart waits till the transaction reorderer is started
func (r *TxReorderer) WaitTillStart() {
	<-r.started
}

// Stop stops the transaction reorderer
func (r *TxReorderer) Stop() {
	r.txQueue.Close()
	close(r.stop)
	<-r.stopped
}

func (r *TxReorderer) enqueueAndResetPendingDataTxBatch() {
	if len(r.pendingDataTxs.Envelopes) == 0 {
		return
	}

	r.logger.Debugf("enqueueing [%d] data transactions", len(r.pendingDataTxs.Envelopes))
	r.txBatchQueue.Enqueue(
		&types.Block_DataTxEnvelopes{
			DataTxEnvelopes: r.pendingDataTxs,
		},
	)
	r.metrics.QueueSize("batch", r.txBatchQueue.Size())

	r.pendingDataTxs = &types.DataTxEnvelopes{}
}
