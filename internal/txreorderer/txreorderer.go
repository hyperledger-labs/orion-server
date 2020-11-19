package txreorderer

import (
	"time"

	"github.ibm.com/blockchaindb/server/internal/queue"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// TxReorderer holds queue and other components needed to reorder
// transactions before creating a next batch of transactions to be
// included in the block
type TxReorderer struct {
	txQueue            *queue.Queue
	txBatchQueue       *queue.Queue
	maxTxCountPerBatch uint32
	batchTimeout       time.Duration
	stop               chan struct{}
	stopped            chan struct{}
	logger             *logger.SugarLogger
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
}

// New creates a transaction reorderer
func New(conf *Config) *TxReorderer {
	return &TxReorderer{
		txQueue:            conf.TxQueue,
		txBatchQueue:       conf.TxBatchQueue,
		maxTxCountPerBatch: conf.MaxTxCountPerBatch,
		batchTimeout:       conf.BatchTimeout,
		stop:               make(chan struct{}),
		stopped:            make(chan struct{}),
		logger:             conf.Logger,
	}
}

// Run runs the transactions batch creator
func (r *TxReorderer) Run() {
	defer close(r.stopped)
	r.logger.Info("starting the transactions reorderer")

	txs := &types.DataTxEnvelopes{}

	for {
		select {
		case <-r.stop:
			r.logger.Info("stopping the transaction reorderer")
			return

		default:
			tx := r.txQueue.Dequeue()
			if tx == nil {
				// when the queue is closed during the teardown/cleanup,
				// the dequeued tx would be nil.
				continue
			}

			switch tx.(type) {
			case *types.DataTxEnvelope:
				txs.Envelopes = append(txs.Envelopes, tx.(*types.DataTxEnvelope))

				if uint32(len(txs.Envelopes)) == r.maxTxCountPerBatch {
					r.logger.Debug("enqueueing data transactions")
					r.enqueueDataTxBatch(txs)
					txs = &types.DataTxEnvelopes{}
				}
			case *types.UserAdministrationTxEnvelope:
				if len(txs.Envelopes) > 0 {
					r.enqueueDataTxBatch(txs)
					txs = &types.DataTxEnvelopes{}
				}

				r.logger.Debug("enqueueing user administrative transaction")
				r.txBatchQueue.Enqueue(
					&types.Block_UserAdministrationTxEnvelope{
						UserAdministrationTxEnvelope: tx.(*types.UserAdministrationTxEnvelope),
					},
				)
			case *types.DBAdministrationTxEnvelope:
				if len(txs.Envelopes) > 0 {
					r.enqueueDataTxBatch(txs)
					txs = &types.DataTxEnvelopes{}
				}

				r.logger.Debug("enqueueing db administrative transaction")
				r.txBatchQueue.Enqueue(
					&types.Block_DBAdministrationTxEnvelope{
						DBAdministrationTxEnvelope: tx.(*types.DBAdministrationTxEnvelope),
					},
				)
			case *types.ConfigTxEnvelope:
				if len(txs.Envelopes) > 0 {
					r.enqueueDataTxBatch(txs)
					txs = &types.DataTxEnvelopes{}
				}

				r.logger.Debug("enqueueing cluster config transaction")
				r.txBatchQueue.Enqueue(
					&types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: tx.(*types.ConfigTxEnvelope),
					},
				)
			}
		}
	}
}

// Stop stops the transaction reorderer
func (r *TxReorderer) Stop() {
	r.txQueue.Close()
	close(r.stop)
	<-r.stopped
}

func (r *TxReorderer) enqueueDataTxBatch(txBatch *types.DataTxEnvelopes) {
	r.logger.Debugf("enqueueing [%d] data transactions", len(txBatch.Envelopes))
	r.txBatchQueue.Enqueue(
		&types.Block_DataTxEnvelopes{
			DataTxEnvelopes: txBatch,
		},
	)
}
