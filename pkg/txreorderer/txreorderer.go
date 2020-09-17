package txreorderer

import (
	"time"

	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

// TxReorderer holds queue and other components needed to reorder
// transactions before creating a next batch of transactions to be
// included in the block
type TxReorderer struct {
	txQueue            *queue.Queue
	txBatchQueue       *queue.Queue
	maxTxCountPerBatch uint32
	batchTimeout       time.Duration
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
		logger:             conf.Logger,
	}
}

// Run runs the transactions batch creator
func (b *TxReorderer) Run() {
	txs := &types.DataTxEnvelopes{}

	for {
		tx := b.txQueue.Dequeue()

		switch tx.(type) {
		case *types.DataTxEnvelope:
			txs.Envelopes = append(txs.Envelopes, tx.(*types.DataTxEnvelope))

			if uint32(len(txs.Envelopes)) == b.maxTxCountPerBatch {
				b.logger.Debug("enqueueing data transactions")
				b.enqueueDataTxBatch(txs)
				txs = &types.DataTxEnvelopes{}
			}
		case *types.UserAdministrationTxEnvelope:
			if len(txs.Envelopes) > 0 {
				b.enqueueDataTxBatch(txs)
				txs = &types.DataTxEnvelopes{}
			}

			b.logger.Debug("enqueueing user administrative transaction")
			b.txBatchQueue.Enqueue(
				&types.Block_UserAdministrationTxEnvelope{
					UserAdministrationTxEnvelope: tx.(*types.UserAdministrationTxEnvelope),
				},
			)
		case *types.DBAdministrationTxEnvelope:
			if len(txs.Envelopes) > 0 {
				b.enqueueDataTxBatch(txs)
				txs = &types.DataTxEnvelopes{}
			}

			b.logger.Debug("enqueueing db administrative transaction")
			b.txBatchQueue.Enqueue(
				&types.Block_DBAdministrationTxEnvelope{
					DBAdministrationTxEnvelope: tx.(*types.DBAdministrationTxEnvelope),
				},
			)
		case *types.ConfigTxEnvelope:
			if len(txs.Envelopes) > 0 {
				b.enqueueDataTxBatch(txs)
				txs = &types.DataTxEnvelopes{}
			}

			b.logger.Debug("enqueueing cluster config transaction")
			b.txBatchQueue.Enqueue(
				&types.Block_ConfigTxEnvelope{
					ConfigTxEnvelope: tx.(*types.ConfigTxEnvelope),
				},
			)
		}
	}
}

func (b *TxReorderer) enqueueDataTxBatch(txBatch *types.DataTxEnvelopes) {
	b.logger.Debugf("enqueueing [%d] data transactions", len(txBatch.Envelopes))
	b.txBatchQueue.Enqueue(
		&types.Block_DataTxEnvelopes{
			DataTxEnvelopes: txBatch,
		},
	)
}
