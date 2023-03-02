package utils

import (
	"math"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
)

var TimeBuckets = []float64{
	math.Inf(-1), 0,
	1e-9, 1e-8, 1e-7, 1e-6, 1e-5,
	1e-4, 2.5e-4, 5e-4, 7.5e-4,
	1e-3, 2.5e-3, 5e-3, 7.5e-3,
	1e-2, 2.5e-2, 5e-2, 7.5e-2,
	1e-1, 2.5e-1, 5e-1, 7.5e-1,
	1, 2.5, 5, 7.5,
	10, 25, 50, 75,
	1e2, 1e3, 1e4, 1e5, 1e6,
	math.Inf(1),
}

var SizeBase10Buckets = []float64{
	math.Inf(-1), 0,
	1, 1e1, 2.5e1, 5e1, 1e2, 2.5e2, 5e2, 1e3, 2.5e3, 5e3,
	1e4, 2.5e4, 5e4, 1e5, 2.5e5, 5e5, 1e6, 2.5e6, 5e6, 1e7, 1e8,
	math.Inf(1),
}

var SizeBase2Buckets = []float64{
	0, 1 << 3, 1 << 6, 1 << 8, 1 << 9,
	1 << 10, 1 << 12, 1 << 14, 1 << 16, 1 << 18,
	1 << 20, 1 << 22, 1 << 24, 1 << 26, 1 << 28,
	1 << 30, math.Inf(1),
}

type TxProcessingMetrics struct {
	latency        *prometheus.HistogramVec
	queueSize      *prometheus.GaugeVec
	txCount        *prometheus.CounterVec
	txPerBlock     prometheus.Histogram
	blockSizeBytes prometheus.Histogram
	txSize         prometheus.Histogram
}

const namespace = "tx_processing"

func NewProcessingMetrics(reg *prometheus.Registry) *TxProcessingMetrics {
	if reg == nil {
		return nil
	}
	s := &TxProcessingMetrics{
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "latency_seconds",
			Buckets:   TimeBuckets,
		}, []string{"process"}),
		queueSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_size_txs",
		}, []string{"queue_type"}),
		txCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "tx_count",
		}, []string{"validation_code", "transaction_type"}),
		txSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "tx_size_bytes",
			Help:      "Total TX size in bytes",
			Buckets:   SizeBase2Buckets,
		}),
		txPerBlock: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "tx_per_block",
			Help:      "The number of transactions per block",
			Buckets:   SizeBase10Buckets,
		}),
		blockSizeBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "block_size_bytes",
			Help:      "Total block size in bytes",
			Buckets:   SizeBase2Buckets,
		}),
	}
	reg.MustRegister(
		s.latency,
		s.queueSize,
		s.txCount,
		s.txSize,
		s.txPerBlock,
		s.blockSizeBytes,
	)
	return s
}

func (s *TxProcessingMetrics) Latency(label string, startTime time.Time) {
	if s != nil {
		s.latency.WithLabelValues(label).Observe(time.Since(startTime).Seconds())
	}
}

func (s *TxProcessingMetrics) QueueSize(label string, size int) {
	if s != nil {
		s.queueSize.WithLabelValues(label).Set(float64(size))
	}
}

func (s *TxProcessingMetrics) IncrementTxCount(flag types.Flag, txType string) {
	if s != nil {
		s.txCount.WithLabelValues(flag.String(), txType).Inc()
	}
}

func (s *TxProcessingMetrics) TxSize(size int) {
	if s != nil {
		s.txSize.Observe(float64(size))
	}
}

func (s *TxProcessingMetrics) TxPerBlock(size int) {
	if s != nil {
		s.txPerBlock.Observe(float64(size))
	}
}

func (s *TxProcessingMetrics) BlockSize(size int64) {
	if s != nil {
		s.blockSizeBytes.Observe(float64(size))
	}
}
