package blockprocessor

import (
	"math"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var timeBuckets = []float64{
	math.Inf(-1), 0, 1e-9, 1e-8, 1e-7, 1e-6, 1e-5,
	1e-4, 2.5e-4, 5e-4, 7.5e-4,
	1e-3, 2.5e-3, 5e-3, 7.5e-3,
	1e-2, 2.5e-2, 5e-2, 7.5e-2,
	1e-1, 2.5e-1, 5e-1, 7.5e-1,
	1, 2.5, 5, 7.5,
	10, 25, 50, 75,
	1e2, 1e3, 1e4, 1e5, 1e6, math.Inf(1),
}

var sizeBase10Buckets = []float64{
	0, 1, 10, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 1e5, 1e6, 1e7, 1e8, math.Inf(1),
}

var sizeBase2Buckets = []float64{
	0, 1 << 6, 1 << 8,
	1 << 10, 1 << 12, 1 << 14, 1 << 16, 1 << 18,
	1 << 20, 1 << 22, 1 << 24, 1 << 26, 1 << 28,
	1 << 30, math.Inf(1),
}

type blockProcessorStats struct {
	processingTime                prometheus.Histogram
	validationTime                prometheus.Histogram
	skipListConstructionTime      prometheus.Histogram
	txMerkelTreeBuildTime         prometheus.Histogram
	commitTime                    prometheus.Histogram
	commitEntriesConstructionTime prometheus.Histogram
	stateTrieUpdateTime           prometheus.Histogram
	blockStoreCommitTime          prometheus.Histogram
	provenanceStoreCommitTime     prometheus.Histogram
	worldstateCommitTime          prometheus.Histogram
	stateTrieCommitTime           prometheus.Histogram
	transactionPerBlock           prometheus.Histogram
	blockSizeBytes                prometheus.Histogram
	transactionCount              *prometheus.CounterVec
}

func newBlockProcessorStats() *blockProcessorStats {
	return &blockProcessorStats{
		processingTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "processing_time",
				Help:      "The time taken in seconds to process a block",
				Buckets:   timeBuckets,
			},
		),
		validationTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "validation_time",
				Help:      "The time taken in seconds to validate a block",
				Buckets:   timeBuckets,
			},
		),
		skipListConstructionTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "skiplist_construction_time",
				Help:      "The time taken in seconds to construct skip list for a block",
				Buckets:   timeBuckets,
			},
		),
		txMerkelTreeBuildTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "merkle_tree_build_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   timeBuckets,
			},
		),
		commitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "commit_time",
				Help:      "The time taken in seconds to commit a block",
				Buckets:   timeBuckets,
			},
		),
		commitEntriesConstructionTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "commit_entries_construction_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   timeBuckets,
			},
		),
		stateTrieUpdateTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "state_trie_update_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   timeBuckets,
			},
		),
		blockStoreCommitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "store_commit_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   timeBuckets,
			},
		),
		provenanceStoreCommitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "provenance_commit_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   timeBuckets,
			},
		),
		worldstateCommitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "worldstate_commit_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   timeBuckets,
			},
		),
		stateTrieCommitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "state_trie_commit_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   timeBuckets,
			},
		),
		transactionPerBlock: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "tx_count",
				Help:      "The number of transactions per block",
				Buckets:   sizeBase10Buckets,
			},
		),
		blockSizeBytes: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "size_bytes",
				Help:      "Total block size in bytes",
				Buckets:   sizeBase2Buckets,
			},
		),
		transactionCount: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "transaction",
				Name:      "count",
				Help:      "Number of transactions processed.",
			},
			[]string{"validation_code", "transaction_type"},
		),
	}
}

func (s *blockProcessorStats) updateProcessingTime(t time.Duration) {
	s.processingTime.Observe(t.Seconds())
}

func (s *blockProcessorStats) updateValidationTime(t time.Duration) {
	s.validationTime.Observe(t.Seconds())
}

func (s *blockProcessorStats) updateSkipListConstructionTime(t time.Duration) {
	s.skipListConstructionTime.Observe(t.Seconds())
}

func (s *blockProcessorStats) updateMerkelTreeBuildTime(t time.Duration) {
	s.txMerkelTreeBuildTime.Observe(t.Seconds())
}

func (s *blockProcessorStats) updateCommitTime(t time.Duration) {
	s.commitTime.Observe(t.Seconds())
}

func (s *blockProcessorStats) updateCommitEntriesConstructionTime(t time.Duration) {
	s.commitEntriesConstructionTime.Observe(t.Seconds())
}

func (s *blockProcessorStats) updateStateTrieUpdateTime(t time.Duration) {
	s.stateTrieUpdateTime.Observe(t.Seconds())
}

func (s *blockProcessorStats) updateBlockStoreCommitTime(t time.Duration) {
	s.blockStoreCommitTime.Observe(t.Seconds())
}

func (s *blockProcessorStats) updateProvenanceStoreCommitTime(t time.Duration) {
	s.provenanceStoreCommitTime.Observe(t.Seconds())
}

func (s *blockProcessorStats) updateWorldStateCommitTime(t time.Duration) {
	s.worldstateCommitTime.Observe(t.Seconds())
}

func (s *blockProcessorStats) updateStateTrieCommitTime(t time.Duration) {
	s.stateTrieCommitTime.Observe(t.Seconds())
}

func (s *blockProcessorStats) updateTransactionsPerBlock(size int) {
	s.transactionPerBlock.Observe(float64(size))
}

func (s *blockProcessorStats) updateBlockSizeBytes(size int64) {
	s.blockSizeBytes.Observe(float64(size))
}

func (s *blockProcessorStats) incrementTransactionCount(flag types.Flag, tx_type string) {
	s.transactionCount.WithLabelValues(flag.String(), tx_type).Inc()
}
