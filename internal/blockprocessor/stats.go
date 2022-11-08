package blockprocessor

import (
	"math"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var buckets = []float64{
	math.Inf(-1), 0, 1e-9, 1e-8, 1e-7, 1e-6, 1e-5, 1e-4, 1e-3, 1e-2, 1e-1,
	1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, math.Inf(1),
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
	transactionCount              *prometheus.CounterVec
}

func newBlockProcessorStats() *blockProcessorStats {
	return &blockProcessorStats{
		processingTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "processing_time",
				Help:      "The time taken in seconds to process a block",
				Buckets:   buckets,
			},
		),
		validationTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "validation_time",
				Help:      "The time taken in seconds to validate a block",
				Buckets:   buckets,
			},
		),
		skipListConstructionTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "skiplist_construction_time",
				Help:      "The time taken in seconds to construct skip list for a block",
				Buckets:   buckets,
			},
		),
		txMerkelTreeBuildTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "merkle_tree_build_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   buckets,
			},
		),
		commitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "commit_time",
				Help:      "The time taken in seconds to commit a block",
				Buckets:   buckets,
			},
		),
		commitEntriesConstructionTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "commit_entries_construction_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   buckets,
			},
		),
		stateTrieUpdateTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "state_trie_update_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   buckets,
			},
		),
		blockStoreCommitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "store_commit_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   buckets,
			},
		),
		provenanceStoreCommitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "provenance_commit_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   buckets,
			},
		),
		worldstateCommitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "worldstate_commit_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   buckets,
			},
		),
		stateTrieCommitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "state_trie_commit_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
				Buckets:   buckets,
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

func (s *blockProcessorStats) updateMerkelTreeBuildTimeTime(t time.Duration) {
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

func (s *blockProcessorStats) incrementTransactionCount(flag types.Flag, txType string) {
	s.transactionCount.WithLabelValues(flag.String(), txType).Inc()
}
