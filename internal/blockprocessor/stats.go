package blockprocessor

import (
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

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
			},
		),
		validationTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "validation_time",
				Help:      "The time taken in seconds to validate a block",
			},
		),
		skipListConstructionTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "skiplist_construction_time",
				Help:      "The time taken in seconds to construct skip list for a block",
			},
		),
		txMerkelTreeBuildTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "merkle_tree_build_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
			},
		),
		commitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace:   "block",
				Name:        "commit_time",
				Help:        "The time taken in seconds to commit a block",
				ConstLabels: map[string]string{},
				Buckets:     []float64{},
			},
		),
		commitEntriesConstructionTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "commit_entries_construction_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
			},
		),
		stateTrieUpdateTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "state_trie_update_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
			},
		),
		blockStoreCommitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "store_commit_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
			},
		),
		provenanceStoreCommitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "provenance_commit_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
			},
		),
		worldstateCommitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "worldstate_commit_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
			},
		),
		stateTrieCommitTime: promauto.NewHistogram(
			prometheus.HistogramOpts{
				Namespace: "block",
				Name:      "state_trie_commit_time",
				Help:      "The time taken in seconds to build a merkle tree of transactions in a block",
			},
		),
		transactionCount: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "transaction_count",
				Help: "Number of transactions processed.",
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

func (s *blockProcessorStats) incrementTransactionCount(flag types.Flag, tx_type string) {
	s.transactionCount.WithLabelValues(flag.String(), tx_type).Inc()
}
