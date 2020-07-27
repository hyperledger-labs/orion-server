package txreorderer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

func TestBatchCreator(t *testing.T) {
	t.Run("different-tx-batch-size", func(t *testing.T) {
		b := NewBatchCreator(queue.New(10), queue.New(10))
		go b.Run()

		testCases := []struct {
			txBatchSize       int
			txs               []*types.TransactionEnvelope
			expectedTxBatches [][]*types.TransactionEnvelope
		}{
			{
				txBatchSize: 1,
				txs: []*types.TransactionEnvelope{
					{
						Signature: []byte("sign-1"),
					},
					{
						Signature: []byte("sign-2"),
					},
				},
				expectedTxBatches: [][]*types.TransactionEnvelope{
					{
						{
							Signature: []byte("sign-1"),
						},
					},
					{
						{
							Signature: []byte("sign-2"),
						},
					},
				},
			},
			{
				txBatchSize: 3,
				txs: []*types.TransactionEnvelope{
					{
						Signature: []byte("sign-1"),
					},
					{
						Signature: []byte("sign-2"),
					},
					{
						Signature: []byte("sign-3"),
					},
				},
				expectedTxBatches: [][]*types.TransactionEnvelope{
					{
						{
							Signature: []byte("sign-1"),
						},
						{
							Signature: []byte("sign-2"),
						},
						{
							Signature: []byte("sign-3"),
						},
					},
				},
			},
		}

		for _, testCase := range testCases {
			txBatchSize = testCase.txBatchSize
			for _, tx := range testCase.txs {
				b.txQueue.Enqueue(tx)
			}

			hasBatchSizeMatched := func() bool {
				if len(testCase.expectedTxBatches) == b.txBatchQueue.Size() {
					return true
				}
				return false
			}
			require.Eventually(t, hasBatchSizeMatched, 2*time.Second, 100*time.Millisecond)

			for _, expectedTxBatch := range testCase.expectedTxBatches {
				txBatch := b.txBatchQueue.Dequeue().([]*types.TransactionEnvelope)
				require.Equal(t, expectedTxBatch, txBatch)
			}
		}
	})
}
