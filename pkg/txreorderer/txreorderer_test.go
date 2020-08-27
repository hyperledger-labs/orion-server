package txreorderer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

func TestTxReorderer(t *testing.T) {
	t.Run("different-tx-batch-size", func(t *testing.T) {
		b := New(&Config{
			TxQueue:            queue.New(10),
			TxBatchQueue:       queue.New(10),
			MaxTxCountPerBatch: 1,
			BatchTimeout:       50 * time.Millisecond,
		})
		go b.Run()

		testCases := []struct {
			MaxTxCountPerBatch uint32
			txs                []*types.TransactionEnvelope
			expectedTxBatches  [][]*types.TransactionEnvelope
		}{
			{
				MaxTxCountPerBatch: 1,
				txs: []*types.TransactionEnvelope{
					{
						Payload: &types.Transaction{
							Type: types.Transaction_DATA,
						},
						Signature: []byte("sign-1"),
					},
					{
						Payload: &types.Transaction{
							Type: types.Transaction_CONFIG,
						},
						Signature: []byte("sign-2"),
					},
					{
						Payload: &types.Transaction{
							Type: types.Transaction_DATA,
						},
						Signature: []byte("sign-3"),
					},
				},
				// as txBatchSize is 1, with three tx, we expect three tx batches
				// each with a single transaction
				expectedTxBatches: [][]*types.TransactionEnvelope{
					{
						{
							Payload: &types.Transaction{
								Type: types.Transaction_DATA,
							},
							Signature: []byte("sign-1"),
						},
					},
					{
						{
							Payload: &types.Transaction{
								Type: types.Transaction_CONFIG,
							},
							Signature: []byte("sign-2"),
						},
					},
					{
						{
							Payload: &types.Transaction{
								Type: types.Transaction_DATA,
							},
							Signature: []byte("sign-3"),
						},
					},
				},
			},
			{
				MaxTxCountPerBatch: 3,
				txs: []*types.TransactionEnvelope{
					{
						Payload: &types.Transaction{
							Type: types.Transaction_DATA,
						},
						Signature: []byte("sign-1"),
					},
					{
						Payload: &types.Transaction{
							Type: types.Transaction_DATA,
						},
						Signature: []byte("sign-2"),
					},
					{
						Payload: &types.Transaction{
							Type: types.Transaction_CONFIG,
						},
						Signature: []byte("sign-3"),
					},
					{
						Payload: &types.Transaction{
							Type: types.Transaction_DATA,
						},
						Signature: []byte("sign-4"),
					},
					{
						Payload: &types.Transaction{
							Type: types.Transaction_CONFIG,
						},
						Signature: []byte("sign-5"),
					},
				},
				// though the txBatchSize is 3, we will have 4 batches
				// due to the presence of config transaction
				expectedTxBatches: [][]*types.TransactionEnvelope{
					{
						{
							Payload: &types.Transaction{
								Type: types.Transaction_DATA,
							},
							Signature: []byte("sign-1"),
						},
						{
							Payload: &types.Transaction{
								Type: types.Transaction_DATA,
							},
							Signature: []byte("sign-2"),
						},
					},
					{
						{
							Payload: &types.Transaction{
								Type: types.Transaction_CONFIG,
							},
							Signature: []byte("sign-3"),
						},
					},
					{
						{
							Payload: &types.Transaction{
								Type: types.Transaction_DATA,
							},
							Signature: []byte("sign-4"),
						},
					},
					{
						{
							Payload: &types.Transaction{
								Type: types.Transaction_CONFIG,
							},
							Signature: []byte("sign-5"),
						},
					},
				},
			},
			{
				MaxTxCountPerBatch: 3,
				txs: []*types.TransactionEnvelope{
					{
						Payload: &types.Transaction{
							Type: types.Transaction_DATA,
						},
						Signature: []byte("sign-1"),
					},
					{
						Payload: &types.Transaction{
							Type: types.Transaction_USER,
						},
						Signature: []byte("sign-2"),
					},
					{
						Payload: &types.Transaction{
							Type: types.Transaction_CONFIG,
						},
						Signature: []byte("sign-3"),
					},
					{
						Payload: &types.Transaction{
							Type: types.Transaction_DATA,
						},
						Signature: []byte("sign-4"),
					},
					{
						Payload: &types.Transaction{
							Type: types.Transaction_DB,
						},
						Signature: []byte("sign-5"),
					},
				},
				// though the txBatchSize is 3, we will have 5 batches
				// due to the presence of config, data, and user transaction
				expectedTxBatches: [][]*types.TransactionEnvelope{
					{
						{
							Payload: &types.Transaction{
								Type: types.Transaction_DATA,
							},
							Signature: []byte("sign-1"),
						},
					},
					{
						{
							Payload: &types.Transaction{
								Type: types.Transaction_USER,
							},
							Signature: []byte("sign-2"),
						},
					},
					{
						{
							Payload: &types.Transaction{
								Type: types.Transaction_CONFIG,
							},
							Signature: []byte("sign-3"),
						},
					},
					{
						{
							Payload: &types.Transaction{
								Type: types.Transaction_DATA,
							},
							Signature: []byte("sign-4"),
						},
					},
					{
						{
							Payload: &types.Transaction{
								Type: types.Transaction_DB,
							},
							Signature: []byte("sign-5"),
						},
					},
				},
			},
		}

		for _, testCase := range testCases {
			b.MaxTxCountPerBatch = testCase.MaxTxCountPerBatch
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
