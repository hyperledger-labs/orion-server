package blockcreator

import (
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

func TestBatchCreator(t *testing.T) {
	setup := func(lastCommittedBlockNumber uint64) *BlockCreator {
		b := New(&Config{
			TxBatchQueue:    queue.New(10),
			BlockQueue:      queue.New(10),
			LastBlockNumber: lastCommittedBlockNumber,
		})
		go b.Run()

		return b
	}

	t.Run("construct-different-sized-blocks", func(t *testing.T) {
		blockCreatorWithLastBlockNumber0 := setup(0)
		blockCreatorWithLastBlockNumber5 := setup(5)

		testCases := []struct {
			blockCreator   *BlockCreator
			txBatches      [][]*types.TransactionEnvelope
			expectedBlocks []*types.Block
		}{
			{
				blockCreator: blockCreatorWithLastBlockNumber0,
				txBatches: [][]*types.TransactionEnvelope{
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
				expectedBlocks: []*types.Block{
					{
						Header: &types.BlockHeader{
							Number: 1,
						},
						TransactionEnvelopes: []*types.TransactionEnvelope{
							{
								Signature: []byte("sign-1"),
							},
						},
					},
					{
						Header: &types.BlockHeader{
							Number: 2,
						},
						TransactionEnvelopes: []*types.TransactionEnvelope{
							{
								Signature: []byte("sign-2"),
							},
						},
					},
				},
			},
			{
				blockCreator: blockCreatorWithLastBlockNumber0,
				txBatches: [][]*types.TransactionEnvelope{
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
				expectedBlocks: []*types.Block{
					{
						Header: &types.BlockHeader{
							Number: 3,
						},
						TransactionEnvelopes: []*types.TransactionEnvelope{
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
			},
			{
				blockCreator: blockCreatorWithLastBlockNumber5,
				txBatches: [][]*types.TransactionEnvelope{
					{
						{
							Signature: []byte("sign-1"),
						},
					},
				},
				expectedBlocks: []*types.Block{
					{
						Header: &types.BlockHeader{
							Number: 6,
						},
						TransactionEnvelopes: []*types.TransactionEnvelope{
							{
								Signature: []byte("sign-1"),
							},
						},
					},
				},
			},
		}

		for _, tt := range testCases {
			b := tt.blockCreator
			for _, txBatch := range tt.txBatches {
				b.txBatchQueue.Enqueue(txBatch)
			}

			hasBlockCountMatched := func() bool {
				if len(tt.expectedBlocks) == b.blockQueue.Size() {
					return true
				}
				return false
			}
			require.Eventually(t, hasBlockCountMatched, 2*time.Second, 100*time.Millisecond)

			for _, expectedBlock := range tt.expectedBlocks {
				block := b.blockQueue.Dequeue().(*types.Block)
				require.True(t, proto.Equal(expectedBlock, block))
			}
		}
	})
}
