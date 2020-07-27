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
	t.Run("assemble-different-sized-blocks", func(t *testing.T) {
		a := NewAssembler(queue.New(10), queue.New(10))
		go a.Run()

		testCases := []struct {
			txBatches      [][]*types.TransactionEnvelope
			expectedBlocks []*types.Block
		}{
			{
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
		}

		for _, testCase := range testCases {
			for _, txBatch := range testCase.txBatches {
				a.txBatchQueue.Enqueue(txBatch)
			}

			hasBlockCountMatched := func() bool {
				if len(testCase.expectedBlocks) == a.blockQueue.Size() {
					return true
				}
				return false
			}
			require.Eventually(t, hasBlockCountMatched, 2*time.Second, 100*time.Millisecond)

			for _, expectedBlock := range testCase.expectedBlocks {
				block := a.blockQueue.Dequeue().(*types.Block)
				require.True(t, proto.Equal(expectedBlock, block))
			}
		}
	})
}
