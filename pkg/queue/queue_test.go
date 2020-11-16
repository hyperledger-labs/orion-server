package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestTransactionQueue(t *testing.T) {
	var txs []*types.UserAdministrationTxEnvelope
	for i := 0; i < 5; i++ {
		txs = append(txs, &types.UserAdministrationTxEnvelope{
			Payload: &types.UserAdministrationTx{
				UserReads:  []*types.UserRead{},
				UserWrites: []*types.UserWrite{},
			},
			Signature: []byte("sign"),
		})
	}

	q := New(5)
	require.Equal(t, 0, q.Size())
	require.False(t, q.IsFull())
	require.True(t, q.IsEmpty())

	for i := 0; i < 5; i++ {
		q.Enqueue(txs[i])
	}
	require.Equal(t, len(txs), q.Size())
	require.True(t, q.IsFull())
	require.False(t, q.IsEmpty())

	for i := 0; i < 5; i++ {
		require.Equal(t, len(txs)-i, q.Size())
		require.Equal(t, txs[i], q.Dequeue().(*types.UserAdministrationTxEnvelope))
	}
	require.Equal(t, 0, q.Size())
	require.False(t, q.IsFull())
	require.True(t, q.IsEmpty())

	blockedDequeue := func() bool {
		q.Dequeue()
		return true
	}
	require.Never(t, blockedDequeue, 1*time.Second, 100*time.Second)
}
