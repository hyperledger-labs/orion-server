// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package queue

import (
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
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

	q.Enqueue(txs[0])
	require.False(t, q.IsEmpty())
	require.Equal(t, txs[0], q.DequeueWithWaitLimit(1*time.Second).(*types.UserAdministrationTxEnvelope))

	blockedDequeue := func() bool {
		q.Dequeue()
		return true
	}
	require.Never(t, blockedDequeue, 1*time.Second, 100*time.Millisecond)

	blockedDequeueWithWaitLimit := func() bool {
		q.DequeueWithWaitLimit(100 * time.Millisecond)
		return true
	}
	require.Eventually(t, blockedDequeueWithWaitLimit, 1*time.Second, 100*time.Millisecond)

	q.Close()
	require.Nil(t, q.Dequeue())

	blockedDequeueWithWaitLimit = func() bool {
		tx := q.DequeueWithWaitLimit(1000 * time.Second)
		return tx == nil
	}
	// though we have set the wait limit to 1000 seconds, the function should return
	// immediately as the queue is closed
	require.Eventually(t, blockedDequeueWithWaitLimit, 1*time.Second, 100*time.Millisecond)
}
