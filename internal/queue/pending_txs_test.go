// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queue_test

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/queue"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestPendingTxs_Async(t *testing.T) {
	pendingTxs := queue.NewPendingTxs(testLogger(t, "debug"))

	var p *queue.CompletionPromise
	require.True(t, pendingTxs.Empty())
	require.False(t, pendingTxs.Add("tx1", p))
	require.True(t, pendingTxs.Has("tx1"))
	require.False(t, pendingTxs.Has("tx2"))
	require.False(t, pendingTxs.Add("tx2", p))
	require.True(t, pendingTxs.Has("tx2"))
	pendingTxs.DoneWithReceipt([]string{"tx1", "tx2"}, nil)
	require.True(t, pendingTxs.Empty())
}

func TestPendingTxs_Sync(t *testing.T) {
	pendingTxs := queue.NewPendingTxs(testLogger(t, "debug"))

	blockHeader := &types.BlockHeader{
		BaseHeader: &types.BlockHeaderBase{
			Number:                5,
			LastCommittedBlockNum: 1,
		},
	}
	expectedReceipt := &types.TxReceipt{
		Header:  blockHeader,
		TxIndex: 0,
	}

	t.Run("Wait before Done", func(t *testing.T) {
		p := queue.NewCompletionPromise(time.Hour)
		require.False(t, pendingTxs.Add("tx3", p))

		go func() {
			time.Sleep(10 * time.Millisecond)
			pendingTxs.DoneWithReceipt([]string{"tx3"}, blockHeader)
		}()

		actualReceipt, err := p.Wait()
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedReceipt, actualReceipt))
	})

	t.Run("Done before Wait", func(t *testing.T) {
		p := queue.NewCompletionPromise(time.Hour)
		require.False(t, pendingTxs.Add("tx3", p))
		pendingTxs.DoneWithReceipt([]string{"tx3"}, blockHeader)
		actualReceipt, err := p.Wait()
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedReceipt, actualReceipt))
	})

	t.Run("Wait before Release with Error", func(t *testing.T) {
		p := queue.NewCompletionPromise(time.Hour)
		require.False(t, pendingTxs.Add("tx3", p))

		go func() {
			time.Sleep(10 * time.Millisecond)
			pendingTxs.ReleaseWithError([]string{"tx3"}, &ierrors.NotLeaderError{LeaderID: 1, LeaderHostPort: "10.10.10.10:666"})
		}()

		actualReceipt, err := p.Wait()
		require.EqualError(t, err, "not a leader, leader is RaftID: 1, with HostPort: 10.10.10.10:666")
		require.Nil(t, actualReceipt)
	})

	t.Run("Release with Error before Wait", func(t *testing.T) {
		p := queue.NewCompletionPromise(time.Hour)
		require.False(t, pendingTxs.Add("tx3", p))
		pendingTxs.ReleaseWithError([]string{"tx3"}, &ierrors.NotLeaderError{LeaderID: 1, LeaderHostPort: "10.10.10.10:666"})
		actualReceipt, err := p.Wait()
		require.EqualError(t, err, "not a leader, leader is RaftID: 1, with HostPort: 10.10.10.10:666")
		require.Nil(t, actualReceipt)
	})
}

func TestPendingTxs_Timeout(t *testing.T) {
	pendingTxs := queue.NewPendingTxs(testLogger(t, "debug"))

	p := queue.NewCompletionPromise(1 * time.Millisecond)
	require.False(t, pendingTxs.Add("tx3", p))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		receipt, err := p.Wait()
		require.EqualError(t, err, "timeout has occurred while waiting for the transaction receipt")
		require.Nil(t, receipt)
	}()

	wg.Wait()
	require.False(t, pendingTxs.Empty())
}
