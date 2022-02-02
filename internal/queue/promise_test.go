// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"testing"
	"time"

	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestPromise_Done(t *testing.T) {
	t.Run("As used in sync tx", func(t *testing.T) {
		promise := NewCompletionPromise(time.Hour)

		go func() {
			time.Sleep(5 * time.Millisecond)
			promise.done(
				&types.TxReceipt{
					Header:  &types.BlockHeader{},
					TxIndex: 666,
				},
			)
		}()

		receipt, err := promise.Wait()
		require.NotNil(t, receipt)
		require.Equal(t, uint64(666), receipt.TxIndex)
		require.NoError(t, err)
	})

	t.Run("Done before Wait", func(t *testing.T) {
		promise := NewCompletionPromise(time.Hour)

		promise.done(
			&types.TxReceipt{
				Header:  &types.BlockHeader{},
				TxIndex: 666,
			},
		)
		receipt, err := promise.Wait()
		require.NotNil(t, receipt)
		require.Equal(t, uint64(666), receipt.TxIndex)
		require.NoError(t, err)

		checkPromiseAfterCompletion(t, promise)
	})

}

func checkPromiseAfterCompletion(t *testing.T, promise *CompletionPromise) {
	require.Panics(t, func() {
		promise.done(&types.TxReceipt{})
	})

	require.Panics(t, func() {
		promise.error(errors.New("oops"))
	})

	receipt, err := promise.Wait()
	require.Nil(t, receipt)
	require.EqualError(t, err, "closed: promise consumed")
}

func TestPromise_Error(t *testing.T) {
	t.Run("As used in sync tx", func(t *testing.T) {
		promise := NewCompletionPromise(time.Hour)

		go func() {
			time.Sleep(5 * time.Millisecond)
			promise.error(&ierrors.NotLeaderError{LeaderID: 3, LeaderHostPort: "10.10.10.10:12345"})
		}()

		receipt, err := promise.Wait()
		require.Nil(t, receipt)
		require.EqualError(t, err, "not a leader, leader is RaftID: 3, with HostPort: 10.10.10.10:12345")
		require.IsType(t, &ierrors.NotLeaderError{}, err)
	})

	t.Run("Error before Wait", func(t *testing.T) {
		promise := NewCompletionPromise(time.Hour)

		promise.error(errors.New("oops"))
		receipt, err := promise.Wait()
		require.Nil(t, receipt)
		require.EqualError(t, err, "oops")

		checkPromiseAfterCompletion(t, promise)
	})
}

func TestPromise_Nil(t *testing.T) {
	// A nil promise is used in an async tx
	var promise *CompletionPromise

	receipt, err := promise.Wait()
	require.Nil(t, receipt)
	require.NoError(t, err)
	promise.done(&types.TxReceipt{Header: &types.BlockHeader{}, TxIndex: 555})
	promise.error(errors.New("oops"))
}

func TestPromise_Timeout(t *testing.T) {
	t.Run("TO then Done", func(t *testing.T) {
		promise := NewCompletionPromise(time.Microsecond)

		receipt, err := promise.Wait()
		require.Nil(t, receipt)
		require.EqualError(t, err, "timeout has occurred while waiting for the transaction receipt")
		require.IsType(t, &ierrors.TimeoutErr{}, err)

		promise.done(&types.TxReceipt{Header: &types.BlockHeader{}, TxIndex: 666}) //should not panic
		receipt, err = promise.Wait()
		require.NotNil(t, receipt)
		require.Equal(t, uint64(666), receipt.TxIndex)
		require.NoError(t, err)
	})

	t.Run("TO then Error", func(t *testing.T) {
		promise := NewCompletionPromise(time.Microsecond)

		receipt, err := promise.Wait()
		require.Nil(t, receipt)
		require.EqualError(t, err, "timeout has occurred while waiting for the transaction receipt")
		require.IsType(t, &ierrors.TimeoutErr{}, err)

		promise.error(errors.New("oops")) //should not panic
		receipt, err = promise.Wait()
		require.Nil(t, receipt)
		require.EqualError(t, err, "oops")
	})
}
