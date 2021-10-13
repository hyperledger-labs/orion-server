// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queue_test

import (
	"sync"
	"testing"

	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/queue"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestNewOneQueueBarrier(t *testing.T) {
	qb := queue.NewOneQueueBarrier(testLogger(t, "debug"))
	require.NotNil(t, qb)
}

func TestOneQueueBarrier_EnqueueWait(t *testing.T) {
	t.Run("Close release producing go-routine", func(t *testing.T) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		qb := queue.NewOneQueueBarrier(testLogger(t, "debug"))
		require.NotNil(t, qb)
		go testEnqueueFunc(t, qb, wg, 1, nil, &ierrors.ClosedError{ErrMsg: "closed"})
		qb.Close()
		wg.Wait()
	})

	t.Run("Reply release producing go-routine", func(t *testing.T) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		qb := queue.NewOneQueueBarrier(testLogger(t, "debug"))
		require.NotNil(t, qb)
		go testEnqueueFunc(t, qb, wg, 1, 2, nil)
		entry, err := qb.Dequeue()
		require.NoError(t, err)
		require.Equal(t, 1, entry)
		err = qb.Reply(2)
		require.NoError(t, err)
		wg.Wait()
	})

	t.Run("Close before reply release producing go-routine", func(t *testing.T) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		qb := queue.NewOneQueueBarrier(testLogger(t, "debug"))
		require.NotNil(t, qb)
		go testEnqueueFunc(t, qb, wg, 1, nil, &ierrors.ClosedError{ErrMsg: "closed"})
		entry, err := qb.Dequeue()
		require.NoError(t, err)
		require.Equal(t, 1, entry)
		err = qb.Close()
		require.NoError(t, err)
		wg.Wait()
	})
}

func TestOneQueueBarrier_Dequeue(t *testing.T) {
	t.Run("Close releases consuming go-routine", func(t *testing.T) {
		wg := &sync.WaitGroup{}
		wg.Add(1)
		qb := queue.NewOneQueueBarrier(testLogger(t, "debug"))
		require.NotNil(t, qb)
		go testDequeueFunc(t, qb, wg, nil, &ierrors.ClosedError{ErrMsg: "closed"})
		qb.Close()
		wg.Wait()
	})

	t.Run("Enqueue releases consuming go-routine", func(t *testing.T) {
		qb := queue.NewOneQueueBarrier(testLogger(t, "debug"))
		require.NotNil(t, qb)

		wgC := &sync.WaitGroup{}
		wgC.Add(1)
		go testDequeueFunc(t, qb, wgC, 1, nil)

		wgP := &sync.WaitGroup{}
		wgP.Add(1)
		go testEnqueueFunc(t, qb, wgP, 1, nil, &ierrors.ClosedError{ErrMsg: "closed"})
		wgC.Wait()

		qb.Close() // Close releases producing go-routine
		wgP.Wait()

		err := qb.Reply(2) // When QB is closed, Reply returns immediately
		require.EqualError(t, err, "closed")
	})

	t.Run("Enqueue releases consuming go-routine, reply releases producing", func(t *testing.T) {
		qb := queue.NewOneQueueBarrier(testLogger(t, "debug"))
		require.NotNil(t, qb)

		wgC := &sync.WaitGroup{}
		wgC.Add(1)
		go func() {
			testDequeueFunc(t, qb, wgC, 1, nil)
			err := qb.Reply(2)
			require.NoError(t, err)
		}()

		wgP := &sync.WaitGroup{}
		wgP.Add(1)
		go testEnqueueFunc(t, qb, wgP, 1, 2, nil)

		wgC.Wait()
		wgP.Wait()

		err := qb.Close()
		require.NoError(t, err)
	})

	t.Run("Enqueue-dequeue many objects", func(t *testing.T) {
		type datum struct {
			x int
			y int
		}

		qb := queue.NewOneQueueBarrier(testLogger(t, "info"))
		require.NotNil(t, qb)

		numEntries := 100
		prime := 113

		wgP := &sync.WaitGroup{}
		wgP.Add(numEntries)
		go func() {
			for i := 1; i <= numEntries; i++ {
				dIn := &datum{x: i, y: 0}
				dOut := &datum{x: i, y: i * prime}
				testEnqueueFunc(t, qb, wgP, dIn, dOut, nil)
			}
		}()

		wgC := &sync.WaitGroup{}
		wgC.Add(numEntries)
		go func() {
			for i := 1; i <= numEntries; i++ {
				expectedD := &datum{x: i, y: 0}
				entry := testDequeueFunc(t, qb, wgC, expectedD, nil)
				dOut := entry.(*datum)
				dOut.y = dOut.x * prime
				err := qb.Reply(dOut)
				require.NoError(t, err)
			}
		}()
		wgC.Wait()
		wgP.Wait()

	})
}

func testEnqueueFunc(t *testing.T, qb *queue.OneQueueBarrier, wg *sync.WaitGroup, entry interface{}, expectReply interface{}, expectErr error) {
	reply, err := qb.EnqueueWait(entry)
	if expectErr == nil {
		require.NoError(t, err)
		require.Equal(t, expectReply, reply)
	} else {
		require.EqualError(t, err, expectErr.Error())
		require.IsType(t, expectErr, err)
		require.Nil(t, reply)
	}
	wg.Done()
}

func testDequeueFunc(t *testing.T, qb *queue.OneQueueBarrier, wg *sync.WaitGroup, expectEntry interface{}, expectErr error) interface{} {
	entry, err := qb.Dequeue()
	if expectErr == nil {
		require.NoError(t, err)
		require.Equal(t, expectEntry, entry)
	} else {
		require.EqualError(t, err, expectErr.Error())
		require.IsType(t, expectErr, err)
		require.Nil(t, entry)
	}
	wg.Done()

	return entry
}

func testLogger(t *testing.T, level string) *logger.SugarLogger {
	c := &logger.Config{
		Level:         level,
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)
	return logger
}
