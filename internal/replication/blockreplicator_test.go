// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	ierrors "github.ibm.com/blockchaindb/server/internal/errors"
	"github.ibm.com/blockchaindb/server/internal/queue"
	"github.ibm.com/blockchaindb/server/internal/replication"
	"github.ibm.com/blockchaindb/server/pkg/logger"
)

func TestBlockReplicator_StartClose(t *testing.T) {
	lg := testLogger(t, "debug")
	qBarrier := queue.NewOneQueueBarrier(lg)
	conf := &replication.Config{BlockOneQueueBarrier: qBarrier, Logger: lg}
	blockReplicator := replication.NewBlockReplicator(conf)

	blockReplicator.Start()
	err := blockReplicator.Close()
	require.NoError(t, err)

	err = qBarrier.Close()
	require.EqualError(t, err, "closed")

	err = blockReplicator.Close()
	require.EqualError(t, err, "block replicator already closed")
}

func TestBlockReplicator_Submit(t *testing.T) {
	t.Run("normal flow", func(t *testing.T) {
		lg := testLogger(t, "debug")
		qBarrier := queue.NewOneQueueBarrier(lg)
		conf := &replication.Config{BlockOneQueueBarrier: qBarrier, Logger: lg}
		blockReplicator := replication.NewBlockReplicator(conf)
		blockReplicator.Start()

		block := "block number 1"
		err := blockReplicator.Submit(block)
		require.NoError(t, err)
		block2commit, err := qBarrier.Dequeue()
		require.NoError(t, err)
		require.NotNil(t, block2commit)
		require.Equal(t, block, block2commit.(string))
		err = qBarrier.Reply(nil)
		require.NoError(t, err)

		block = "block number 2"
		err = blockReplicator.Submit(block)
		require.NoError(t, err)
		block2commit, err = qBarrier.Dequeue()
		require.NoError(t, err)
		require.NotNil(t, block2commit)
		require.Equal(t, block, block2commit.(string))
		err = qBarrier.Reply(nil)
		require.NoError(t, err)

		err = blockReplicator.Close()
		require.NoError(t, err)
	})

	t.Run("normal flow: close before reply", func(t *testing.T) {
		lg := testLogger(t, "debug")
		qBarrier := queue.NewOneQueueBarrier(lg)
		conf := &replication.Config{BlockOneQueueBarrier: qBarrier, Logger: lg}
		blockReplicator := replication.NewBlockReplicator(conf)
		blockReplicator.Start()

		block := "block number 1"
		err := blockReplicator.Submit(block)
		require.NoError(t, err)
		block2commit, err := qBarrier.Dequeue()
		require.NoError(t, err)
		require.NotNil(t, block2commit)
		require.Equal(t, block, block2commit.(string))

		err = blockReplicator.Close()
		require.NoError(t, err)
	})

	t.Run("normal flow: blocked submit", func(t *testing.T) {
		lg := testLogger(t, "debug")
		qBarrier := queue.NewOneQueueBarrier(lg)
		conf := &replication.Config{BlockOneQueueBarrier: qBarrier, Logger: lg}
		blockReplicator := replication.NewBlockReplicator(conf)
		blockReplicator.Start()

		var wg sync.WaitGroup
		wg.Add(1)
		block := "some block"
		submitManyTillClosed := func() {
			for i := 0; i < 1000; i++ { // larger then the raft pipeline channel
				err := blockReplicator.Submit(block)
				if i == 0 {
					require.NoError(t, err)
				}
				if err != nil {
					wg.Done()
					require.EqualError(t, err, "block replicator closed")
					require.IsType(t, &ierrors.ClosedError{}, err)
					break
				}
			}
		}

		go submitManyTillClosed()

		block2commit, err := qBarrier.Dequeue()
		require.NoError(t, err)
		require.NotNil(t, block2commit)
		require.Equal(t, block, block2commit.(string))
		err = qBarrier.Reply(nil)
		require.NoError(t, err)

		err = blockReplicator.Close()
		require.NoError(t, err)

		wg.Wait()
	})
}

func testLogger(t *testing.T, level string) *logger.SugarLogger {
	c := &logger.Config{
		Level:         level,
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	lg, err := logger.New(c)
	require.NoError(t, err)
	return lg
}
