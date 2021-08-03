// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication_test

import (
	"github.com/IBM-Blockchain/bcdb-server/internal/comm"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"

	ierrors "github.com/IBM-Blockchain/bcdb-server/internal/errors"
	"github.com/IBM-Blockchain/bcdb-server/internal/queue"
	"github.com/IBM-Blockchain/bcdb-server/internal/replication"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBlockReplicator_StartClose_Fast(t *testing.T) {
	env := createNodeEnv(t, "info")
	require.NotNil(t, env)
	defer os.RemoveAll(env.testDir)

	err := env.conf.Transport.Start()
	require.NoError(t, err)

	env.blockReplicator.Start()
	err = env.blockReplicator.Close()
	require.NoError(t, err)

	env.conf.Transport.Close()

	err = env.conf.BlockOneQueueBarrier.Close()
	require.EqualError(t, err, "closed")

	err = env.blockReplicator.Close()
	require.EqualError(t, err, "block replicator already closed")
}

func TestBlockReplicator_StartClose_Slow(t *testing.T) {
	env := createNodeEnv(t, "info")
	require.NotNil(t, env)
	defer os.RemoveAll(env.testDir)

	err := env.conf.Transport.Start()
	require.NoError(t, err)
	env.blockReplicator.Start()

	// wait for the node to become leader
	isLeaderCond := func() bool {
		return env.blockReplicator.IsLeader() == nil
	}
	assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	err = env.blockReplicator.Close()
	require.NoError(t, err)

	env.conf.Transport.Close()

	err = env.conf.BlockOneQueueBarrier.Close()
	require.EqualError(t, err, "closed")

	err = env.blockReplicator.Close()
	require.EqualError(t, err, "block replicator already closed")
}

func TestBlockReplicator_Restart(t *testing.T) {
	env := createNodeEnv(t, "info")
	require.NotNil(t, env)
	defer os.RemoveAll(env.testDir)

	err := env.conf.Transport.Start()
	require.NoError(t, err)
	env.blockReplicator.Start()

	// wait for the node to become leader
	isLeaderCond := func() bool {
		return env.blockReplicator.IsLeader() == nil
	}
	assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	err = env.blockReplicator.Close()
	require.NoError(t, err)
	env.conf.Transport.Close()

	//recreate
	env.conf.Transport = comm.NewHTTPTransport(&comm.Config{LocalConf: env.conf.LocalConf, Logger: env.conf.Logger})
	env.conf.BlockOneQueueBarrier = queue.NewOneQueueBarrier(env.conf.Logger)
	env.blockReplicator, err = replication.NewBlockReplicator(env.conf)
	require.NoError(t, err)
	err = env.conf.Transport.SetConsensusListener(env.blockReplicator)
	require.NoError(t, err)
	err = env.conf.Transport.UpdateClusterConfig(env.conf.ClusterConfig)
	require.NoError(t, err)

	//restart
	err = env.conf.Transport.Start()
	require.NoError(t, err)
	env.blockReplicator.Start()

	assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	err = env.blockReplicator.Close()
	require.NoError(t, err)
	env.conf.Transport.Close()
}

func TestBlockReplicator_Submit(t *testing.T) {
	t.Run("normal flow", func(t *testing.T) {
		env := createNodeEnv(t, "info")
		require.NotNil(t, env)
		defer os.RemoveAll(env.testDir)

		err := env.conf.Transport.Start()
		require.NoError(t, err)
		env.blockReplicator.Start()

		// wait for the node to become leader
		isLeaderCond := func() bool {
			return env.blockReplicator.IsLeader() == nil
		}
		assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

		proposeBlock := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number:                1,
					LastCommittedBlockNum: 0,
				},
			},
		}

		err = env.blockReplicator.Submit(proposeBlock)
		require.NoError(t, err)

		block2commit, err := env.conf.BlockOneQueueBarrier.Dequeue()
		require.NoError(t, err)
		require.NotNil(t, block2commit)
		require.True(t, proto.Equal(proposeBlock.GetHeader(), block2commit.(*types.Block).GetHeader()), "in: %+v, out: %+v", proposeBlock, block2commit)
		require.NotNil(t, block2commit.(*types.Block).GetConsensusMetadata())
		raftIndex := block2commit.(*types.Block).GetConsensusMetadata().GetRaftIndex()
		require.True(t, raftIndex > 0)
		err = env.conf.BlockOneQueueBarrier.Reply(nil)
		require.NoError(t, err)

		proposeBlock = &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number:                2,
					LastCommittedBlockNum: 1,
				},
			},
		}
		err = env.blockReplicator.Submit(proposeBlock)
		require.NoError(t, err)
		block2commit, err = env.conf.BlockOneQueueBarrier.Dequeue()
		require.NoError(t, err)
		require.NotNil(t, block2commit)
		require.True(t, proto.Equal(proposeBlock.GetHeader(), block2commit.(*types.Block).GetHeader()), "in: %+v, out: %+v", proposeBlock, block2commit)
		require.NotNil(t, block2commit.(*types.Block).GetConsensusMetadata())
		require.True(t, block2commit.(*types.Block).GetConsensusMetadata().GetRaftIndex() > raftIndex)
		err = env.conf.BlockOneQueueBarrier.Reply(nil)
		require.NoError(t, err)

		err = env.blockReplicator.Close()
		require.NoError(t, err)
		env.conf.Transport.Close()
	})

	t.Run("normal flow: close before commit reply", func(t *testing.T) {
		env := createNodeEnv(t, "info")
		require.NotNil(t, env)
		defer os.RemoveAll(env.testDir)

		err := env.conf.Transport.Start()
		require.NoError(t, err)
		env.blockReplicator.Start()

		// wait for the node to become leader
		isLeaderCond := func() bool {
			return env.blockReplicator.IsLeader() == nil
		}
		assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

		block := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number:                1,
					LastCommittedBlockNum: 0,
				},
			},
		}

		err = env.blockReplicator.Submit(block)
		require.NoError(t, err)

		block2commit, err := env.conf.BlockOneQueueBarrier.Dequeue()
		require.NoError(t, err)
		require.NotNil(t, block2commit)

		err = env.blockReplicator.Close()
		require.NoError(t, err)
		env.conf.Transport.Close()
	})

	t.Run("normal flow: blocked submit", func(t *testing.T) {
		env := createNodeEnv(t, "info")
		require.NotNil(t, env)
		defer os.RemoveAll(env.testDir)

		err := env.conf.Transport.Start()
		require.NoError(t, err)
		env.blockReplicator.Start()

		// wait for the node to become leader
		isLeaderCond := func() bool {
			return env.blockReplicator.IsLeader() == nil
		}
		assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

		var wg sync.WaitGroup
		wg.Add(1)

		submitManyTillClosed := func() {
			for i := 0; i < 1000; i++ { // larger than the raft pipeline
				block := &types.Block{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number:                uint64(i + 1),
							LastCommittedBlockNum: uint64(i),
						},
					},
				}

				err := env.blockReplicator.Submit(block)
				if i == 0 {
					require.NoError(t, err)
				}
				if err != nil {
					wg.Done()
					switch err.(type) {
					case *ierrors.NotLeaderError:
						require.EqualError(t, err, "not a leader, leader is: 0")
					case *ierrors.ClosedError:
						require.EqualError(t, err, "block replicator closed")
					default:
						t.Fail()
					}
					break
				}
			}
		}

		go submitManyTillClosed()

		block2commit, err := env.conf.BlockOneQueueBarrier.Dequeue()
		require.NoError(t, err)
		require.NotNil(t, block2commit)
		require.Equal(t, uint64(1), block2commit.(*types.Block).GetHeader().GetBaseHeader().GetNumber())
		err = env.conf.BlockOneQueueBarrier.Reply(nil)
		require.NoError(t, err)

		err = env.blockReplicator.Close()
		require.NoError(t, err)
		env.conf.Transport.Close()

		wg.Wait()
	})

	t.Run("restart", func(t *testing.T) {
		env := createNodeEnv(t, "info")
		require.NotNil(t, env)
		defer os.RemoveAll(env.testDir)

		err := env.conf.Transport.Start()
		require.NoError(t, err)
		env.blockReplicator.Start()

		// wait for the node to become leader
		isLeaderCond := func() bool {
			return env.blockReplicator.IsLeader() == nil
		}
		assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

		proposedBlock := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 2,
				},
			},
		}
		err = env.blockReplicator.Submit(proposedBlock)
		require.NoError(t, err)

		block2commit, err := env.conf.BlockOneQueueBarrier.Dequeue()
		require.NoError(t, err)
		err = env.ledger.Append(block2commit.(*types.Block))
		require.NoError(t, err)
		err = env.conf.BlockOneQueueBarrier.Reply(nil)
		require.NoError(t, err)

		proposedBlock = &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 3,
				},
			},
		}
		err = env.blockReplicator.Submit(proposedBlock)
		require.NoError(t, err)
		block2commit, err = env.conf.BlockOneQueueBarrier.Dequeue()
		require.NoError(t, err)
		err = env.ledger.Append(block2commit.(*types.Block))
		require.NoError(t, err)
		err = env.conf.BlockOneQueueBarrier.Reply(nil)
		require.NoError(t, err)

		// close
		err = env.blockReplicator.Close()
		require.NoError(t, err)
		env.conf.Transport.Close()

		//recreate
		env.conf.Transport = comm.NewHTTPTransport(&comm.Config{LocalConf: env.conf.LocalConf, Logger: env.conf.Logger})
		env.conf.BlockOneQueueBarrier = queue.NewOneQueueBarrier(env.conf.Logger)
		env.blockReplicator, err = replication.NewBlockReplicator(env.conf)
		require.NoError(t, err)
		err = env.conf.Transport.SetConsensusListener(env.blockReplicator)
		require.NoError(t, err)
		err = env.conf.Transport.UpdateClusterConfig(env.conf.ClusterConfig)
		require.NoError(t, err)

		//restart
		err = env.conf.Transport.Start()
		require.NoError(t, err)
		env.blockReplicator.Start()

		isLeaderCond = func() bool {
			return env.blockReplicator.IsLeader() == nil
		}
		assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

		proposedBlock = &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 4,
				},
			},
		}
		err = env.blockReplicator.Submit(proposedBlock)
		require.NoError(t, err)

		block2commit, err = env.conf.BlockOneQueueBarrier.Dequeue()
		require.NoError(t, err)
		err = env.ledger.Append(block2commit.(*types.Block))
		require.NoError(t, err)
		err = env.conf.BlockOneQueueBarrier.Reply(nil)
		require.NoError(t, err)

		// close
		err = env.blockReplicator.Close()
		require.NoError(t, err)
		env.conf.Transport.Close()
	})
}

