// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication_test

import (
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/comm"
	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/queue"
	"github.com/hyperledger-labs/orion-server/internal/replication"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Scenario: start and close w/o waiting for a leader
func TestBlockReplicator_StartClose_Fast(t *testing.T) {
	env := createNodeEnv(t, "info")
	require.NotNil(t, env)

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

// Scenario: start, wait for the node to become a leader, and close
func TestBlockReplicator_StartClose_Slow(t *testing.T) {
	env := createNodeEnv(t, "info")
	require.NotNil(t, env)

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

// Scenario: start, close, and restart. No blocks submitted.
// The node is expected to restart from the "empty" snapshot and the genesis block.
func TestBlockReplicator_Restart(t *testing.T) {
	env := createNodeEnv(t, "info")
	require.NotNil(t, env)

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
	env.conf.Transport, _ = comm.NewHTTPTransport(&comm.Config{LocalConf: env.conf.LocalConf, Logger: env.conf.Logger})
	env.conf.BlockOneQueueBarrier = queue.NewOneQueueBarrier(env.conf.Logger)
	env.blockReplicator, err = replication.NewBlockReplicator(env.conf)
	require.NoError(t, err)
	err = env.conf.Transport.SetConsensusListener(env.blockReplicator)
	require.NoError(t, err)
	err = env.conf.Transport.SetClusterConfig(env.conf.ClusterConfig)
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

// Scenario: start, submit a block, check it is enqueued for commit, and:
// - close after commit replies
// - close while the block-replicator is blocked waiting for a reply,
// - close while the submit go-routine is busy submitting,
// - restart: the nod is expected to recover from an "empty" snapshot and a single entry, as no snapshots are taken.
func TestBlockReplicator_Submit(t *testing.T) {
	t.Run("normal flow", func(t *testing.T) {
		env := createNodeEnv(t, "info")
		require.NotNil(t, env)

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
			defer wg.Done()

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
					switch err.(type) {
					case *ierrors.NotLeaderError:
						require.EqualError(t, err, "not a leader, leader is RaftID: 0, with HostPort: ")
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
		require.Equal(t, uint64(2), block2commit.(*types.Block).GetHeader().GetBaseHeader().GetNumber())
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
		env.conf.Transport, _ = comm.NewHTTPTransport(&comm.Config{LocalConf: env.conf.LocalConf, Logger: env.conf.Logger})
		env.conf.BlockOneQueueBarrier = queue.NewOneQueueBarrier(env.conf.Logger)
		env.blockReplicator, err = replication.NewBlockReplicator(env.conf)
		require.NoError(t, err)
		err = env.conf.Transport.SetConsensusListener(env.blockReplicator)
		require.NoError(t, err)
		err = env.conf.Transport.SetClusterConfig(env.conf.ClusterConfig)
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

// Scenario: check that a config block is allowed to commit, if
// - it carries admin updates, or
// - it carries CA updates.
func TestBlockReplicator_ReConfig(t *testing.T) {
	t.Run("update admins", func(t *testing.T) {
		env := createNodeEnv(t, "info")
		require.NotNil(t, env)
		err := env.Start()
		require.NoError(t, err)

		// wait for the node to become leader
		isLeaderCond := func() bool {
			return env.blockReplicator.IsLeader() == nil
		}
		assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

		clusterConfig := proto.Clone(clusterConfig1node).(*types.ClusterConfig)
		clusterConfig.Admins = append(clusterConfig.Admins, &types.Admin{
			Id:          "another-admin",
			Certificate: []byte("something"),
		})

		proposeBlock := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number:                2,
					LastCommittedBlockNum: 0,
				},
			},
			Payload: &types.Block_ConfigTxEnvelope{
				ConfigTxEnvelope: &types.ConfigTxEnvelope{
					Payload: &types.ConfigTx{
						NewConfig: clusterConfig,
					},
				},
			},
		}

		err = env.blockReplicator.Submit(proposeBlock)
		require.NoError(t, err)

		heightCond := func() bool {
			h, err := env.ledger.Height()
			require.NoError(t, err)
			return h == uint64(2)
		}
		require.Eventually(t, heightCond, 30*time.Second, 100*time.Millisecond)

		err = env.blockReplicator.Close()
		require.NoError(t, err)
		env.conf.Transport.Close()
	})

	t.Run("update CAs", func(t *testing.T) {
		env := createNodeEnv(t, "info")
		require.NotNil(t, env)
		err := env.Start()
		require.NoError(t, err)

		// wait for the node to become leader
		isLeaderCond := func() bool {
			return env.blockReplicator.IsLeader() == nil
		}
		assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

		clusterConfig := proto.Clone(clusterConfig1node).(*types.ClusterConfig)
		clusterConfig.CertAuthConfig = &types.CAConfig{Roots: [][]byte{[]byte("root")}}

		proposeBlock := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number:                2,
					LastCommittedBlockNum: 0,
				},
			},
			Payload: &types.Block_ConfigTxEnvelope{
				ConfigTxEnvelope: &types.ConfigTxEnvelope{
					Payload: &types.ConfigTx{
						NewConfig: clusterConfig,
					},
				},
			},
		}

		err = env.blockReplicator.Submit(proposeBlock)
		require.NoError(t, err)

		heightCond := func() bool {
			h, err := env.ledger.Height()
			require.NoError(t, err)
			return h == uint64(2)
		}
		require.Eventually(t, heightCond, 30*time.Second, 100*time.Millisecond)

		err = env.blockReplicator.Close()
		require.NoError(t, err)
		env.conf.Transport.Close()
	})
}

// Scenario: check that snapshots are taken as expected, and can be recovered from.
func TestBlockReplicator_Snapshots(t *testing.T) {
	// Scenario:
	// - configure the cluster to take snapshots every 2 blocks,
	// - submit blocks and verify the last 4 snapshots exist.
	t.Run("take a snapshot every two blocks", func(t *testing.T) {
		lg := testLogger(t, "info")
		testDir := t.TempDir()

		block := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number:                1,
					LastCommittedBlockNum: 1,
				},
			},
			Payload: &types.Block_DataTxEnvelopes{},
		}
		blockData, err := proto.Marshal(block)
		require.NoError(t, err)
		blockLen := uint64(len(blockData))

		clusterConfig := proto.Clone(clusterConfig1node).(*types.ClusterConfig)
		clusterConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize = blockLen + 1 // take a snapshot every 2 blocks
		env, err := newNodeEnv(1, testDir, lg, clusterConfig)
		require.NoError(t, err)
		require.NotNil(t, env)

		err = env.Start()
		require.NoError(t, err)

		// wait for the node to become leader
		isLeaderCond := func() bool {
			return env.blockReplicator.IsLeader() == nil
		}
		assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

		numBlocks := uint64(20)
		for i := uint64(0); i < numBlocks; i++ {
			b := proto.Clone(block).(*types.Block)
			b.Header.BaseHeader.Number = 2 + i
			err := env.blockReplicator.Submit(b)
			require.NoError(t, err)

		}

		assert.Eventually(t, func() bool {
			h, err := env.ledger.Height()
			if err == nil && h == numBlocks+1 {
				return true
			}
			return false
		}, 30*time.Second, 100*time.Millisecond)

		err = env.blockReplicator.Close()
		require.NoError(t, err)
		env.conf.Transport.Close()

		snapList := replication.ListSnapshots(env.conf.Logger, env.conf.LocalConf.Replication.SnapDir)
		require.Equal(t, 4, len(snapList)) //implementation keeps last 4 snapshots
		t.Logf("Snap list: %v", snapList)
	})

	// Scenario:
	// - configure the cluster to take snapshots every 4 blocks;
	// - submit 20 blocks, which should create 4 snapshots exactly, with no trailing log entries;
	// - close the node and restart it, the node is expected to start from last snapshot;
	// - submit 6 blocks, which should create one more snapshot and two following entries;
	// - close the node and restart it, the node is expected to start from last snapshot, and recover two trailing entries;
	// - submit 6 blocks, and make sure the ledger is correct, then close.
	// Correct behavior after restart is checked by submitting more blocks and checking that they commit.
	t.Run("restart from a snapshot", func(t *testing.T) {
		lg := testLogger(t, "info")
		testDir := t.TempDir()

		block := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number:                 1,
					PreviousBaseHeaderHash: make([]byte, 32), //just to get the length right
					LastCommittedBlockHash: make([]byte, 32), //just to get the length right
					LastCommittedBlockNum:  0,
				},
			},
		}
		blockData, err := proto.Marshal(block)
		require.NoError(t, err)
		blockLen := uint64(len(blockData))

		clusterConfig := proto.Clone(clusterConfig1node).(*types.ClusterConfig)
		clusterConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize = 3*blockLen + blockLen/2 // take a snapshot every 4 data blocks
		env, err := newNodeEnv(1, testDir, lg, clusterConfig)
		require.NoError(t, err)
		require.NotNil(t, env)

		err = env.Start()
		require.NoError(t, err)

		// wait for the node to become leader
		isLeaderCond := func() bool {
			return env.blockReplicator.IsLeader() == nil
		}
		assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

		numBlocks := uint64(20)
		for i := uint64(0); i < numBlocks; i++ {
			b := proto.Clone(block).(*types.Block)
			b.Header.BaseHeader.Number = 2 + i
			err := env.blockReplicator.Submit(b)
			require.NoError(t, err)

		}
		assert.Eventually(t, func() bool {
			h, err := env.ledger.Height()
			if err == nil && h == numBlocks+1 {
				return true
			}
			return false
		}, 30*time.Second, 100*time.Millisecond)

		err = env.Close()
		require.NoError(t, err)

		snapList := replication.ListSnapshots(env.conf.Logger, env.conf.LocalConf.Replication.SnapDir)
		require.Equal(t, 4, len(snapList))
		t.Logf("Snap list after close: %v", snapList)

		//recreate - snapshot and no following entries
		err = env.Restart()
		require.NoError(t, err)

		isLeaderCond = func() bool {
			return env.blockReplicator.IsLeader() == nil
		}
		assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

		// add 6 blocks, to create another snapshot and 2 more entries
		numBlocksExtra := uint64(6)
		for i := numBlocks; i < numBlocks+numBlocksExtra; i++ {
			b := proto.Clone(block).(*types.Block)
			b.Header.BaseHeader.Number = 2 + i
			err := env.blockReplicator.Submit(b)
			require.NoError(t, err)
		}
		assert.Eventually(t, func() bool {
			h, err := env.ledger.Height()
			if err == nil && h == numBlocks+numBlocksExtra+1 {
				return true
			}
			return false
		}, 30*time.Second, 100*time.Millisecond)

		err = env.Close()
		require.NoError(t, err)

		snapList2 := replication.ListSnapshots(env.conf.Logger, env.conf.LocalConf.Replication.SnapDir)
		require.Equal(t, 4, len(snapList2))
		t.Logf("Snap list after 2nd close: %v", snapList2)
		require.Equal(t, snapList[3], snapList2[2])

		//recreate - from a snapshot and two following entries
		err = env.Restart()
		require.NoError(t, err)

		isLeaderCond = func() bool {
			return env.blockReplicator.IsLeader() == nil
		}
		assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

		// add another 6 blocks and make sure the ledger is correct
		numBlocks += numBlocksExtra
		for i := numBlocks; i < numBlocks+numBlocksExtra; i++ {
			b := proto.Clone(block).(*types.Block)
			b.Header.BaseHeader.Number = 2 + i
			err := env.blockReplicator.Submit(b)
			require.NoError(t, err)
		}
		assert.Eventually(t, func() bool {
			h, err := env.ledger.Height()
			if err == nil && h == numBlocks+numBlocksExtra+1 {
				return true
			}
			return false
		}, 30*time.Second, 100*time.Millisecond)

		err = env.Close()
		require.NoError(t, err)
	})
}
