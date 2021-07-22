// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication_test

import (
	"github.com/IBM-Blockchain/bcdb-server/internal/comm"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/config"
	ierrors "github.com/IBM-Blockchain/bcdb-server/internal/errors"
	"github.com/IBM-Blockchain/bcdb-server/internal/queue"
	"github.com/IBM-Blockchain/bcdb-server/internal/replication"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBlockReplicator_StartClose_Fast(t *testing.T) {
	env := testEnv1node(t, "info")
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
	env := testEnv1node(t, "info")
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
	env := testEnv1node(t, "info")
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
		env := testEnv1node(t, "debug")
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
		env := testEnv1node(t, "debug")
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
		env := testEnv1node(t, "debug")
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
			for i := 0; i < 1000; i++ { // larger then the raft pipeline
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
					require.EqualError(t, err, "block replicator closed")
					require.IsType(t, &ierrors.ClosedError{}, err)
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
		env := testEnv1node(t, "debug")
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

var clusterConfig1node = &types.ClusterConfig{
	Nodes: []*types.NodeConfig{&types.NodeConfig{
		Id:          "node1",
		Address:     "127.0.0.1",
		Port:        22001,
		Certificate: []byte("bogus-cert"),
	}},
	ConsensusConfig: &types.ConsensusConfig{
		Algorithm: "raft",
		Members: []*types.PeerConfig{{
			NodeId:   "node1",
			RaftId:   1,
			PeerHost: "127.0.0.1",
			PeerPort: 23001,
		}},
		RaftConfig: &types.RaftConfig{
			TickInterval:         "20ms",
			ElectionTicks:        100,
			HeartbeatTicks:       10,
			MaxInflightBlocks:    50,
			SnapshotIntervalSize: math.MaxUint64,
		},
	},
}

type testEnv struct {
	testDir         string
	conf            *replication.Config
	blockReplicator *replication.BlockReplicator
	ledger          *memLedger
}

func testEnv1node(t *testing.T, level string) *testEnv {
	lg := testLogger(t, level)

	testDir, err := ioutil.TempDir("", "replication-test")
	require.NoError(t, err)

	localConf := &config.LocalConfiguration{
		Server: config.ServerConf{
			Identity: config.IdentityConf{
				ID: "node1",
			},
		},
		Replication: config.ReplicationConf{
			WALDir:  path.Join(testDir, "wal"),
			SnapDir: path.Join(testDir, "snap"),
			Network: config.NetworkConf{
				Address: "127.0.0.1",
				Port:    23001,
			},
			TLS: config.TLSConf{
				Enabled: false,
			},
		},
	}

	peerTransport := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConf,
		Logger:    lg,
	})

	qBarrier := queue.NewOneQueueBarrier(lg)

	ledger := &memLedger{}
	proposedBlock := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number: 1,
			},
		},
	}
	err = ledger.Append(proposedBlock) //genesis block
	require.NoError(t, err)

	conf := &replication.Config{
		LocalConf:            localConf,
		ClusterConfig:        clusterConfig1node,
		LedgerReader:         ledger,
		Transport:            peerTransport,
		BlockOneQueueBarrier: qBarrier,
		Logger:               lg,
	}

	blockReplicator, err := replication.NewBlockReplicator(conf)
	if err != nil {
		os.RemoveAll(testDir)
		return nil
	}

	err = conf.Transport.SetConsensusListener(blockReplicator)
	if err != nil {
		os.RemoveAll(testDir)
		return nil
	}

	err = conf.Transport.UpdateClusterConfig(conf.ClusterConfig)
	if err != nil {
		os.RemoveAll(testDir)
		return nil
	}

	return &testEnv{
		testDir:         testDir,
		conf:            conf,
		blockReplicator: blockReplicator,
		ledger:          ledger,
	}
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

type memLedger struct {
	mutex  sync.Mutex
	ledger []*types.Block
}

func (l *memLedger) Height() (uint64, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	return uint64(len(l.ledger)), nil
}

func (l *memLedger) Append(block *types.Block) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if h := len(l.ledger); h > 0 {
		if l.ledger[h-1].GetHeader().GetBaseHeader().GetNumber()+1 != block.GetHeader().GetBaseHeader().Number {
			return errors.Errorf("block number [%d] out of sequence, expected [%d]",
				block.GetHeader().GetBaseHeader().Number, l.ledger[h-1].GetHeader().GetBaseHeader().GetNumber()+1)
		}
	}

	l.ledger = append(l.ledger, block)
	return nil
}

func (l *memLedger) Get(blockNum uint64) (*types.Block, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if blockNum-1 >= uint64(len(l.ledger)) {
		return nil, errors.Errorf("block number out of bounds: %d, len: %d", blockNum, len(l.ledger))
	}
	return l.ledger[blockNum-1], nil
}
