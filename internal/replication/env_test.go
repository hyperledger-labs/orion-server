package replication_test

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/config"
	"github.com/IBM-Blockchain/bcdb-server/internal/comm"
	"github.com/IBM-Blockchain/bcdb-server/internal/queue"
	"github.com/IBM-Blockchain/bcdb-server/internal/replication"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var nodePortBase = uint32(22000)
var peerPortBase = uint32(23000)

var raftConfigNoSnapshots = &types.RaftConfig{
	TickInterval:         "20ms",
	ElectionTicks:        100,
	HeartbeatTicks:       10,
	MaxInflightBlocks:    50,
	SnapshotIntervalSize: math.MaxUint64, // never take snapshots
}

var clusterConfig1node = &types.ClusterConfig{
	Nodes: []*types.NodeConfig{&types.NodeConfig{
		Id:          "node1",
		Address:     "127.0.0.1",
		Port:        nodePortBase + 1,
		Certificate: []byte("bogus-cert"),
	}},
	Admins: []*types.Admin{&types.Admin{
		Id:          "admin",
		Certificate: []byte("something"),
	}},
	ConsensusConfig: &types.ConsensusConfig{
		Algorithm: "raft",
		Members: []*types.PeerConfig{{
			NodeId:   "node1",
			RaftId:   1,
			PeerHost: "127.0.0.1",
			PeerPort: peerPortBase + 1,
		}},
		RaftConfig: raftConfigNoSnapshots,
	},
}

// A single node environment around a BlockReplicator
type nodeEnv struct {
	testDir         string
	conf            *replication.Config
	blockReplicator *replication.BlockReplicator
	ledger          *memLedger
	stopServeCh     chan struct{}
}

func createNodeEnv(t *testing.T, level string) *nodeEnv {
	lg := testLogger(t, level)
	testDir, err := ioutil.TempDir("", "replication-test")
	require.NoError(t, err)

	env, err := newNodeEnv(1, testDir, lg, clusterConfig1node)
	if err != nil {
		os.RemoveAll(testDir)
		return nil
	}
	env.testDir = testDir // clean the top level

	return env
}

func (n *nodeEnv) Start() error {
	if err := n.conf.Transport.Start(); err != nil {
		return err
	}
	n.blockReplicator.Start()

	go n.ServeCommit()

	return nil
}

func (n *nodeEnv) Close() error {
	close(n.stopServeCh)
	if err := n.blockReplicator.Close(); err != nil {
		return err
	}
	n.conf.Transport.Close()
	return nil
}

// Restart a closed node.
// The node must be closed before restarting it.
func (n *nodeEnv) Restart() error {
	var err error

	//verify node is closed
	select {
	case <-n.stopServeCh:
		break
	default:
		return errors.New("node must be closed before Restart")
	}

	//recreate
	n.conf.Transport = comm.NewHTTPTransport(
		&comm.Config{
			LedgerReader: n.conf.LedgerReader,
			LocalConf:    n.conf.LocalConf,
			Logger:       n.conf.Logger,
		},
	)
	n.conf.BlockOneQueueBarrier = queue.NewOneQueueBarrier(n.conf.Logger)
	n.stopServeCh = make(chan struct{})
	n.blockReplicator, err = replication.NewBlockReplicator(n.conf)
	if err != nil {
		return err
	}

	err = n.conf.Transport.SetConsensusListener(n.blockReplicator)
	if err != nil {
		return err
	}

	err = n.conf.Transport.UpdateClusterConfig(n.conf.ClusterConfig)
	if err != nil {
		return err
	}

	//restart
	err = n.Start()
	if err != nil {
		return err
	}

	return nil
}

func (n *nodeEnv) ServeCommit() {
	lg := n.conf.Logger
	lg.Debug("Starting to serve commit loop")
	for {
		select {
		case <-n.stopServeCh:
			lg.Info("Stopping to serve commit loop")
			return
		default:
			b, err := n.conf.BlockOneQueueBarrier.Dequeue()
			if err != nil {
				lg.Errorf("Stopping to serve commit loop, error: %s", err)
				return
			}
			block2commit := b.(*types.Block)
			err = n.ledger.Append(block2commit)
			if err != nil {
				lg.Panicf("Stopping to serve commit loop, error: %s", err)
				return
			}
			switch block2commit.Payload.(type) {
			case *types.Block_ConfigTxEnvelope:
				clusterConfig := block2commit.GetConfigTxEnvelope().GetPayload().GetNewConfig()
				err = n.conf.BlockOneQueueBarrier.Reply(clusterConfig)
				if err != nil {
					lg.Errorf("Stopping to serve commit loop, error: %s", err)
					return
				}
			default:
				err = n.conf.BlockOneQueueBarrier.Reply(nil)
				if err != nil {
					lg.Errorf("Stopping to serve commit loop, error: %s", err)
					return
				}
			}
		}
	}
}

// A cluster environment around a set of BlockReplicator objects.
type clusterEnv struct {
	nodes   []*nodeEnv
	testDir string
}

// create a clusterEnv
func createClusterEnv(t *testing.T, logLevel string, nNodes int, raftConf *types.RaftConfig) *clusterEnv {
	lg := testLogger(t, logLevel)

	testDir, err := ioutil.TempDir("", "replication-test")
	require.NoError(t, err)

	clusterConfig := &types.ClusterConfig{
		ConsensusConfig: &types.ConsensusConfig{
			Algorithm:  "raft",
			RaftConfig: raftConf,
		},
	}

	if raftConf == nil {
		clusterConfig.ConsensusConfig.RaftConfig = raftConfigNoSnapshots
	}

	for n := uint32(1); n <= uint32(nNodes); n++ {
		nodeID := fmt.Sprintf("node%d", n)
		nodeConfig := &types.NodeConfig{
			Id:          nodeID,
			Address:     "127.0.0.1",
			Port:        nodePortBase + n,
			Certificate: []byte("bogus-cert"),
		}
		peerConfig := &types.PeerConfig{
			NodeId:   nodeID,
			RaftId:   uint64(n),
			PeerHost: "127.0.0.1",
			PeerPort: peerPortBase + n,
		}
		clusterConfig.Nodes = append(clusterConfig.Nodes, nodeConfig)
		clusterConfig.ConsensusConfig.Members = append(clusterConfig.ConsensusConfig.Members, peerConfig)
	}

	cEnv := &clusterEnv{testDir: testDir}

	for n := uint32(1); n <= uint32(nNodes); n++ {
		nEnv, err := newNodeEnv(n, testDir, lg, clusterConfig)
		if err != nil {
			os.RemoveAll(testDir)
			return nil
		}

		cEnv.nodes = append(cEnv.nodes, nEnv)
	}

	return cEnv
}

func newNodeEnv(n uint32, testDir string, lg *logger.SugarLogger, clusterConfig *types.ClusterConfig) (*nodeEnv, error) {
	nodeID := fmt.Sprintf("node%d", n)
	localTestDir := path.Join(testDir, nodeID)

	localConf := &config.LocalConfiguration{
		Server: config.ServerConf{
			Identity: config.IdentityConf{
				ID: nodeID,
			},
		},
		Replication: config.ReplicationConf{
			WALDir:  path.Join(testDir, nodeID, "wal"),
			SnapDir: path.Join(testDir, nodeID, "snap"),
			Network: config.NetworkConf{
				Address: "127.0.0.1",
				Port:    peerPortBase + n,
			},
			TLS: config.TLSConf{
				Enabled: false,
			},
		},
	}

	qBarrier := queue.NewOneQueueBarrier(lg)

	ledger := &memLedger{}
	proposedBlock := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number: 1,
			},
		},
	}
	if err := ledger.Append(proposedBlock); err != nil { //genesis block
		return nil, err
	}

	peerTransport := comm.NewHTTPTransport(&comm.Config{
		LedgerReader: ledger,
		LocalConf:    localConf,
		Logger:       lg,
	})

	conf := &replication.Config{
		LocalConf:            localConf,
		ClusterConfig:        clusterConfig,
		LedgerReader:         ledger,
		Transport:            peerTransport,
		BlockOneQueueBarrier: qBarrier,
		Logger:               lg,
	}

	blockReplicator, err := replication.NewBlockReplicator(conf)
	if err != nil {
		return nil, err
	}

	err = conf.Transport.SetConsensusListener(blockReplicator)
	if err != nil {
		return nil, err
	}

	err = conf.Transport.UpdateClusterConfig(conf.ClusterConfig)
	if err != nil {
		return nil, err
	}

	env := &nodeEnv{
		testDir:         localTestDir,
		conf:            conf,
		blockReplicator: blockReplicator,
		ledger:          ledger,
		stopServeCh:     make(chan struct{}),
	}

	return env, nil
}

// find the index [0,N) of the leader node, -1 if no leader.
func (c *clusterEnv) FindLeaderIndex() int {
	for idx, e := range c.nodes {
		leader := e.blockReplicator.IsLeader()
		if leader == nil {
			return idx
		}
	}

	return -1
}

// find the index [0,N) of the leader node, if all indices agree; -1 if no agreed leader.
func (c *clusterEnv) AgreedLeaderIndex(indices ...int) int {
	if len(indices) == 0 {
		for i := 0; i < len(c.nodes); i++ {
			indices = append(indices, i)
		}
	}
	leaderIdx := c.FindLeaderIndex()
	if leaderIdx < 0 {
		return leaderIdx
	}

	leaderRaftID := c.nodes[leaderIdx].blockReplicator.RaftID()

	for _, idx := range indices {
		node := c.nodes[idx]
		if node.blockReplicator.GetLeaderID() != leaderRaftID {
			return -1
		}
	}

	return leaderIdx
}

// find if all indices agree on a leader
func (c *clusterEnv) ExistsAgreedLeader(indices ...int) bool {
	return c.AgreedLeaderIndex(indices...) >= 0
}

// assert all the ledgers specified in 'indices' are of equal 'height'.
func (c *clusterEnv) AssertEqualHeight(height uint64, indices ...int) bool {
	if len(indices) == 0 {
		for i := 0; i < len(c.nodes); i++ {
			indices = append(indices, i)
		}
	}

	for _, idx := range indices {
		n := c.nodes[idx]
		if h, err := n.ledger.Height(); err != nil || h != height {
			return false
		}
	}
	return true
}

// assert all the ledgers specified in 'indices' are equal.
func (c *clusterEnv) AssertEqualLedger(indices ...int) error {
	if len(indices) == 0 {
		for i := 0; i < len(c.nodes); i++ {
			indices = append(indices, i)
		}
	}

	var prevNode *nodeEnv
	for i, idx := range indices {
		currNode := c.nodes[idx]
		if i > 0 {
			id1 := currNode.blockReplicator.RaftID()
			id2 := prevNode.blockReplicator.RaftID()
			h1, _ := currNode.ledger.Height()
			h2, _ := prevNode.ledger.Height()
			if h1 != h2 {
				return errors.Errorf("different heights, nodes RaftID: %d %d", id1, id2)
			}

			for h := uint64(1); h <= h1; h++ {
				b1, _ := currNode.ledger.Get(h)
				b2, _ := prevNode.ledger.Get(h)
				if !proto.Equal(b1, b2) {
					return errors.Errorf("different blocks, height %d, nodes RaftID: %d %d, blocks %+v %+v", h, id1, id2, b1, b2)
				}
			}

		}
		prevNode = currNode
	}

	return nil
}

// memLedger mocks the block processor, which commits blocks and keeps them in the ledger.
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
	} else if block.GetHeader().GetBaseHeader().Number != 1 {
		return errors.Errorf("first block number [%d] must be 1",
			block.GetHeader().GetBaseHeader().Number)
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

func TestNodeEnv_LifeCycle(t *testing.T) {
	t.Run("can't start twice", func(t *testing.T) {
		node := createNodeEnv(t, "info")
		err := node.Start()
		require.NoError(t, err)
		err = node.Start()
		require.EqualError(t, err, "error while creating a tcp listener: listen tcp 127.0.0.1:23001: bind: address already in use")
		err = node.Close()
		require.NoError(t, err)
	})
	t.Run("can't restart before close", func(t *testing.T) {
		node := createNodeEnv(t, "info")
		err := node.Start()
		require.NoError(t, err)
		err = node.Restart()
		require.EqualError(t, err, "node must be closed before Restart")
		err = node.Close()
		require.NoError(t, err)
	})
	t.Run("can't close twice", func(t *testing.T) {
		node := createNodeEnv(t, "info")
		err := node.Start()
		require.NoError(t, err)
		err = node.Close()
		require.NoError(t, err)
		require.Panics(t, func() {
			_ = node.Close()
		})
	})
}
