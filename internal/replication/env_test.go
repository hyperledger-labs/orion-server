package replication_test

import (
	"fmt"
	"math"
	"path"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/comm"
	"github.com/hyperledger-labs/orion-server/internal/queue"
	"github.com/hyperledger-labs/orion-server/internal/replication"
	"github.com/hyperledger-labs/orion-server/internal/replication/mocks"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
	pendingTxs      *mocks.PendingTxsReleaser
	configValidator *mocks.ConfigTxValidator
	stopServeCh     chan struct{}
	isRunning       bool
}

func createNodeEnv(t *testing.T, level string) *nodeEnv {
	lg := testLogger(t, level)
	testDir := t.TempDir()

	env, err := newNodeEnv(1, testDir, lg, clusterConfig1node)
	if err != nil {
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

	n.isRunning = true
	return nil
}

func (n *nodeEnv) Close() error {
	close(n.stopServeCh)
	err := n.blockReplicator.Close()
	n.conf.Transport.Close()

	n.isRunning = false
	return err
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
	n.conf.Transport, _ = comm.NewHTTPTransport(
		&comm.Config{
			LedgerReader: n.conf.LedgerReader,
			LocalConf:    n.conf.LocalConf,
			Logger:       n.conf.Logger,
		},
	)
	n.conf.BlockOneQueueBarrier = queue.NewOneQueueBarrier(n.conf.Logger)
	n.conf.PendingTxs = queue.NewPendingTxs(n.conf.Logger)
	n.stopServeCh = make(chan struct{})
	n.blockReplicator, err = replication.NewBlockReplicator(n.conf)
	if err != nil {
		return err
	}

	err = n.conf.Transport.SetConsensusListener(n.blockReplicator)
	if err != nil {
		return err
	}

	err = n.conf.Transport.SetClusterConfig(n.conf.ClusterConfig)
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
	nodes                 []*nodeEnv
	clusterConfigSequence []*types.ClusterConfig
	testDir               string
	lg                    *logger.SugarLogger
}

// create a clusterEnv
func createClusterEnv(t *testing.T, nNodes int, raftConf *types.RaftConfig, logLevel string, logOpts ...zap.Option) *clusterEnv {
	lg := testLogger(t, logLevel, logOpts...)

	testDir := t.TempDir()

	clusterConfig := &types.ClusterConfig{
		ConsensusConfig: &types.ConsensusConfig{
			Algorithm:  "raft",
			RaftConfig: raftConf,
		},
	}

	if raftConf == nil {
		clusterConfig.ConsensusConfig.RaftConfig = raftConfigNoSnapshots
	}
	clusterConfig.ConsensusConfig.RaftConfig.MaxRaftId = uint64(nNodes)

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

	cEnv := &clusterEnv{
		testDir: testDir,
		lg:      lg,
	}

	cEnv.clusterConfigSequence = append(cEnv.clusterConfigSequence, clusterConfig)
	for n := uint32(1); n <= uint32(nNodes); n++ {
		nEnv, err := newNodeEnv(n, testDir, lg, proto.Clone(clusterConfig).(*types.ClusterConfig))
		if err != nil {
			return nil
		}

		cEnv.nodes = append(cEnv.nodes, nEnv)
	}

	return cEnv
}

func destroyClusterEnv(t *testing.T, env *clusterEnv) {
	for _, node := range env.nodes {
		if node.isRunning {
			err := node.Close()
			assert.NoError(t, err)
		}
	}
}

// create a BlockReplicator environment with a genesis block
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
	pendingTxs := &mocks.PendingTxsReleaser{}
	configValidator := &mocks.ConfigTxValidator{}
	configValidator.ValidateReturns(&types.ValidationInfo{Flag: types.Flag_VALID}, nil)
	ledger := &memLedger{}
	genesisBlock := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number: 1,
			},
		},
		Payload: &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					NewConfig: clusterConfig,
				},
				Signature: []byte("sig"),
			},
		},
		ConsensusMetadata: nil,
	}
	if err := ledger.Append(genesisBlock); err != nil { //genesis block
		return nil, err
	}

	peerTransport, _ := comm.NewHTTPTransport(&comm.Config{
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
		PendingTxs:           pendingTxs,
		ConfigValidator:      configValidator,
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

	err = conf.Transport.SetClusterConfig(conf.ClusterConfig)
	if err != nil {
		return nil, err
	}

	env := &nodeEnv{
		testDir:         localTestDir,
		conf:            conf,
		blockReplicator: blockReplicator,
		ledger:          ledger,
		pendingTxs:      pendingTxs,
		configValidator: configValidator,
		stopServeCh:     make(chan struct{}),
	}

	return env, nil
}

// create a BlockReplicator environment with a join block
func newNodeEnvJoin(n uint32, testDir string, lg *logger.SugarLogger, clusterConfig *types.ClusterConfig, joinBlock *types.Block) (*nodeEnv, error) {
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
	pendingTxs := &mocks.PendingTxsReleaser{}
	configValidator := &mocks.ConfigTxValidator{}
	configValidator.ValidateReturns(&types.ValidationInfo{Flag: types.Flag_VALID}, nil)
	ledger := &memLedger{}

	peerTransport, _ := comm.NewHTTPTransport(&comm.Config{
		LedgerReader: ledger,
		LocalConf:    localConf,
		Logger:       lg,
	})

	conf := &replication.Config{
		LocalConf:            localConf,
		ClusterConfig:        clusterConfig,
		JoinBlock:            joinBlock,
		LedgerReader:         ledger,
		Transport:            peerTransport,
		BlockOneQueueBarrier: qBarrier,
		PendingTxs:           pendingTxs,
		ConfigValidator:      configValidator,
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

	err = conf.Transport.SetClusterConfig(conf.ClusterConfig)
	if err != nil {
		return nil, err
	}

	env := &nodeEnv{
		testDir:         localTestDir,
		conf:            conf,
		blockReplicator: blockReplicator,
		ledger:          ledger,
		pendingTxs:      pendingTxs,
		configValidator: configValidator,
		stopServeCh:     make(chan struct{}),
	}

	return env, nil
}

func (c *clusterEnv) NextNodeConfig() (nextRaftID uint32, config *types.ClusterConfig, configBlock *types.Block) {
	nextRaftID = uint32(len(c.nodes)) + 1
	num := len(c.clusterConfigSequence)
	updatedClusterConfig := proto.Clone(c.clusterConfigSequence[num-1]).(*types.ClusterConfig)
	nodeConfig := &types.NodeConfig{
		Id:          fmt.Sprintf("node%d", nextRaftID),
		Address:     "127.0.0.1",
		Port:        nodePortBase + nextRaftID,
		Certificate: []byte("bogus-cert"),
	}
	peerConfig := &types.PeerConfig{
		NodeId:   fmt.Sprintf("node%d", nextRaftID),
		RaftId:   uint64(nextRaftID),
		PeerHost: "127.0.0.1",
		PeerPort: peerPortBase + nextRaftID,
	}
	updatedClusterConfig.Nodes = append(updatedClusterConfig.Nodes, nodeConfig)
	updatedClusterConfig.ConsensusConfig.Members = append(updatedClusterConfig.ConsensusConfig.Members, peerConfig)
	c.clusterConfigSequence = append(c.clusterConfigSequence, updatedClusterConfig)

	configBlock = &types.Block{
		Header: &types.BlockHeader{BaseHeader: &types.BlockHeaderBase{Number: 1}},
		Payload: &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					UserId:    "admin",
					TxId:      fmt.Sprintf("config-add-%d", nextRaftID),
					NewConfig: updatedClusterConfig,
				},
			},
		},
	}

	return nextRaftID, updatedClusterConfig, configBlock
}

func (c *clusterEnv) AddNode(t *testing.T, n uint32, clusterConfig *types.ClusterConfig, joinBlock *types.Block) {
	node, err := newNodeEnvJoin(n, c.testDir, c.lg, clusterConfig, joinBlock)
	require.NoError(t, err)
	c.nodes = append(c.nodes, node)
	c.clusterConfigSequence = append(c.clusterConfigSequence, clusterConfig)
}

func (c *clusterEnv) UpdateConfig() {
	num := len(c.clusterConfigSequence)
	for _, node := range c.nodes {
		node.conf.ClusterConfig = c.clusterConfigSequence[num-1]
	}
}

func (c *clusterEnv) LastConfig() *types.ClusterConfig {
	num := len(c.clusterConfigSequence) - 1
	return proto.Clone(c.clusterConfigSequence[num-1]).(*types.ClusterConfig)
}

func (c *clusterEnv) RemoveFirstNodeConfig() (config *types.ClusterConfig, configBlock *types.Block) {
	config = c.LastConfig()
	removedID := config.ConsensusConfig.Members[0].RaftId
	config.Nodes = config.Nodes[1:]
	config.ConsensusConfig.Members = config.ConsensusConfig.Members[1:]
	c.clusterConfigSequence = append(c.clusterConfigSequence, config)

	configBlock = &types.Block{
		Header: &types.BlockHeader{BaseHeader: &types.BlockHeaderBase{Number: 1}},
		Payload: &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					UserId:    "admin",
					TxId:      fmt.Sprintf("config-add-%d", removedID),
					NewConfig: config,
				},
			},
		},
	}

	return config, configBlock
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

func (c *clusterEnv) SymmetricConnectivity(indices ...int) bool {
	if len(indices) == 0 {
		for i := 0; i < len(c.nodes); i++ {
			indices = append(indices, i)
		}
	}

	var leader uint64
	var activePeers map[string]*types.PeerConfig
	for k, idx := range indices {
		if k == 0 {
			leader, activePeers = c.nodes[idx].blockReplicator.GetClusterStatus()

			if len(activePeers) != len(indices) {
				return false
			}

			for _, idx := range indices {
				id := c.nodes[idx].conf.LocalConf.Server.Identity.ID
				if activePeer, ok := activePeers[id]; !ok || activePeer.NodeId != id {
					return false
				}
			}

			continue
		}

		l, ap := c.nodes[idx].blockReplicator.GetClusterStatus()
		if l != leader || !reflect.DeepEqual(ap, activePeers) {
			return false
		}
	}

	return true
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

func testLogger(t *testing.T, level string, opts ...zap.Option) *logger.SugarLogger {
	c := &logger.Config{
		Level:         level,
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	lg, err := logger.New(c, opts...)
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

func testDataBlock(approxDataSize int) (*types.Block, uint64) {
	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number:                 1,
				PreviousBaseHeaderHash: make([]byte, 16),
				LastCommittedBlockHash: make([]byte, 16),
				LastCommittedBlockNum:  1,
			},
		},
		Payload: &types.Block_DataTxEnvelopes{DataTxEnvelopes: &types.DataTxEnvelopes{
			Envelopes: []*types.DataTxEnvelope{
				{
					Payload: &types.DataTx{
						MustSignUserIds: []string{"alice"},
						TxId:            "txid",
						DbOperations: []*types.DBOperation{{
							DbName:     "my-db",
							DataWrites: []*types.DataWrite{{Key: "bogus", Value: make([]byte, approxDataSize)}},
						}},
					},
					Signatures: map[string][]byte{"alice": []byte("alice-sig")},
				},
			}}},
	}
	dataBlockLength := uint64(len(utils.MarshalOrPanic(block)))
	return block, dataBlockLength
}

func testSubmitDataBlocks(t *testing.T, env *clusterEnv, numBlocks uint64, approxDataSize int, indices ...int) {
	block, _ := testDataBlock(approxDataSize)
	leaderIdx := env.AgreedLeaderIndex()
	heightBefore, err := env.nodes[leaderIdx].ledger.Height()
	require.NoError(t, err, "Error: %s", err)
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return env.AssertEqualHeight(heightBefore+numBlocks, indices...) }, 30*time.Second, 100*time.Millisecond)
}
