// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/replication"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Scenario: reconfigure a node endpoint
// - start 3 nodes, submit some blocks, verify replication
// - stop node 3
// - submit some blocks, verify replication on 2 nodes
// - submit a config-tx that reflects the 3rd node new PeerConfig, a changed endpoint 127.0.0.1:23004
// - restart node 3 on the new endpoint
// - verify 3rd node is back to the cluster and catches up.
func TestBlockReplicator_ReConfig_Endpoint(t *testing.T) {
	var countMutex sync.Mutex
	var updatedCount int

	isCountEqual := func(num int) bool {
		countMutex.Lock()
		defer countMutex.Unlock()

		return updatedCount == num
	}

	clusterConfigHook := func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "New cluster config committed, going to apply to block replicator:") &&
			strings.Contains(entry.Message, "peer_port:23004") {
			countMutex.Lock()
			defer countMutex.Unlock()

			updatedCount++
		}
		return nil
	}

	env := createClusterEnv(t, 3, nil, "info", zap.Hooks(clusterConfigHook))
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.AgreedLeaderIndex() >= 0
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	block, _ := testDataBlock(0)

	leaderIdx := env.AgreedLeaderIndex()
	expectedNotLeaderErr := fmt.Sprintf("not a leader, leader is RaftID: %d, with HostPort: 127.0.0.1:2200%d", leaderIdx+1, leaderIdx+1)
	follower1 := (leaderIdx + 1) % 3
	follower2 := (leaderIdx + 2) % 3
	numBlocks := uint64(100)
	for i := uint64(0); i < numBlocks; i++ {
		b1 := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b1)
		require.NoError(t, err)

		// submission to a follower will cause an error
		b2 := proto.Clone(block).(*types.Block)
		err = env.nodes[follower1].blockReplicator.Submit(b2)
		require.EqualError(t, err, expectedNotLeaderErr)
		b3 := proto.Clone(block).(*types.Block)
		err = env.nodes[follower2].blockReplicator.Submit(b3)
		require.EqualError(t, err, expectedNotLeaderErr)
	}

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// stop node 3
	env.nodes[2].Close()

	// wait for some node [1,2] to become a leader
	isLeaderCond2 := func() bool {
		return env.AgreedLeaderIndex(0, 1) >= 0
	}
	require.Eventually(t, isLeaderCond2, 30*time.Second, 100*time.Millisecond)
	leaderIdx = env.AgreedLeaderIndex(0, 1)
	expectedNotLeaderErr = fmt.Sprintf("not a leader, leader is RaftID: %d, with HostPort: 127.0.0.1:2200%d", leaderIdx+1, leaderIdx+1)
	follower1 = (leaderIdx + 1) % 2
	for i := numBlocks; i < 2*numBlocks; i++ {
		b1 := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b1)
		require.NoError(t, err)

		// submission to a follower will cause an error
		b2 := proto.Clone(block).(*types.Block)
		err = env.nodes[follower1].blockReplicator.Submit(b2)
		require.EqualError(t, err, expectedNotLeaderErr)
	}

	// nodes [1,2] are in sync, node 3 is down
	require.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks+1, 0, 1) }, 30*time.Second, 100*time.Millisecond)

	// a config tx that updates the two running members
	env.nodes[2].conf.LocalConf.Replication.Network.Port++ // this will be the new port of node 3
	clusterConfig := proto.Clone(env.nodes[0].conf.ClusterConfig).(*types.ClusterConfig)
	clusterConfig.ConsensusConfig.Members[2].PeerPort = env.nodes[2].conf.LocalConf.Replication.Network.Port
	proposeBlock := &types.Block{
		Header: &types.BlockHeader{BaseHeader: &types.BlockHeaderBase{Number: 2}},
		Payload: &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					NewConfig: clusterConfig,
				},
			},
		},
	}
	err := env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return isCountEqual(2) }, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks+2, 0, 1) }, 30*time.Second, 100*time.Millisecond)

	countMutex.Lock()
	updatedCount = 0
	countMutex.Unlock()

	// restart node 3 on a new port
	env.nodes[2].Restart()

	// after re-config node3 catches up, and knows who the leader is
	require.Eventually(t, func() bool { return isCountEqual(1) }, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks + 2) }, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return env.SymmetricConnectivity() }, 10*time.Second, 1000*time.Millisecond)

	t.Log("Closing")
	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}
}

// Scenario: remove a peer from the cluster
// - start 5 nodes, wait for leader, submit a few blocks and verify reception by all
// - submit a config tx to remove a node that is NOT the leader
// - wait for a new leader, from remaining nodes
// - ensure removed node had shut-down replication and is detached from the cluster
func TestBlockReplicator_ReConfig_RemovePeer(t *testing.T) {
	env := createClusterEnv(t, 5, nil, "info")
	defer destroyClusterEnv(t, env)
	require.Equal(t, 5, len(env.nodes))

	numBlocks := uint64(10)
	leaderIdx := testReConfigPeerRemoveBefore(t, env, numBlocks)

	// a config tx that updates the membership by removing a peer that is NOT the leader
	removePeerIdx := (leaderIdx + 1) % 5
	remainingPeers := []int{0, 1, 2, 3, 4}
	remainingPeers = append(remainingPeers[:removePeerIdx], remainingPeers[removePeerIdx+1:]...)

	testReConfigPeerRemovePropose(t, env, leaderIdx, removePeerIdx, numBlocks)

	testReConfigPeerRemoveAfter(t, env, removePeerIdx, remainingPeers)
}

// Scenario: remove a leader from the cluster
// - start 5 nodes, wait for leader, submit a few blocks and verify reception by all
// - submit a config tx to remove a node that IS the leader
// - wait for a new leader, from remaining nodes
// - ensure removed node had shut-down replication and is detached from the cluster
func TestBlockReplicator_ReConfig_RemovePeerLeader(t *testing.T) {
	env := createClusterEnv(t, 5, nil, "info")
	defer destroyClusterEnv(t, env)
	require.Equal(t, 5, len(env.nodes))

	numBlocks := uint64(10)
	leaderIdx := testReConfigPeerRemoveBefore(t, env, numBlocks)

	// a config tx that updates the membership by removing a peer that IS the leader
	removePeerIdx := leaderIdx
	remainingPeers := []int{0, 1, 2, 3, 4}
	remainingPeers = append(remainingPeers[:removePeerIdx], remainingPeers[removePeerIdx+1:]...)

	testReConfigPeerRemovePropose(t, env, leaderIdx, removePeerIdx, numBlocks)

	testReConfigPeerRemoveAfter(t, env, removePeerIdx, remainingPeers)
}

func testReConfigPeerRemoveBefore(t *testing.T, env *clusterEnv, numBlocks uint64) int {
	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.AgreedLeaderIndex() >= 0
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	block, _ := testDataBlock(0)

	leaderIdx := env.AgreedLeaderIndex()
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)
	return leaderIdx
}

// a config tx that updates the membership by removing a peer
func testReConfigPeerRemovePropose(t *testing.T, env *clusterEnv, leaderIdx, removePeerIdx int, numBlocks uint64) {
	t.Logf("Leader RaftID: %d, Removing RaftID: %d", leaderIdx+1, removePeerIdx+1)

	updatedClusterConfig := proto.Clone(env.nodes[0].conf.ClusterConfig).(*types.ClusterConfig)
	updatedClusterConfig.Nodes = append(updatedClusterConfig.Nodes[:removePeerIdx], updatedClusterConfig.Nodes[removePeerIdx+1:]...)
	updatedClusterConfig.ConsensusConfig.Members = append(updatedClusterConfig.ConsensusConfig.Members[:removePeerIdx], updatedClusterConfig.ConsensusConfig.Members[removePeerIdx+1:]...)

	proposeBlock := &types.Block{
		Header: &types.BlockHeader{BaseHeader: &types.BlockHeaderBase{Number: 2}},
		Payload: &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					NewConfig: updatedClusterConfig,
				},
			},
		},
	}

	err := env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 2) }, 30*time.Second, 100*time.Millisecond)
}

func testReConfigPeerRemoveAfter(t *testing.T, env *clusterEnv, removePeerIdx int, remainingPeers []int) {
	// wait for some node to become a leader
	isLeaderCond2 := func() bool {
		return env.AgreedLeaderIndex(remainingPeers...) >= 0
	}
	require.Eventually(t, isLeaderCond2, 30*time.Second, 100*time.Millisecond)

	// make sure the removed node had detached from the cluster
	removedHasNoLeader := func() bool {
		err := env.nodes[removePeerIdx].blockReplicator.IsLeader()
		return err.Error() == "not a leader, leader is RaftID: 0, with HostPort: "
	}
	require.Eventually(t, removedHasNoLeader, 10*time.Second, 100*time.Millisecond)

	t.Log("Closing")
	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}
}

// Scenario: add a peer to the cluster
// - start 3 nodes, wait for leader, submit a few blocks and verify reception by all
// - submit a config tx to adds a 4th peer, wait for all 3 to get it
// - start the 4th peer with a join block derived from said config-tx
// - ensure the new node generates a snapshot from the join block
// - ensure the new node can restart from that snapshot
// - submit a few blocks and check that all nodes got them, including the 4th node
func TestBlockReplicator_ReConfig_AddPeer(t *testing.T) {
	var countMutex sync.Mutex
	var addedCount int

	nodeAddedHook := func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Applied config changes: [{Type:ConfChangeAddNode NodeID:4") {
			countMutex.Lock()
			defer countMutex.Unlock()

			addedCount++
		}
		return nil
	}

	isCountOver := func(num int) bool {
		countMutex.Lock()
		defer countMutex.Unlock()

		return addedCount >= num
	}

	env := createClusterEnv(t, 3, nil, "info", zap.Hooks(nodeAddedHook))
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.AgreedLeaderIndex() >= 0
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)
	leaderIdx := env.AgreedLeaderIndex()

	numBlocks := uint64(10)
	approxDataSize := 32
	testSubmitDataBlocks(t, env, numBlocks, approxDataSize)

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// a config tx that updates the membership by adding a 4th peer
	next, updatedClusterConfig, proposeBlock := env.NextNodeConfig()
	err := env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks+2, 0, 1, 2) }, 30*time.Second, 100*time.Millisecond)
	joinBlock, err := env.nodes[0].ledger.Get(numBlocks + 2)
	require.NoError(t, err)
	t.Logf("join-block: H: %+v, M: %+v", joinBlock.GetHeader(), joinBlock.GetConsensusMetadata())

	// wait for some node to become a leader
	isLeaderCond2 := func() bool {
		return env.AgreedLeaderIndex(0, 1, 2) >= 0
	}
	require.Eventually(t, isLeaderCond2, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return isCountOver(3) }, 30*time.Second, 100*time.Millisecond)

	// start the new node
	env.AddNode(t, next, updatedClusterConfig, joinBlock)
	env.UpdateConfig()
	err = env.nodes[next-1].Start()
	require.NoError(t, err)
	t.Logf("Started node: %d", next)

	// wait for some node to become a leader, including the 4th node
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	// make sure the 4th node created a snapshot from the join block
	snapList := replication.ListSnapshots(env.nodes[next-1].conf.Logger, env.nodes[next-1].conf.LocalConf.Replication.SnapDir)
	t.Logf("Snapshot list: %v", snapList)
	require.True(t, len(snapList) == 1)
	require.Equal(t, snapList[0], joinBlock.GetConsensusMetadata().GetRaftIndex())

	// restart the new node, to check it can recover from the join-block snapshot
	err = env.nodes[next-1].Close()
	require.NoError(t, err)
	err = env.nodes[next-1].Restart()
	require.NoError(t, err)
	t.Logf("Re-Started node: %d", next)

	// wait for some node to become a leader, including the 4th node
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	// submit a few blocks and check that all nodes got them, including the 4th node
	testSubmitDataBlocks(t, env, numBlocks, approxDataSize)

	t.Log("Closing")
	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}
}

// Scenario: add a peer to the cluster, with frequent snapshots
// - start 3 nodes, configured to take frequent snapshots, wait for leader,
// - submit a few blocks and verify reception by all, these blocks will create snapshots
// - submit a config tx to adds a 4th peer, wait for all 3 to get it
// - start the 4th peer with a join block derived from said config-tx
// - submit a few blocks and check that all nodes got them, including the 4th node
func TestBlockReplicator_ReConfig_AddPeer_WithSnapshots(t *testing.T) {
	block, dataBlockLength := testDataBlock(2048)
	raftConfig := proto.Clone(raftConfigNoSnapshots).(*types.RaftConfig)
	raftConfig.SnapshotIntervalSize = 4 * dataBlockLength
	t.Logf("configure frequent snapshots, 4x block size; block size: %d, SnapshotIntervalSize: %d", dataBlockLength, raftConfig.SnapshotIntervalSize)

	env := createClusterEnv(t, 3, raftConfig, "info")
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.AgreedLeaderIndex() >= 0
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	leaderIdx := env.AgreedLeaderIndex()
	numBlocks := uint64(10)
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// a config tx that updates the membership by adding a 4th peer
	next, updatedClusterConfig, proposeBlock := env.NextNodeConfig()
	err := env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks+2, 0, 1, 2) }, 30*time.Second, 100*time.Millisecond)
	joinBlock, err := env.nodes[0].ledger.Get(numBlocks + 2)
	require.NoError(t, err)
	t.Logf("join-block: H: %+v, M: %+v", joinBlock.GetHeader(), joinBlock.GetConsensusMetadata())
	t.Logf("join-block: Config: %+v", joinBlock.GetPayload().(*types.Block_ConfigTxEnvelope).ConfigTxEnvelope.GetPayload().GetNewConfig())

	// wait for some node to become a leader
	isLeaderCond2 := func() bool {
		return env.AgreedLeaderIndex(0, 1, 2) >= 0
	}
	require.Eventually(t, isLeaderCond2, 30*time.Second, 100*time.Millisecond)

	// start the new node
	env.AddNode(t, next, updatedClusterConfig, joinBlock)
	env.UpdateConfig()
	err = env.nodes[next-1].Start()
	require.NoError(t, err)
	t.Logf("Started node: %d", next)

	// wait for some node to become a leader, including the 4th node
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	// submit few blocks and check that all nodes got them, including the 4th node
	leaderIdx = env.AgreedLeaderIndex()
	for i := uint64(0); i < 1; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 3) }, 5*time.Second, 100*time.Millisecond)

	t.Log("Closing")
	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}
}

// Scenario: add a peer to the cluster, with frequent snapshots, and restarts
// - start 3 nodes, configured to take frequent snapshots, wait for leader,
// - submit a few blocks and verify reception by all, these blocks will create snapshots
// - submit a config tx to adds a 4th peer, wait for all 3 to get it
// - shut down the 3 nodes
// - start the 4th peer with a join block derived from said config-tx
// - on height==0, check: cluster status, is-leader
// - start node 1, check: on-boarding completes, cluster status, is-leader
// - start node 2, check: leader
// - submit a few blocks and check that 1,2,4 nodes got them
// - start node 3, confirm it catches up
func TestBlockReplicator_ReConfig_AddPeer_WithSnapshots_RestartsAfterJoin(t *testing.T) {
	block, dataBlockLength := testDataBlock(2048)
	raftConfig := proto.Clone(raftConfigNoSnapshots).(*types.RaftConfig)
	raftConfig.SnapshotIntervalSize = 4 * dataBlockLength
	t.Logf("configure frequent snapshots, 4x block size; block size: %d, SnapshotIntervalSize: %d", dataBlockLength, raftConfig.SnapshotIntervalSize)

	env := createClusterEnv(t, 3, raftConfig, "info")
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.AgreedLeaderIndex() >= 0
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	leaderIdx := env.AgreedLeaderIndex()
	numBlocks := uint64(10)
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// a config tx that updates the membership by adding a 4th peer
	next, updatedClusterConfig, proposeBlock := env.NextNodeConfig()
	err := env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks+2, 0, 1, 2) }, 30*time.Second, 100*time.Millisecond)
	joinBlock, err := env.nodes[0].ledger.Get(numBlocks + 2)
	require.NoError(t, err)
	t.Logf("join-block: H: %+v, M: %+v", joinBlock.GetHeader(), joinBlock.GetConsensusMetadata())
	t.Logf("join-block: Config: %+v", joinBlock.GetPayload().(*types.Block_ConfigTxEnvelope).ConfigTxEnvelope.GetPayload().GetNewConfig())

	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks + 2) }, 30*time.Second, 100*time.Millisecond)

	env.AddNode(t, next, updatedClusterConfig, joinBlock)
	env.UpdateConfig()

	t.Log("Closing nodes: 1,2,3")
	// stop nodes 1,2,3
	for i := 0; i < 3; i++ {
		err := env.nodes[i].Close()
		require.NoError(t, err)
	}

	// Start the new node - no peers
	err = env.nodes[next-1].Start()
	require.NoError(t, err)
	t.Logf("Started node: %d", next)

	leaderID, activePeers := env.nodes[next-1].blockReplicator.GetClusterStatus()
	require.Equal(t, uint64(0), leaderID)
	require.Len(t, activePeers, 1)
	_, existNode4 := activePeers[env.nodes[next-1].conf.LocalConf.Server.Identity.ID]
	require.True(t, existNode4)
	heightNode4, err := env.nodes[next-1].ledger.Height()
	require.NoError(t, err)
	require.Equal(t, uint64(0), heightNode4)
	require.EqualError(t, env.nodes[next-1].blockReplicator.IsLeader(), "not a leader, leader is RaftID: 0, with HostPort: ")
	require.Equal(t, uint64(0), env.nodes[next-1].blockReplicator.GetLeaderID())

	t.Log("Closing node 4")
	err = env.nodes[next-1].Close()
	require.NoError(t, err)

	t.Log("Restarting node 4")
	err = env.nodes[next-1].Restart()
	require.NoError(t, err)

	leaderID, activePeers = env.nodes[next-1].blockReplicator.GetClusterStatus()
	require.Equal(t, uint64(0), leaderID)
	require.Len(t, activePeers, 1)
	_, existNode4 = activePeers[env.nodes[next-1].conf.LocalConf.Server.Identity.ID]
	require.True(t, existNode4)
	heightNode4, err = env.nodes[next-1].ledger.Height()
	require.NoError(t, err)
	require.Equal(t, uint64(0), heightNode4)
	require.EqualError(t, env.nodes[next-1].blockReplicator.IsLeader(), "not a leader, leader is RaftID: 0, with HostPort: ")
	require.Equal(t, uint64(0), env.nodes[next-1].blockReplicator.GetLeaderID())

	t.Log("Restarting node 1")
	err = env.nodes[0].Restart()
	require.NoError(t, err)

	require.Eventually(t,
		func() bool {
			h4, err := env.nodes[next-1].ledger.Height()
			require.NoError(t, err)
			return h4 >= numBlocks+2
		},
		30*time.Second, 100*time.Millisecond)

	leaderID, activePeers = env.nodes[next-1].blockReplicator.GetClusterStatus()
	require.Equal(t, uint64(0), leaderID)
	_, existNode4 = activePeers[env.nodes[next-1].conf.LocalConf.Server.Identity.ID]
	require.True(t, existNode4)
	require.EqualError(t, env.nodes[next-1].blockReplicator.IsLeader(), "not a leader, leader is RaftID: 0, with HostPort: ")
	require.Equal(t, uint64(0), env.nodes[next-1].blockReplicator.GetLeaderID())

	t.Log("Restarting node 2")
	err = env.nodes[1].Restart()
	require.NoError(t, err)

	//nodes 1,2,4 select a leader, node 4 catches up
	require.Eventually(t, func() bool {
		return env.AgreedLeaderIndex(0, 1, 3) >= 0
	}, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks+2, 0, 1, 3) }, 30*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		_, activePeers = env.nodes[next-1].blockReplicator.GetClusterStatus()
		if len(activePeers) == 3 {
			return true
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	t.Log("Restarting node 3")
	err = env.nodes[2].Restart()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return env.AgreedLeaderIndex(0, 1, 2, 3) >= 0
	}, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks+2, 0, 1, 2, 3) }, 30*time.Second, 100*time.Millisecond)

	t.Log("Closing all")
	for _, i := range []int{0, 1, 2, 3} {
		err = env.nodes[i].Close()
		require.NoError(t, err)
	}
}

// Scenario: add a peer to the cluster, restarts after join
// - start 3 nodes, wait for leader,
// - submit a few blocks and verify reception by all,
// - submit a config tx to adds a 4th peer, wait for all 3 to get it
// - shut down the 3 nodes
// - start the 4th peer with a join block derived from said config-tx
// - on height==0, check: cluster status, is-leader
// - start node 1, check: on-boarding completes, cluster status, is-leader
// - start node 2, check: agreed leader
// - submit a few blocks and check that 1,2,4 nodes got them
// - start node 3, confirm it catches up
func TestBlockReplicator_ReConfig_AddPeer_RestartsAfterJoin(t *testing.T) {
	block, _ := testDataBlock(128)
	env := createClusterEnv(t, 3, nil, "info")
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.AgreedLeaderIndex() >= 0
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	leaderIdx := env.AgreedLeaderIndex()
	numBlocks := uint64(10)
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// a config tx that updates the membership by adding a 4th peer
	next, updatedClusterConfig, proposeBlock := env.NextNodeConfig()
	err := env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks+2, 0, 1, 2) }, 30*time.Second, 100*time.Millisecond)
	joinBlock, err := env.nodes[0].ledger.Get(numBlocks + 2)
	require.NoError(t, err)
	t.Logf("join-block: H: %+v, M: %+v", joinBlock.GetHeader(), joinBlock.GetConsensusMetadata())
	t.Logf("join-block: Config: %+v", joinBlock.GetPayload().(*types.Block_ConfigTxEnvelope).ConfigTxEnvelope.GetPayload().GetNewConfig())

	t.Log("Closing nodes: 1,2,3")
	// stop nodes 1,2,3
	for i := 0; i < 3; i++ {
		err := env.nodes[i].Close()
		require.NoError(t, err)
	}

	// Start the new node - no peers
	env.AddNode(t, next, updatedClusterConfig, joinBlock)
	env.UpdateConfig()
	err = env.nodes[next-1].Start()
	require.NoError(t, err)
	t.Logf("Started node: %d", next)

	leaderID, activePeers := env.nodes[next-1].blockReplicator.GetClusterStatus()
	require.Equal(t, uint64(0), leaderID)
	require.Len(t, activePeers, 1)
	_, existNode4 := activePeers[env.nodes[next-1].conf.LocalConf.Server.Identity.ID]
	require.True(t, existNode4)
	heightNode4, err := env.nodes[next-1].ledger.Height()
	require.NoError(t, err)
	require.Equal(t, uint64(0), heightNode4)
	require.EqualError(t, env.nodes[next-1].blockReplicator.IsLeader(), "not a leader, leader is RaftID: 0, with HostPort: ")
	require.Equal(t, uint64(0), env.nodes[next-1].blockReplicator.GetLeaderID())

	t.Log("Closing node 4")
	err = env.nodes[next-1].Close()
	require.NoError(t, err)

	t.Log("Restarting node 4")
	err = env.nodes[next-1].Restart()
	require.NoError(t, err)

	leaderID, activePeers = env.nodes[next-1].blockReplicator.GetClusterStatus()
	require.Equal(t, uint64(0), leaderID)
	require.Len(t, activePeers, 1)
	_, existNode4 = activePeers[env.nodes[next-1].conf.LocalConf.Server.Identity.ID]
	require.True(t, existNode4)
	heightNode4, err = env.nodes[next-1].ledger.Height()
	require.NoError(t, err)
	require.Equal(t, uint64(0), heightNode4)
	require.EqualError(t, env.nodes[next-1].blockReplicator.IsLeader(), "not a leader, leader is RaftID: 0, with HostPort: ")
	require.Equal(t, uint64(0), env.nodes[next-1].blockReplicator.GetLeaderID())

	t.Log("Restarting node 1")
	err = env.nodes[0].Restart()
	require.NoError(t, err)

	require.Eventually(t,
		func() bool {
			h4, err := env.nodes[next-1].ledger.Height()
			require.NoError(t, err)
			return h4 == numBlocks+2
		},
		30*time.Second, 100*time.Millisecond)

	leaderID, activePeers = env.nodes[next-1].blockReplicator.GetClusterStatus()
	require.Equal(t, uint64(0), leaderID)
	_, existNode4 = activePeers[env.nodes[next-1].conf.LocalConf.Server.Identity.ID]
	require.True(t, existNode4)
	heightNode4, err = env.nodes[next-1].ledger.Height()
	require.NoError(t, err)
	require.Equal(t, numBlocks+2, heightNode4)
	require.EqualError(t, env.nodes[next-1].blockReplicator.IsLeader(), "not a leader, leader is RaftID: 0, with HostPort: ")
	require.Equal(t, uint64(0), env.nodes[next-1].blockReplicator.GetLeaderID())

	t.Log("Restarting node 2")
	err = env.nodes[1].Restart()
	require.NoError(t, err)

	//nodes 1,2,4 select a leader
	require.Eventually(t, func() bool {
		return env.AgreedLeaderIndex(0, 1, 3) >= 0
	}, 30*time.Second, 100*time.Millisecond)

	leaderID, activePeers = env.nodes[next-1].blockReplicator.GetClusterStatus()
	require.True(t, leaderID > 0)
	require.Len(t, activePeers, 3)
	for _, i := range []int{0, 1, 3} {
		_, existNode := activePeers[env.nodes[i].conf.LocalConf.Server.Identity.ID]
		require.True(t, existNode)
	}

	// add some blocks
	leaderIdx = env.AgreedLeaderIndex(0, 1, 3)
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}

	t.Log("Restarting node 3")
	err = env.nodes[2].Restart()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return env.AgreedLeaderIndex(0, 1, 2, 3) >= 0
	}, 30*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks + 2) }, 30*time.Second, 100*time.Millisecond)

	t.Log("Closing all")
	for _, i := range []int{0, 1, 2, 3} {
		err = env.nodes[i].Close()
		require.NoError(t, err)
	}
}

// Scenario: add a peer to the cluster, with restarts
// - start 3 nodes, wait for leader,
// - submit a few blocks and verify reception by all,
// - submit a config tx to adds a 4th peer, wait for all 3 to get it
// - restart the 3 nodes, they re-apply the config change
// - start the 4th peer with a join block derived from said config-tx
func TestBlockReplicator_ReConfig_AddPeer_RestartBeforeJoin(t *testing.T) {
	block, _ := testDataBlock(128)
	env := createClusterEnv(t, 3, nil, "debug")
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.AgreedLeaderIndex() >= 0
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	leaderIdx := env.AgreedLeaderIndex()
	numBlocks := uint64(10)
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// a config tx that updates the membership by adding a 4th peer
	next, updatedClusterConfig, proposeBlock := env.NextNodeConfig()
	err := env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks+2, 0, 1, 2) }, 30*time.Second, 100*time.Millisecond)
	joinBlock, err := env.nodes[0].ledger.Get(numBlocks + 2)
	require.NoError(t, err)
	t.Logf("join-block: H: %+v, M: %+v", joinBlock.GetHeader(), joinBlock.GetConsensusMetadata())
	t.Logf("join-block: Config: %+v", joinBlock.GetPayload().(*types.Block_ConfigTxEnvelope).ConfigTxEnvelope.GetPayload().GetNewConfig())

	env.AddNode(t, next, updatedClusterConfig, joinBlock)
	env.UpdateConfig()

	t.Log("Stopping nodes: 1,2,3")
	// stop nodes 1,2,3
	for i := 0; i < 3; i++ {
		err := env.nodes[i].Close()
		require.NoError(t, err)
	}
	t.Log("Restarting nodes: 1,2,3")
	for i := 0; i < 3; i++ {
		err := env.nodes[i].Restart()
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return env.AgreedLeaderIndex(0, 1, 2) >= 0 }, 30*time.Second, 100*time.Millisecond)

	// Start the new node
	err = env.nodes[next-1].Start()
	require.NoError(t, err)
	t.Logf("Started node: %d", next)

	require.Eventually(t,
		func() bool {
			h4, err := env.nodes[next-1].ledger.Height()
			require.NoError(t, err)
			return h4 == numBlocks+2
		},
		30*time.Second, 100*time.Millisecond)

	//nodes 1,2,3,4 select a leader
	require.Eventually(t, func() bool {
		return env.AgreedLeaderIndex(0, 1, 2, 3) >= 0
	}, 30*time.Second, 100*time.Millisecond)

	leaderIdx = env.AgreedLeaderIndex()
	leaderID, activePeers := env.nodes[next-1].blockReplicator.GetClusterStatus()
	require.Equal(t, leaderID, env.nodes[leaderIdx].blockReplicator.RaftID())

	require.Eventually(t, func() bool {
		_, activePeers = env.nodes[next-1].blockReplicator.GetClusterStatus()
		if len(activePeers) == 4 {
			return true
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)

	t.Log("Closing all")
	for _, i := range []int{0, 1, 2, 3} {
		err = env.nodes[i].Close()
		require.NoError(t, err)
	}
}

// Scenario: add a peer to the cluster, with frequent snapshots, and restarts
// - start 3 nodes, configured to take frequent snapshots, wait for leader,
// - submit a few blocks and verify reception by all, these blocks will create snapshots
// - submit a config tx to adds a 4th peer, wait for all 3 to get it
// - submit a few blocks and verify reception by all, these blocks will create snapshots which capture config change
// - restart the 3 nodes
// - start the 4th peer with a join block derived from said config-tx
func TestBlockReplicator_ReConfig_AddPeer_WithSnapshots_RestartBeforeJoin(t *testing.T) {
	block, dataBlockLength := testDataBlock(2048)
	raftConfig := proto.Clone(raftConfigNoSnapshots).(*types.RaftConfig)
	raftConfig.SnapshotIntervalSize = 4 * dataBlockLength
	t.Logf("configure frequent snapshots, 4x block size; block size: %d, SnapshotIntervalSize: %d", dataBlockLength, raftConfig.SnapshotIntervalSize)

	env := createClusterEnv(t, 3, raftConfig, "debug")
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.AgreedLeaderIndex() >= 0
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	leaderIdx := env.AgreedLeaderIndex()
	numBlocks := uint64(10)
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// a config tx that updates the membership by adding a 4th peer
	next, updatedClusterConfig, proposeBlock := env.NextNodeConfig()
	err := env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks+2, 0, 1, 2) }, 30*time.Second, 100*time.Millisecond)
	joinBlock, err := env.nodes[0].ledger.Get(numBlocks + 2)
	require.NoError(t, err)
	t.Logf("join-block: H: %+v, M: %+v", joinBlock.GetHeader(), joinBlock.GetConsensusMetadata())
	t.Logf("join-block: Config: %+v", joinBlock.GetPayload().(*types.Block_ConfigTxEnvelope).ConfigTxEnvelope.GetPayload().GetNewConfig())

	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks+2, 0, 1, 2) }, 30*time.Second, 100*time.Millisecond)

	env.AddNode(t, next, updatedClusterConfig, joinBlock)
	env.UpdateConfig()

	t.Log("Stopping nodes: 1,2,3")
	// stop nodes 1,2,3
	for i := 0; i < 3; i++ {
		err := env.nodes[i].Close()
		require.NoError(t, err)
	}
	t.Log("Restarting nodes: 1,2,3")
	for i := 0; i < 3; i++ {
		err := env.nodes[i].Restart()
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return env.AgreedLeaderIndex(0, 1, 2) >= 0 }, 30*time.Second, 100*time.Millisecond)

	// Start the new node
	err = env.nodes[next-1].Start()
	require.NoError(t, err)
	t.Logf("Started node: %d", next)

	require.Eventually(t,
		func() bool {
			h4, err := env.nodes[next-1].ledger.Height()
			require.NoError(t, err)
			return h4 >= numBlocks+2
		},
		30*time.Second, 100*time.Millisecond)

	//nodes 1,2,3,4 select a leader
	require.Eventually(t, func() bool {
		return env.AgreedLeaderIndex(0, 1, 2, 3) >= 0
	}, 30*time.Second, 100*time.Millisecond)

	leaderIdx = env.AgreedLeaderIndex()
	leaderID, activePeers := env.nodes[next-1].blockReplicator.GetClusterStatus()
	require.Equal(t, leaderID, env.nodes[leaderIdx].blockReplicator.RaftID())
	require.Eventually(t, func() bool {
		_, activePeers = env.nodes[next-1].blockReplicator.GetClusterStatus()
		if len(activePeers) == 4 {
			return true
		}
		return false
	}, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks+2, 0, 1, 2, 3) }, 30*time.Second, 100*time.Millisecond)

	t.Log("Closing all")
	for _, i := range []int{0, 1, 2, 3} {
		err = env.nodes[i].Close()
		require.NoError(t, err)
	}
}

// Scenario: add and remove nodes until original IDs are all removed
func TestBlockReplicator_ReConfig_AddRemovePeersRolling(t *testing.T) {
	approxDataSize := 32
	numBlocks := uint64(10)

	env := createClusterEnv(t, 3, nil, "info")
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	require.Eventually(t, func() bool { return env.AgreedLeaderIndex() >= 0 }, 30*time.Second, 100*time.Millisecond)

	testSubmitDataBlocks(t, env, numBlocks, approxDataSize, 0, 1, 2)

	// === add 4th node ===
	testRollingAddNode(t, env, 0, 1, 2)

	// === remove the 1st node ===
	testRollingRemoveNode(t, env, 0, 1, 2, 3)

	// === add 5th node ===
	testRollingAddNode(t, env, 1, 2, 3)

	// === remove the 2nd node ===
	testRollingRemoveNode(t, env, 1, 2, 3, 4)

	// === remove the 3nd node ===
	testRollingRemoveNode(t, env, 2, 3, 4)

	// === add 6th node ===
	testRollingAddNode(t, env, 3, 4)

	t.Log("Closing")
	for idx := 3; idx <= 5; idx++ {
		err := env.nodes[idx].Close()
		require.NoError(t, err)
	}
}

// Scenario: add and remove nodes until original IDs are all removed, with frequent snapshots
func TestBlockReplicator_ReConfig_AddRemovePeersRolling_WithSnapshots(t *testing.T) {
	approxDataSize := 2048
	numBlocks := uint64(10)
	_, dataBlockLength := testDataBlock(approxDataSize)
	raftConfig := proto.Clone(raftConfigNoSnapshots).(*types.RaftConfig)
	raftConfig.SnapshotIntervalSize = 4 * dataBlockLength
	t.Logf("configure frequent snapshots, 4x block size; block size: %d, SnapshotIntervalSize: %d", dataBlockLength, raftConfig.SnapshotIntervalSize)

	env := createClusterEnv(t, 3, raftConfig, "info")
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	require.Eventually(t, func() bool { return env.AgreedLeaderIndex() >= 0 }, 30*time.Second, 100*time.Millisecond)

	testSubmitDataBlocks(t, env, numBlocks, approxDataSize)

	// === add 4th node ===
	testRollingAddNode(t, env, 0, 1, 2)

	// === remove the 1st node ===
	testRollingRemoveNode(t, env, 0, 1, 2, 3)

	// === add 5th node ===
	testRollingAddNode(t, env, 1, 2, 3)

	// === remove the 2nd node
	testRollingRemoveNode(t, env, 1, 2, 3, 4)

	// === remove the 3nd node
	testRollingRemoveNode(t, env, 2, 3, 4)

	// === add 6th node ===
	testRollingAddNode(t, env, 3, 4)

	t.Log("Closing")
	for idx := 3; idx <= 5; idx++ {
		err := env.nodes[idx].Close()
		require.NoError(t, err)
	}
}

func testRollingAddNode(t *testing.T, env *clusterEnv, indicesBefore ...int) {
	leaderIdx := env.AgreedLeaderIndex(indicesBefore...)
	heightBefore, err := env.nodes[leaderIdx].ledger.Height()
	require.NoError(t, err)

	// a config tx that updates the membership by adding a peer
	nextID, updatedClusterConfig, proposeBlock := env.NextNodeConfig()
	err = env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return env.AssertEqualHeight(heightBefore+1, indicesBefore...) }, 30*time.Second, 100*time.Millisecond)
	joinBlock, err := env.nodes[leaderIdx].ledger.Get(heightBefore + 1)
	require.NoError(t, err)
	t.Logf("Adding node: %d, join-block: H: %+v, M: %+v", nextID, joinBlock.GetHeader(), joinBlock.GetConsensusMetadata())

	// wait for some node to become a leader
	require.Eventually(t, func() bool { return env.AgreedLeaderIndex(indicesBefore...) >= 0 }, 30*time.Second, 100*time.Millisecond)

	// start the new node
	env.AddNode(t, nextID, updatedClusterConfig, joinBlock)
	env.UpdateConfig()
	err = env.nodes[nextID-1].Start()
	require.NoError(t, err)
	t.Logf("Started node: %d", nextID)

	// wait for some node to become a leader, including the added node
	indicesAfter := append(indicesBefore, int(nextID-1))
	require.Eventually(t, func() bool { return env.AgreedLeaderIndex(indicesAfter...) >= 0 }, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return env.AssertEqualHeight(heightBefore+1, indicesAfter...) }, 30*time.Second, 100*time.Millisecond)
}

func testRollingRemoveNode(t *testing.T, env *clusterEnv, removeIdx int, indicesAfter ...int) {
	err := env.nodes[removeIdx].Close()
	require.NoError(t, err)
	t.Logf("Closed node: %d", removeIdx+1)

	require.Eventually(t, func() bool { return env.AgreedLeaderIndex(indicesAfter...) >= 0 }, 30*time.Second, 100*time.Millisecond)
	leaderIdx := env.AgreedLeaderIndex(indicesAfter...)
	heightBefore, err := env.nodes[leaderIdx].ledger.Height()
	require.NoError(t, err)

	_, proposeBlock := env.RemoveFirstNodeConfig()
	err = env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return env.AssertEqualHeight(heightBefore+1, indicesAfter...) }, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return env.AgreedLeaderIndex(indicesAfter...) >= 0 }, 30*time.Second, 100*time.Millisecond)
	leaderIdx = env.AgreedLeaderIndex(indicesAfter...)
	t.Logf("Removed node: %d, leader is: %d", removeIdx+1, leaderIdx)
}

// Scenario: A config transaction that does not pass preorder validation is rejected.
// Two cases are tested:
// - The mock failure is generated due to an internal error in validation
// - The mock failure is generated due to an invalid config transaction
func TestBlockReplicator_ReConfig_PreorderValidation(t *testing.T) {
	approxDataSize := 32
	numBlocks := uint64(10)

	var countMutex sync.Mutex
	var rejectCount int

	rejectedConfigHook := func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "mock failure message") {
			countMutex.Lock()
			defer countMutex.Unlock()

			rejectCount++
		}
		return nil
	}

	isCountEqualGreater := func(num int) bool {
		countMutex.Lock()
		defer countMutex.Unlock()

		return rejectCount >= num
	}
	env := createClusterEnv(t, 3, nil, "info", zap.Hooks(rejectedConfigHook))
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader and submit some blocks
	require.Eventually(t, func() bool { return env.AgreedLeaderIndex() >= 0 }, 30*time.Second, 100*time.Millisecond)
	leaderIdx := env.AgreedLeaderIndex()
	testSubmitDataBlocks(t, env, numBlocks, approxDataSize)

	var releaseErrorsMutex sync.Mutex
	var releaseErrors []string
	env.nodes[leaderIdx].pendingTxs.ReleaseWithErrorCalls(func(txIDs []string, err error) {
		releaseErrorsMutex.Lock()
		releaseErrors = append(releaseErrors, err.Error())
		releaseErrorsMutex.Unlock()
	})

	// A config tx that updates the membership by adding a peer:

	// 1. will fail because of an emulated internal error in validation
	env.nodes[leaderIdx].configValidator.ValidateReturns(nil, errors.New("mock failure message"))
	_, _, proposeBlock := env.NextNodeConfig()
	err := env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return env.nodes[leaderIdx].pendingTxs.ReleaseWithErrorCallCount() == 1 }, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return isCountEqualGreater(1) }, 30*time.Second, 100*time.Millisecond)

	countMutex.Lock()
	rejectCount = 0
	countMutex.Unlock()

	// 2. will fail because of an emulated invalid config tx
	env.nodes[leaderIdx].configValidator.ValidateReturns(
		&types.ValidationInfo{Flag: types.Flag_INVALID_INCORRECT_ENTRIES, ReasonIfInvalid: "mock failure message"}, nil)
	_, _, proposeBlock = env.NextNodeConfig()
	err = env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return env.nodes[leaderIdx].pendingTxs.ReleaseWithErrorCallCount() == 2 }, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return isCountEqualGreater(1) }, 30*time.Second, 100*time.Millisecond)

	releaseErrorsMutex.Lock()
	require.Equal(t, "mock failure message", releaseErrors[0])                            // release with internal error
	require.Equal(t, "Invalid config tx, reason: mock failure message", releaseErrors[1]) // release with invalid config
	releaseErrorsMutex.Unlock()

	t.Log("Closing")
	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}
}

// Scenario:
// - Start 3 nodes together, wait for leader.
// - Submit config blocks fast, until the leader encounters the limit on no in-flight config blocks a few times.
func TestBlockReplicator_3Node_InFlightConfig(t *testing.T) {
	var countMutex sync.Mutex
	var inFlightLogMsgCount int

	inFlightLogMsgHook := func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "In-flight config block [") &&
			strings.Contains(entry.Message, "], waiting for block to commit") {
			countMutex.Lock()
			defer countMutex.Unlock()

			inFlightLogMsgCount++
		}
		return nil
	}

	isCountOver := func(num int) bool {
		countMutex.Lock()
		defer countMutex.Unlock()

		return inFlightLogMsgCount > num
	}

	raftConfig := proto.Clone(raftConfigNoSnapshots).(*types.RaftConfig)
	raftConfig.SnapshotIntervalSize = 1000000
	env := createClusterEnv(t, 3, raftConfig, "debug", zap.Hooks(inFlightLogMsgHook))
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.AgreedLeaderIndex() >= 0
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)
	leaderIdx := env.AgreedLeaderIndex()

	// submits config TXs fast until we hit the in-flight log message several times
	var numConfigBlocks uint64
	for numConfigBlocks = 1; ; numConfigBlocks++ {
		// a config tx that updates the SnapshotIntervalSize
		clusterConfig := proto.Clone(env.nodes[0].conf.ClusterConfig).(*types.ClusterConfig)
		clusterConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize = raftConfig.SnapshotIntervalSize + numConfigBlocks

		proposeBlock := &types.Block{
			Header: &types.BlockHeader{BaseHeader: &types.BlockHeaderBase{Number: 2}},
			Payload: &types.Block_ConfigTxEnvelope{
				ConfigTxEnvelope: &types.ConfigTxEnvelope{
					Payload: &types.ConfigTx{
						UserId:               "admin",
						TxId:                 fmt.Sprintf("config-%d", numConfigBlocks),
						ReadOldConfigVersion: &types.Version{BlockNum: numConfigBlocks, TxNum: 0},
						NewConfig:            clusterConfig,
					},
				},
			},
		}

		err := env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
		require.NoError(t, err)

		if isCountOver(20) {
			break
		}
	}

	t.Logf("Num config blocks submitted: %d", numConfigBlocks)
	require.Eventually(t, func() bool { return env.AssertEqualHeight(numConfigBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// submit a few additional data blocks to test all is well
	testSubmitDataBlocks(t, env, 10, 32)

	t.Log("Closing")
	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}

	for _, node := range env.nodes {
		require.Equal(t, 0, node.pendingTxs.ReleaseWithErrorCallCount())
	}

	countMutex.Lock()
	defer countMutex.Unlock()
	t.Logf("Num config blocks: %d, Num. in-flight waits: %d", numConfigBlocks, inFlightLogMsgCount)
}
