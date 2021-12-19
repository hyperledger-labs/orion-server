// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication_test

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/pkg/types"
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
	defer os.RemoveAll(env.testDir)
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

	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number:                1,
				LastCommittedBlockNum: 1,
			},
		},
		Payload: &types.Block_DataTxEnvelopes{},
	}

	leaderIdx := env.AgreedLeaderIndex()
	expectedNotLeaderErr := fmt.Sprintf("not a leader, leader is RaftID: %d, with HostPort: 127.0.0.1:2200%d", leaderIdx+1, leaderIdx+1)
	follower1 := (leaderIdx + 1) % 3
	follower2 := (leaderIdx + 2) % 3
	numBlocks := uint64(100)
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)

		// submission to a follower will cause an error
		err = env.nodes[follower1].blockReplicator.Submit(b)
		require.EqualError(t, err, expectedNotLeaderErr)
		err = env.nodes[follower2].blockReplicator.Submit(b)
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
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)

		// submission to a follower will cause an error
		err = env.nodes[follower1].blockReplicator.Submit(b)
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
	defer os.RemoveAll(env.testDir)
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
	defer os.RemoveAll(env.testDir)
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

	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number:                1,
				LastCommittedBlockNum: 1,
			},
		},
		Payload: &types.Block_DataTxEnvelopes{},
	}

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
	defer os.RemoveAll(env.testDir)
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

	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number:                1,
				LastCommittedBlockNum: 1,
			},
		},
		Payload: &types.Block_DataTxEnvelopes{},
	}

	leaderIdx := env.AgreedLeaderIndex()
	numBlocks := uint64(10)
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// a config tx that updates the membership by adding a 4th peer
	updatedClusterConfig := proto.Clone(env.nodes[0].conf.ClusterConfig).(*types.ClusterConfig)
	nodeConfig := &types.NodeConfig{
		Id:          "node4",
		Address:     "127.0.0.1",
		Port:        nodePortBase + 4,
		Certificate: []byte("bogus-cert"),
	}
	peerConfig := &types.PeerConfig{
		NodeId:   "node4",
		RaftId:   uint64(4),
		PeerHost: "127.0.0.1",
		PeerPort: peerPortBase + 4,
	}
	updatedClusterConfig.Nodes = append(updatedClusterConfig.Nodes, nodeConfig)
	updatedClusterConfig.ConsensusConfig.Members = append(updatedClusterConfig.ConsensusConfig.Members, peerConfig)

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

	// wait for some node to become a leader
	isLeaderCond2 := func() bool {
		return env.AgreedLeaderIndex(0, 1, 2) >= 0
	}
	require.Eventually(t, isLeaderCond2, 30*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return isCountOver(3) }, 30*time.Second, 100*time.Millisecond)

	//TODO bootstrap a 4th node, see: https://github.com/hyperledger-labs/orion-server/issues/260
	t.Log("Closing")
	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}
}
