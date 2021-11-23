// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Scenario: reconfigure a node endpoint
// - start 3 nodes, submit some blocks, verify replication
// - restart node 3 on another endpoint, not yet reflected in the shared-config, node is partitioned from raft
// - submit some blocks, verify replication on 2 nodes
// - submit a config-tx that reflects the 3rd node new PeerConfig
// - verify 3rd node is back to the cluster and catches up.
func TestBlockReplicator_ReConfig_Endpoint(t *testing.T) {
	env := createClusterEnv(t, 3, nil, "info")
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
	assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

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

	assert.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// restart node 3 on a new port, it will be partitioned from Raft
	env.nodes[2].Close()
	env.nodes[2].conf.LocalConf.Replication.Network.Port++
	env.nodes[2].Restart()

	// wait for some node [1,2] to become a leader
	isLeaderCond2 := func() bool {
		return env.AgreedLeaderIndex(0, 1) >= 0
	}
	assert.Eventually(t, isLeaderCond2, 30*time.Second, 100*time.Millisecond)
	leaderIdx = env.AgreedLeaderIndex(0, 1)
	expectedNotLeaderErr = fmt.Sprintf("not a leader, leader is RaftID: %d, with HostPort: 127.0.0.1:2200%d", leaderIdx+1, leaderIdx+1)
	follower1 = (leaderIdx + 1) % 2
	follower2 = 2
	for i := numBlocks; i < 2*numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)

		// submission to a follower will cause an error
		err = env.nodes[follower1].blockReplicator.Submit(b)
		assert.EqualError(t, err, expectedNotLeaderErr)
		// submission to a partitioned node will cause an error
		err = env.nodes[follower2].blockReplicator.Submit(b)
		assert.EqualError(t, err, "not a leader, leader is RaftID: 0, with HostPort: ")
	}

	// nodes [1,2] are in sync, node 3 is partitioned
	assert.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks+1, 0, 1) }, 30*time.Second, 100*time.Millisecond)
	h3, err := env.nodes[2].ledger.Height()
	require.NoError(t, err)
	assert.Equal(t, numBlocks+1, h3)

	// a config tx that updates the two running members
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
	err = env.nodes[leaderIdx].blockReplicator.Submit(proposeBlock)
	require.NoError(t, err)

	// after re-config node3 is no longer partitioned from the cluster, catches up, and knows who the leader is
	assert.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks + 2) }, 30*time.Second, 100*time.Millisecond)
	err = env.nodes[follower2].blockReplicator.Submit(proto.Clone(block).(*types.Block))
	assert.EqualError(t, err, expectedNotLeaderErr)

	t.Log("Closing")
	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}
}
