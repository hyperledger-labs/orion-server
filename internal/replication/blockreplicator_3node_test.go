package replication_test

import (
	"os"
	"testing"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/internal/httputils"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Scenario:
// - Start 3 nodes together, wait for leader,
// - Stop the leader, wait for new leader,
// - Stop the leader, remaining node has no leader.
func TestBlockReplicator_3Node_StartCloseStep(t *testing.T) {
	env := createClusterEnv(t, "info", 3, nil)
	defer os.RemoveAll(env.testDir)
	require.Equal(t, 3, len(env.nodes))

	// start 3 at once
	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for an agreed leader
	assert.Eventually(t, func() bool { return env.ExistsAgreedLeader() }, 30*time.Second, 100*time.Millisecond)

	//close the leader, wait for some node to become a new leader
	leaderIndex1 := env.FindLeaderIndex()
	require.True(t, leaderIndex1 >= 0)
	t.Logf("Leader #1 index: %d", leaderIndex1)
	err := env.nodes[leaderIndex1].Close()
	require.NoError(t, err)
	isLeaderCond := func() bool {
		idx := env.FindLeaderIndex()
		return idx >= 0 && idx != leaderIndex1
	}
	assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	//close the 2nd leader, remaining node has no leader
	leaderIndex2 := env.FindLeaderIndex()
	require.True(t, leaderIndex2 >= 0)
	t.Logf("Leader #2 index: %d", leaderIndex2)
	err = env.nodes[leaderIndex2].Close()
	require.NoError(t, err)

	//remaining node has no leader
	var lastIndex int
	for i := range env.nodes {
		if i == leaderIndex1 || i == leaderIndex2 {
			continue
		}
		lastIndex = i
	}
	noLeaderCond := func() bool {
		err := env.nodes[lastIndex].blockReplicator.IsLeader()
		if err != nil && err.LeaderID == 0 {
			return true
		}
		return false
	}
	assert.Eventually(t, noLeaderCond, 30*time.Second, 100*time.Millisecond)

	err = env.nodes[lastIndex].Close()
	require.NoError(t, err)
}

// Scenario:
// - Start 1 node, no leader
// - Start 2nd node, wait for leader,
// - Start 3rd, wait for consistent leader on all
func TestBlockReplicator_3Node_StartStepClose(t *testing.T) {
	env := createClusterEnv(t, "info", 3, nil)
	defer os.RemoveAll(env.testDir)
	require.Equal(t, 3, len(env.nodes))

	err := env.nodes[0].Start()
	require.NoError(t, err)
	noLeaderCond := func() bool {
		err := env.nodes[0].blockReplicator.IsLeader()
		if err != nil && err.LeaderID == 0 {
			return true
		}
		return false
	}
	assert.Eventually(t, noLeaderCond, 30*time.Second, 100*time.Millisecond)

	err = env.nodes[1].Start()
	require.NoError(t, err)
	assert.Eventually(t, func() bool { return env.ExistsAgreedLeader(0, 1) }, 30*time.Second, 100*time.Millisecond)

	err = env.nodes[2].Start()
	require.NoError(t, err)
	assert.Eventually(t, func() bool { return env.ExistsAgreedLeader() }, 30*time.Second, 100*time.Millisecond)

	//close all
	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}
}

// Scenario:
// - Start 3 nodes together, wait for leader,
// - Stop the leader, wait for new leader,
// - Restart the node, wait for consistent leader on all
// - Stop a follower, wait for consistent leader on two,
// - Restart the node, wait for consistent leader on all
func TestBlockReplicator_3Node_Restart(t *testing.T) {
	env := createClusterEnv(t, "info", 3, nil)
	defer os.RemoveAll(env.testDir)
	require.Equal(t, 3, len(env.nodes))

	// start 3 at once
	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for an agreed leader
	sameLeaderCond := func() bool {
		return env.ExistsAgreedLeader()
	}
	assert.Eventually(t, sameLeaderCond, 30*time.Second, 100*time.Millisecond)

	//close the leader, wait for some node to become a new leader
	leaderIndex1 := env.FindLeaderIndex()
	require.True(t, leaderIndex1 >= 0)
	t.Logf("Stopping leader, index: %d", leaderIndex1)
	err := env.nodes[leaderIndex1].Close()
	require.NoError(t, err)
	isLeaderCond1 := func() bool {
		idx := env.FindLeaderIndex()
		return idx >= 0 && idx != leaderIndex1
	}
	assert.Eventually(t, isLeaderCond1, 30*time.Second, 100*time.Millisecond)

	//restart the old leader
	t.Logf("Restarting node, index: %d", leaderIndex1)
	err = env.nodes[leaderIndex1].Restart()
	require.NoError(t, err)
	assert.Eventually(t, sameLeaderCond, 30*time.Second, 100*time.Millisecond)

	leaderIndex2 := env.FindLeaderIndex()
	require.True(t, leaderIndex2 >= 0)
	followerIndex := (leaderIndex2 + 1) % 3
	if followerIndex == leaderIndex1 {
		followerIndex = (leaderIndex2 + 2) % 3
	}
	t.Logf("Stopping follower, index: %d", followerIndex)
	err = env.nodes[followerIndex].Close()
	require.NoError(t, err)
	isLeaderCond2 := func() bool {
		idx := env.FindLeaderIndex()
		return idx >= 0 && idx == leaderIndex2 //stopping a follower does not change the leader
	}
	assert.Eventually(t, isLeaderCond2, 30*time.Second, 100*time.Millisecond)

	//restart the follower
	t.Logf("Restarting node, index: %d", followerIndex)
	err = env.nodes[followerIndex].Restart()
	require.NoError(t, err)
	assert.Eventually(t, sameLeaderCond, 30*time.Second, 100*time.Millisecond)

	//close all
	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}
}

// Scenario:
// - Start 3 nodes together, wait for leader,
// - Submit 100 blocks, wait for all ledgers to get them.
func TestBlockReplicator_3Node_Submit(t *testing.T) {
	env := createClusterEnv(t, "info", 3, nil)
	defer os.RemoveAll(env.testDir)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.FindLeaderIndex() >= 0
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

	leaderIdx := env.FindLeaderIndex()
	numBlocks := uint64(100)
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		b.Header.BaseHeader.Number = 2 + i
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)

	}

	assert.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}
}

// Scenario:
// - Start 3 nodes together, wait for leader, submit 100 blocks, wait for all ledgers to get them.
// - Stop a follower node,  wait for leader, submit 100 blocks, wait for 2 ledgers to get them.
// - Restart the node, wait for leader, wait for node to get missing blocks.
// - Stop a leader node,  wait for new leader, submit 100 blocks, wait for 2 ledgers to get them.
// - Restart the node, wait for leader, wait for node to get missing blocks.
func TestBlockReplicator_3Node_SubmitRecover(t *testing.T) {
	env := createClusterEnv(t, "info", 3, nil)
	defer os.RemoveAll(env.testDir)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.FindLeaderIndex() >= 0
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

	leaderIdx := env.FindLeaderIndex()
	numBlocks := uint64(100)
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		b.Header.BaseHeader.Number = 2 + i
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)

	}

	// all get first 100
	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// follower recovery
	followerIdx1 := (leaderIdx + 1) % 3
	followerIdx2 := (leaderIdx + 2) % 3

	err := env.nodes[followerIdx1].Close()
	require.NoError(t, err)
	t.Logf("Stopped follower node, index: %d", followerIdx1)
	for i := numBlocks; i < 2*numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		b.Header.BaseHeader.Number = 2 + i
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks+1, leaderIdx, followerIdx2) }, 30*time.Second, 100*time.Millisecond)

	err = env.nodes[followerIdx1].Restart()
	require.NoError(t, err)
	t.Logf("Restarted follower node, index: %d", followerIdx1)
	assert.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// leader recovery
	err = env.nodes[leaderIdx].Close()
	require.NoError(t, err)
	t.Logf("Stopped leader node, index: %d", leaderIdx)
	assert.Eventually(t, func() bool { return env.ExistsAgreedLeader(followerIdx1, followerIdx2) }, 30*time.Second, 100*time.Millisecond)
	newLeaderIdx := env.FindLeaderIndex()
	for i := 2 * numBlocks; i < 3*numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		b.Header.BaseHeader.Number = 2 + i
		err := env.nodes[newLeaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return env.AssertEqualHeight(3*numBlocks+1, followerIdx1, followerIdx2) }, 30*time.Second, 100*time.Millisecond)

	err = env.nodes[leaderIdx].Restart()
	require.NoError(t, err)
	t.Logf("Restarted old leader node, index: %d", leaderIdx)
	assert.Eventually(t, func() bool { return env.AssertEqualHeight(3*numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}
}

// Scenario:
// - Configure cluster to take snapshots every approx. 5 blocks.
// - Start 3 nodes together, wait for leader, submit 10 blocks, wait for all ledgers to get them.
// - Stop a follower node,  wait for leader, submit 10 blocks, wait for 2 ledgers to get them.
// - Restart the node, wait for leader, wait for node to get missing blocks.
//   Recovering node is expected to get a snapshot from the leader and trigger catch-up from it.
// - Stop a leader node,  wait for new leader, submit 10 blocks, wait for 2 ledgers to get them.
// - Restart the node, wait for leader, wait for node to get missing blocks.
//   Recovering node is expected to get a snapshot from the leader and trigger catch-up from it.
func TestBlockReplicator_3Node_Catchup(t *testing.T) {
	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number:                1,
				LastCommittedBlockNum: 1,
			},
		},
		Payload: &types.Block_DataTxEnvelopes{},
	}
	raftConfig := proto.Clone(raftConfigNoSnapshots).(*types.RaftConfig)
	raftConfig.SnapshotIntervalSize = uint64(4*len(httputils.MarshalOrPanic(block)) + 1) // snapshot every ~5 blocks

	env := createClusterEnv(t, "info", 3, raftConfig)
	defer os.RemoveAll(env.testDir)
	require.Equal(t, 3, len(env.nodes))

	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for a common leader
	isLeaderCond := func() bool {
		return env.ExistsAgreedLeader()
	}
	assert.Eventually(t, isLeaderCond, 10*time.Second, 100*time.Millisecond)

	leaderIdx := env.FindLeaderIndex()
	numBlocks := uint64(10)
	for i := uint64(0); i < numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		b.Header.BaseHeader.Number = 2 + i
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}

	// all get first 10
	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// follower recovery
	followerIdx1 := (leaderIdx + 1) % 3
	followerIdx2 := (leaderIdx + 2) % 3

	err := env.nodes[followerIdx1].Close()
	require.NoError(t, err)
	t.Logf("Stopped follower node, index: %d", followerIdx1)
	for i := numBlocks; i < 2*numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		b.Header.BaseHeader.Number = 2 + i
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks+1, leaderIdx, followerIdx2) }, 30*time.Second, 100*time.Millisecond)

	err = env.nodes[followerIdx1].Restart()
	require.NoError(t, err)
	t.Logf("Restarted follower node, index: %d", followerIdx1)
	assert.Eventually(t, func() bool { return env.AssertEqualHeight(2*numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	// leader recovery
	err = env.nodes[leaderIdx].Close()
	require.NoError(t, err)
	t.Logf("Stopped leader node, index: %d", leaderIdx)
	assert.Eventually(t, func() bool { return env.ExistsAgreedLeader(followerIdx1, followerIdx2) }, 30*time.Second, 100*time.Millisecond)
	newLeaderIdx := env.FindLeaderIndex()
	for i := 2 * numBlocks; i < 3*numBlocks; i++ {
		b := proto.Clone(block).(*types.Block)
		b.Header.BaseHeader.Number = 2 + i
		err := env.nodes[newLeaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return env.AssertEqualHeight(3*numBlocks+1, followerIdx1, followerIdx2) }, 30*time.Second, 100*time.Millisecond)

	err = env.nodes[leaderIdx].Restart()
	require.NoError(t, err)
	t.Logf("Restarted old leader node, index: %d", leaderIdx)
	assert.Eventually(t, func() bool { return env.AssertEqualHeight(3*numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}

	require.NoError(t, env.AssertEqualLedger())
}
