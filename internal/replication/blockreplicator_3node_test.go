// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication_test

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	interrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Scenario:
// - Start 3 nodes together, wait for leader,
// - Stop the leader, wait for new leader,
// - Stop the leader, remaining node has no leader.
func TestBlockReplicator_3Node_StartCloseStep(t *testing.T) {
	var countMutex sync.Mutex
	var campaignLogMsgCount int
	campaignLogMsgHook := func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Starting to campaign every") ||
			strings.Contains(entry.Message, "This node was selected to run a leader election campaign on the new cluster") {
			countMutex.Lock()
			defer countMutex.Unlock()

			campaignLogMsgCount++
		}
		return nil
	}

	env := createClusterEnv(t, 3, nil, "info", zap.Hooks(campaignLogMsgHook))
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	// start 3 at once
	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for an agreed leader
	assert.Eventually(t, func() bool { return env.ExistsAgreedLeader() }, 30*time.Second, 100*time.Millisecond)
	assert.Eventually(t, func() bool { return env.SymmetricConnectivity() }, 30*time.Second, 100*time.Millisecond)
	assert.Eventually(t, func() bool {
		countMutex.Lock()
		defer countMutex.Unlock()
		return campaignLogMsgCount == 2
	}, 30*time.Second, 100*time.Millisecond)
	leaderIndex1 := env.FindLeaderIndex()
	followerIndex1 := (leaderIndex1 + 1) % 3
	followerIndex2 := (leaderIndex1 + 1) % 3

	//check the followers redirect to the leader
	expectedLeaderErr := &interrors.NotLeaderError{
		LeaderID:       uint64(leaderIndex1 + 1),
		LeaderHostPort: fmt.Sprintf("127.0.0.1:%d", int(nodePortBase)+leaderIndex1+1),
	}
	for _, f := range []int{followerIndex1, followerIndex2} {
		err := env.nodes[f].blockReplicator.IsLeader()
		require.EqualError(t, err, expectedLeaderErr.Error())
	}

	//close the leader, wait for some node to become a new leader
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
	env := createClusterEnv(t, 3, nil, "info")
	defer destroyClusterEnv(t, env)
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
	assert.Eventually(t, func() bool { return env.SymmetricConnectivity(0, 1) }, 30*time.Second, 100*time.Millisecond)

	err = env.nodes[2].Start()
	require.NoError(t, err)
	assert.Eventually(t, func() bool { return env.ExistsAgreedLeader() }, 30*time.Second, 100*time.Millisecond)
	assert.Eventually(t, func() bool { return env.SymmetricConnectivity() }, 30*time.Second, 100*time.Millisecond)
}

// Scenario:
// - Start 3 nodes together, wait for leader,
// - Stop the leader, wait for new leader,
// - Restart the node, wait for consistent leader on all
// - Stop a follower, wait for consistent leader on two,
// - Restart the node, wait for consistent leader on all
func TestBlockReplicator_3Node_Restart(t *testing.T) {
	env := createClusterEnv(t, 3, nil, "info")
	defer destroyClusterEnv(t, env)
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
}

// Scenario:
// - Start 3 nodes together, wait for leader,
// - Submit 100 blocks, wait for all ledgers to get them.
func TestBlockReplicator_3Node_Submit(t *testing.T) {
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

	assert.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	t.Log("Closing")
	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}

	for _, node := range env.nodes {
		require.Equal(t, 0, node.pendingTxs.ReleaseWithErrorCallCount())
	}
}

// Scenario:
// - Start 3 nodes together, wait for leader, submit 100 blocks, wait for all ledgers to get them.
// - Stop a follower node,  wait for leader, submit 100 blocks, wait for 2 ledgers to get them.
// - Restart the node, wait for leader, wait for node to get missing blocks.
// - Stop a leader node,  wait for new leader, submit 100 blocks, wait for 2 ledgers to get them.
// - Restart the node, wait for leader, wait for node to get missing blocks.
func TestBlockReplicator_3Node_SubmitRecover(t *testing.T) {
	env := createClusterEnv(t, 3, nil, "info")
	defer destroyClusterEnv(t, env)
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
	assert.Eventually(t, func() bool { return env.SymmetricConnectivity(followerIdx1, followerIdx2) }, 30*time.Second, 100*time.Millisecond)
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
	raftConfig.SnapshotIntervalSize = uint64(4*len(utils.MarshalOrPanic(block)) + 1) // snapshot every ~5 blocks

	env := createClusterEnv(t, 3, raftConfig, "info")
	defer destroyClusterEnv(t, env)
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
	assert.Eventually(t, func() bool { return env.SymmetricConnectivity(followerIdx1, followerIdx2) }, 30*time.Second, 100*time.Millisecond)
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

// Scenario:
// - Start 2 nodes out of 3 together, wait for leader.
// - Continuously submit blocks to fill the leader's pipeline.
// - After 100 blocks close the follower, so that the current leader loses its leadership.
// - Continue to submit blocks, anticipating that from some point they will be rejected.
// - Wait for a ReleaseWithError to be called from within the block replicator as it drains the internal proposal channel.
func TestBlockReplicator_3Node_LeadershipLoss(t *testing.T) {
	env := createClusterEnv(t, 3, nil, "info")
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	//start 2 of 3
	for i, node := range env.nodes {
		if i == 2 {
			continue
		}
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.AgreedLeaderIndex(0, 1) >= 0
	}
	assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number:                1,
				LastCommittedBlockNum: 1,
			},
		},
		Payload: &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{
				Envelopes: []*types.DataTxEnvelope{
					{
						Payload: &types.DataTx{
							TxId: "txid:1",
						},
					},
					{
						Payload: &types.DataTx{
							TxId: "txid:2",
						},
					},
				},
			},
		},
	}

	leaderIdx := env.AgreedLeaderIndex(0, 1)
	numBlocks := uint64(100)
	stopCh := make(chan interface{})
	wg100 := sync.WaitGroup{}
	wg100.Add(1)
	wgStop := sync.WaitGroup{}
	wgStop.Add(1)

	var iLostLeadership uint64

	go func() {
		defer wgStop.Done()

	LOOP:
		for i := uint64(0); ; i++ {
			select {
			case <-stopCh:
				t.Logf("Submiter stopped: %d", i)
				break LOOP
			default:
				b := proto.Clone(block).(*types.Block)
				b.Header.BaseHeader.Number = 2 + i
				err := env.nodes[leaderIdx].blockReplicator.Submit(b)
				if i <= numBlocks {
					require.NoError(t, err)
				} else {
					if err != nil && iLostLeadership == 0 {
						require.Contains(t, err.Error(), "not a leader")
						iLostLeadership = i
						t.Logf("Lost leadership: %d", i)
					}
				}

				if i == numBlocks {
					t.Logf("Submitted: %d", i)
					wg100.Done() // stop a node and eventually loose leadership
				}
			}
		}
	}()

	wg100.Wait()

	// close the follower
	err := env.nodes[(leaderIdx+1)%2].Close()
	require.NoError(t, err)

	// eventually there will be a Release, as the internal proposal channel drains
	assert.Eventually(t, func() bool { return env.nodes[leaderIdx].pendingTxs.ReleaseWithErrorCallCount() > 0 }, 60*time.Second, 100*time.Millisecond)
	close(stopCh)
	t.Log("before stopped: ")
	wgStop.Wait()
	t.Log("stopped: ")
	err = env.nodes[leaderIdx].Close()
	require.NoError(t, err)
}

func TestBlockReplicator_3Node_LeadershipLossRecovery(t *testing.T) {
	env := createClusterEnv(t, 3, nil, "info")
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	//start 2 of 3
	for i, node := range env.nodes {
		if i == 2 {
			continue
		}
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.AgreedLeaderIndex(0, 1) >= 0
	}
	assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number:                1,
				LastCommittedBlockNum: 1,
			},
		},
		Payload: &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{
				Envelopes: []*types.DataTxEnvelope{
					{
						Payload: &types.DataTx{
							TxId: "txid:1",
						},
					},
					{
						Payload: &types.DataTx{
							TxId: "txid:2",
						},
					},
				},
			},
		},
	}

	leaderIdx := env.AgreedLeaderIndex(0, 1)
	followerIdx := (leaderIdx + 1) % 2

	numBlocks := uint64(100)
	var numSubmitted uint64
	stopCh := make(chan interface{})
	wg100 := sync.WaitGroup{}
	wg100.Add(1)
	wgStop := sync.WaitGroup{}
	wgStop.Add(1)

	var lostLeadership bool

	go func() {
		defer wgStop.Done()

	LOOP:
		for i := uint64(0); ; i++ {
			select {
			case <-stopCh:
				t.Logf("Submiter stopped: %d", i)
				break LOOP
			default:
				b := proto.Clone(block).(*types.Block)
				b.Header.BaseHeader.Number = 2 + i
				err := env.nodes[leaderIdx].blockReplicator.Submit(b)
				if i <= numBlocks {
					require.NoError(t, err)
				} else {
					if err != nil && !lostLeadership {
						require.Contains(t, err.Error(), "not a leader")
						lostLeadership = true
						t.Logf("Lost leadership at block: %d", i)
					}
				}

				if i == numBlocks {
					t.Logf("Submitted: %d", i)
					wg100.Done() // stop a node and eventually loose leadership
				}
				atomic.StoreUint64(&numSubmitted, i)
			}
		}
	}()

	wg100.Wait()

	// close the follower
	err := env.nodes[followerIdx].Close()
	require.NoError(t, err)

	// eventually there will be a Release, as the internal proposal channel drains
	assert.Eventually(t, func() bool { return env.nodes[leaderIdx].pendingTxs.ReleaseWithErrorCallCount() > 0 }, 60*time.Second, 100*time.Millisecond)

	// restart the follower
	err = env.nodes[followerIdx].Restart()
	require.NoError(t, err)

	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)
	leaderIdx2 := env.AgreedLeaderIndex(0, 1)
	require.Equal(t, leaderIdx, leaderIdx2) // The old leader will be elected as the new one

	numSubmittedAfterRecovery := atomic.LoadUint64(&numSubmitted)
	require.Eventually(t, func() bool {
		return atomic.LoadUint64(&numSubmitted) > numSubmittedAfterRecovery+1000
	}, 30*time.Second, 100*time.Millisecond)

	close(stopCh)
	t.Log("before stopped: ")
	wgStop.Wait()
	t.Log("stopped: ")

	require.Eventually(t, func() bool { return env.AssertEqualLedger(0, 1) == nil }, 30*time.Second, 100*time.Millisecond)
}

// Scenario:
// - Start 3 together, wait for leader.
// - Submit a few blocks to the leader (node X).
// - Repeat:
//   1. close the leader
//   2. wait for new leader
//   3. submit a few blocks
//   4. restart the stopped node.
//   5. Until node X, the first leader, is elected again.
// - Check for consistent ledgers.
// This tests for consistent block numbering at the leader after re-election.
func TestBlockReplicator_3Node_LeaderReElected(t *testing.T) {
	env := createClusterEnv(t, 3, nil, "info")
	defer destroyClusterEnv(t, env)
	require.Equal(t, 3, len(env.nodes))

	//start 3
	for _, node := range env.nodes {
		err := node.Start()
		require.NoError(t, err)
	}

	// wait for some node to become a leader
	isLeaderCond := func() bool {
		return env.AgreedLeaderIndex() >= 0
	}
	assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)
	firstLeaderIdx := env.AgreedLeaderIndex()
	t.Logf("First leader index is: %d", firstLeaderIdx)
	currentLeaderIdx := firstLeaderIdx

	// submit a few blocks
	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number:                1,
				LastCommittedBlockNum: 1,
			},
		},
		Payload: &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{
				Envelopes: []*types.DataTxEnvelope{
					{
						Payload: &types.DataTx{
							TxId: "txid:1",
						},
					},
					{
						Payload: &types.DataTx{
							TxId: "txid:2",
						},
					},
				},
			},
		},
	}
	numBlocks := uint64(1)
	for n := 0; n < 5; n++ {
		b := proto.Clone(block).(*types.Block)
		err := env.nodes[firstLeaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
		numBlocks++
	}
	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks) }, 30*time.Second, 100*time.Millisecond)

	// kill leader, wait for new one, submit some blocks, restart prev. leader; repeat until first leader re-elected
	timeout := time.After(5 * time.Minute)
	for {
		err := env.nodes[currentLeaderIdx].Close()
		require.NoError(t, err)

		follower1idx := (currentLeaderIdx + 1) % 3
		follower2idx := (currentLeaderIdx + 1) % 3
		isLeaderOf2Cond := func() bool {
			return env.AgreedLeaderIndex(follower1idx, follower2idx) >= 0
		}
		assert.Eventually(t, isLeaderOf2Cond, 30*time.Second, 100*time.Millisecond)
		prevLeaderIdx := currentLeaderIdx
		currentLeaderIdx = env.AgreedLeaderIndex(follower1idx, follower2idx)
		t.Logf("Current leader index is: %d", currentLeaderIdx)

		for n := 0; n < 5; n++ {
			b := proto.Clone(block).(*types.Block)
			err := env.nodes[currentLeaderIdx].blockReplicator.Submit(b)
			require.NoError(t, err)
			numBlocks++
		}
		require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks, follower1idx, follower2idx) }, 30*time.Second, 100*time.Millisecond)

		env.nodes[prevLeaderIdx].Restart()
		assert.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

		// until the first leader is re-elected
		if currentLeaderIdx == firstLeaderIdx {
			t.Logf("First leader (index: %d) was re-elected", currentLeaderIdx)
			break
		}

		select {
		case <-timeout:
			t.Fatalf("test failed, leader never re-elected after %s", 5*time.Minute)
		default:
			continue
		}
	}

	require.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks) }, 30*time.Second, 100*time.Millisecond)

	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}

	require.NoError(t, env.AssertEqualLedger())
}

// Scenario:
// - Start 3 nodes together, wait for leader.
// - Submit blocks fast, until the leader exceeds the maximal number in-flight-blocks a few times.
func TestBlockReplicator_3Node_InFlightBlocks(t *testing.T) {
	var countMutex sync.Mutex
	var inFlightLogMsgCount int

	inFlightLogMsgHook := func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Number of in-flight blocks exceeds max") {
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

	env := createClusterEnv(t, 3, nil, "debug", zap.Hooks(inFlightLogMsgHook))
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
	var numBlocks uint64
	for {
		b := proto.Clone(block).(*types.Block)
		b.Header.BaseHeader.Number = 2 + numBlocks
		err := env.nodes[leaderIdx].blockReplicator.Submit(b)
		require.NoError(t, err)
		numBlocks++
		if isCountOver(4) {
			break
		}
	}

	t.Logf("Num blocks: %d", numBlocks)
	assert.Eventually(t, func() bool { return env.AssertEqualHeight(numBlocks + 1) }, 30*time.Second, 100*time.Millisecond)

	t.Log("Closing")
	for _, node := range env.nodes {
		err := node.Close()
		require.NoError(t, err)
	}

	for _, node := range env.nodes {
		require.Equal(t, 0, node.pendingTxs.ReleaseWithErrorCallCount())
	}

	require.True(t, isCountOver(4))
}
