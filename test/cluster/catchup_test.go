// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/internal/replication"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func init() {
	baseNodePort = 6100
	basePeerPort = 7100
}

func createKeys(k int) []int {
	keys := make([]int, k)
	for i, _ := range keys {
		keys[i] = i
	}
	return keys
}

// Scenario:
// - start 3 servers in a cluster and change SnapshotIntervalSize to 4K.
// - submit data txs.
// - shutdown the leader/follower.
// - submit data txs.
// - restart the server.
// - make sure the server is in sync with previous txs.
func NodeRecoveryWithCatchup(t *testing.T, victimIsLeader bool) {
	dir, err := ioutil.TempDir("", "int-test")
	require.NoError(t, err)

	nPort, pPort := getPorts(3)
	setupConfig := &setup.Config{
		NumberOfServers:     3,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		CheckRedirectFunc: func(req *http.Request, via []*http.Request) error {
			return errors.Errorf("Redirect blocked in test client: url: '%s', referrer: '%s', #via: %d", req.URL, req.Referer(), len(via))
		},
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())

	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1, 2)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	snapList := replication.ListSnapshots(c.GetLogger(), filepath.Join(c.Servers[leaderIndex].ConfigDir(), "etcdraft", "snap"))
	require.Equal(t, 0, len(snapList))
	t.Logf("Snap list: %v", snapList)

	//get current cluster config
	configEnv, err := c.Servers[leaderIndex].QueryConfig(t)
	require.NoError(t, err)
	require.NotNil(t, configEnv)

	newConfig := configEnv.GetResponse().GetConfig()
	newConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize = 4 * 1024
	version := configEnv.GetResponse().GetMetadata().GetVersion()
	txsCount := 0

	//change SnapshotIntervalSize to 4K
	txID, rcpt, err := c.Servers[leaderIndex].SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner())
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	txsCount++
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	//restart the cluster so that the SnapshotIntervalSize will update
	require.NoError(t, c.Restart())

	leaderIndex = -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1, 2)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	keys := createKeys(15)
	data := make([]byte, 1024)
	for _, key := range keys {
		txID, rcpt, err := c.Servers[leaderIndex].WriteDataTx(t, worldstate.DefaultDBName, strconv.Itoa(key), data)
		t.Logf("key-%+v", key)
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		txsCount++
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, uint64(txsCount+1), 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	//shutdown the victim server
	follower1 := (leaderIndex + 1) % 3
	follower2 := (leaderIndex + 2) % 3

	victimServer, activeServer := follower1, leaderIndex
	if victimIsLeader {
		victimServer, activeServer = leaderIndex, follower1
	}
	require.NoError(t, c.ShutdownServer(c.Servers[victimServer]))

	//find new leader
	newLeaderIndex := -1
	require.Eventually(t, func() bool {
		newLeaderIndex = c.AgreedLeader(t, activeServer, follower2)
		return newLeaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	//submit txs with data size > SnapshotIntervalSize
	for _, key := range keys {
		txID, rcpt, err := c.Servers[newLeaderIndex].WriteDataTx(t, worldstate.DefaultDBName, strconv.Itoa(key+15), data)
		t.Logf("key-%+v", key+15)
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		txsCount++
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, uint64(txsCount+1), activeServer, follower2)
	}, 30*time.Second, 100*time.Millisecond)

	//restart the victim
	require.NoError(t, c.StartServer(c.Servers[victimServer]))

	//make sure the server is in sync with previous txs
	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, uint64(txsCount+1), 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	keys = createKeys(30)
	var dataEnv *types.GetDataResponseEnvelope
	for _, key := range keys {
		require.Eventually(t, func() bool {
			dataEnv, err = c.Servers[follower1].QueryData(t, worldstate.DefaultDBName, strconv.Itoa(key))
			return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
		}, 30*time.Second, 100*time.Millisecond)
		dataResp := dataEnv.GetResponse().GetValue()
		require.Equal(t, dataResp, data)
	}

	newLeaderIndex = -1
	require.Eventually(t, func() bool {
		newLeaderIndex = c.AgreedLeader(t, 0, 1, 2)
		return newLeaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	snapList = replication.ListSnapshots(c.GetLogger(), filepath.Join(c.Servers[newLeaderIndex].ConfigDir(), "etcdraft", "snap"))
	require.NotEqual(t, 0, len(snapList))
	t.Logf("Snap list: %v", snapList)
}

func TestFollowerRecoveryWithCatchup(t *testing.T) {
	NodeRecoveryWithCatchup(t, false)
}

func TestLeaderRecoveryWithCatchup(t *testing.T) {
	NodeRecoveryWithCatchup(t, true)

}

//Scenario:
// round 1:
// - start 3 servers in a cluster and change SnapshotIntervalSize to 4K.
// - wait for one to be the leader.
// - submit a tx.
// - stop the leader.
// round 2:
// - find leader2.
// - shut down the leader/follower.
// - submit a tx to the remaining server.
// - there is no majority to pick leader => tx fails.
// - restart one server so now there will be a majority to pick a leader.
// round 3:
// - find leader3.
// - submit a tx => tx accepted.
// - restart the third server => all 3 nodes are active.
// round 4:
// - find leader4.
// - submit a tx => tx accepted.
func StopServerNoMajorityToChooseLeaderWithCatchup(t *testing.T, victimIsLeader bool) {
	dir, err := ioutil.TempDir("", "int-test")
	require.NoError(t, err)

	nPort, pPort := getPorts(3)
	setupConfig := &setup.Config{
		NumberOfServers:     3,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		CheckRedirectFunc: func(req *http.Request, via []*http.Request) error {
			return errors.Errorf("Redirect blocked in test client: url: '%s', referrer: '%s', #via: %d", req.URL, req.Referer(), len(via))
		},
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())

	leaderRound1 := -1
	require.Eventually(t, func() bool {
		leaderRound1 = c.AgreedLeader(t, 0, 1, 2)
		return leaderRound1 >= 0
	}, 30*time.Second, 100*time.Millisecond)

	snapList := replication.ListSnapshots(c.GetLogger(), filepath.Join(c.Servers[leaderRound1].ConfigDir(), "etcdraft", "snap"))
	require.Equal(t, 0, len(snapList))
	t.Logf("Snap list: %v", snapList)

	//get current cluster config
	configEnv, err := c.Servers[leaderRound1].QueryConfig(t)
	require.NoError(t, err)
	require.NotNil(t, configEnv)

	newConfig := configEnv.GetResponse().GetConfig()
	newConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize = 4 * 1024
	version := configEnv.GetResponse().GetMetadata().GetVersion()
	txsCount := 0

	//change SnapshotIntervalSize to 4K
	txID, rcpt, err := c.Servers[leaderRound1].SetConfigTx(t, newConfig, version, c.Servers[leaderRound1].AdminSigner())
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	txsCount++
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	//restart the cluster so that the SnapshotIntervalSize will update
	require.NoError(t, c.Restart())

	leaderRound1 = -1
	require.Eventually(t, func() bool {
		leaderRound1 = c.AgreedLeader(t, 0, 1, 2)
		return leaderRound1 >= 0
	}, 30*time.Second, 100*time.Millisecond)

	keys := createKeys(10)
	data := make([]byte, 1024)
	for _, key := range keys {
		txID, rcpt, err = c.Servers[leaderRound1].WriteDataTx(t, worldstate.DefaultDBName, strconv.Itoa(key), data)
		t.Logf("key-%+v", key)
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		txsCount++
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}
	var dataEnv *types.GetDataResponseEnvelope
	for _, key := range keys {
		require.Eventually(t, func() bool {
			dataEnv, err = c.Servers[leaderRound1].QueryData(t, worldstate.DefaultDBName, strconv.Itoa(key))
			return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
		}, 30*time.Second, 100*time.Millisecond)
		dataResp := dataEnv.GetResponse().GetValue()
		require.Equal(t, dataResp, data)
	}
	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, uint64(txsCount+1), 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	follower1 := (leaderRound1 + 1) % 3
	follower2 := (leaderRound1 + 2) % 3

	require.NoError(t, c.ShutdownServer(c.Servers[leaderRound1]))

	//find new leader and make sure there are only 2 active nodes
	leaderRound2 := -1
	require.Eventually(t, func() bool {
		leaderRound2 = c.AgreedLeader(t, follower1, follower2)
		return leaderRound2 >= 0
	}, 30*time.Second, 100*time.Millisecond)

	t.Logf("Stopped node %d, new leader index is: %d; 2-node quorum", leaderRound1, leaderRound2)

	follower1Round2 := follower1
	follower2Round2 := leaderRound1
	if leaderRound2 == follower1 {
		follower1Round2 = follower2
	}

	//servers alive: leaderRound2, follower1Round2
	require.Eventually(t, func() bool {
		clusterStatusResEnv, err := c.Servers[leaderRound2].QueryClusterStatus(t)
		return err == nil && clusterStatusResEnv != nil && len(clusterStatusResEnv.GetResponse().GetActive()) == 2
	}, 30*time.Second, 100*time.Millisecond)

	victimServer, activeServer := follower1Round2, leaderRound2
	if victimIsLeader {
		victimServer, activeServer = leaderRound2, follower1Round2
	}
	require.NoError(t, c.ShutdownServer(c.Servers[victimServer]))

	//only one server is active now => no majority to pick leader
	require.Eventually(t, func() bool {
		_, _, err = c.Servers[activeServer].WriteDataTx(t, worldstate.DefaultDBName, "bob", data)
		return err != nil && err.Error() == "failed to submit transaction, server returned: status: 503 Service Unavailable, message: Cluster leader unavailable"
	}, 60*time.Second, 100*time.Millisecond)

	//restart one server => 2 servers are active: follower1Round2, leaderRound2
	require.NoError(t, c.StartServer(c.Servers[victimServer]))

	//find the new leader
	leaderRound3 := -1
	require.Eventually(t, func() bool {
		leaderRound3 = c.AgreedLeader(t, leaderRound2, follower1Round2)
		return leaderRound3 >= 0
	}, 30*time.Second, 100*time.Millisecond)

	t.Logf("Started node %d, leader index is: %d; 2-node quorum", victimServer, leaderRound3)

	follower1Round3 := leaderRound2
	if leaderRound3 == leaderRound2 {
		follower1Round3 = follower1Round2
	}

	for _, key := range keys {
		txID, rcpt, err = c.Servers[leaderRound3].WriteDataTx(t, worldstate.DefaultDBName, strconv.Itoa(key+10), data)
		t.Logf("key-%+v", key+10)
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}

	for _, key := range keys {
		require.Eventually(t, func() bool {
			dataEnv, err = c.Servers[leaderRound3].QueryData(t, worldstate.DefaultDBName, strconv.Itoa(key+10))
			return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
		}, 30*time.Second, 100*time.Millisecond)
		dataResp := dataEnv.GetResponse().GetValue()
		require.Equal(t, dataResp, data)
	}

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, rcpt.Header.BaseHeader.Number, leaderRound3, follower1Round3)
	}, 30*time.Second, 100*time.Millisecond)

	//restart the third node => all 3 nodes are active
	require.NoError(t, c.StartServer(c.Servers[follower2Round2]))

	//find the new leader
	leaderRound4 := -1
	require.Eventually(t, func() bool {
		leaderRound4 = c.AgreedLeader(t, 0, 1, 2)
		return leaderRound4 >= 0
	}, 30*time.Second, 100*time.Millisecond)

	t.Logf("Started node %d, leader index is: %d; all 3 nodes are up", follower2Round2, leaderRound4)

	for _, key := range keys {
		txID, rcpt, err = c.Servers[leaderRound4].WriteDataTx(t, worldstate.DefaultDBName, strconv.Itoa(key+20), data)
		t.Logf("key-%+v", key+20)
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}
	for _, key := range keys {
		require.Eventually(t, func() bool {
			dataEnv, err = c.Servers[leaderRound4].QueryData(t, worldstate.DefaultDBName, strconv.Itoa(key+20))
			return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
		}, 30*time.Second, 100*time.Millisecond)
		dataResp := dataEnv.GetResponse().GetValue()
		require.Equal(t, dataResp, data)
	}

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, rcpt.Header.BaseHeader.Number, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	snapList = replication.ListSnapshots(c.GetLogger(), filepath.Join(c.Servers[leaderRound4].ConfigDir(), "etcdraft", "snap"))
	require.NotEqual(t, 0, len(snapList))
	t.Logf("Snap list: %v", snapList)
}

func TestStopFollowerNoMajorityToChooseLeaderWithCatchup(t *testing.T) {
	StopServerNoMajorityToChooseLeaderWithCatchup(t, false)
}

func TestStopLeaderNoMajorityToChooseLeaderWithCatchup(t *testing.T) {
	StopServerNoMajorityToChooseLeaderWithCatchup(t, true)

}
