// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var baseNodePort uint32
var basePeerPort uint32
var portMutex sync.Mutex

func init() {
	baseNodePort = 6100
	basePeerPort = 7100
}

func getPorts(num uint32) (node uint32, peer uint32) {
	portMutex.Lock()
	defer portMutex.Unlock()

	node = baseNodePort
	peer = basePeerPort
	baseNodePort += num
	basePeerPort += num

	return
}

// Scenario:
// - start 3 servers in a cluster.
// - wait for one to be the leader.
// - submit a tx to each of the 3, with a client that catches redirects.
// - expect 1 tx to be accepted, and 2 to be redirected.
// - check that the tx accepted is committed and that the written key-value is replicated to all servers.
func TestBasicCluster(t *testing.T) {
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

	time.Sleep(10 * time.Second) //TODO replace by a call to detect a leader

	var leaderValue int
	var txOK int
	var txRedirected int
	for i, srv := range c.Servers {
		txID, rcpt, _, err := srv.WriteDataTx(t, worldstate.DefaultDBName, "john", []byte{uint8(i), uint8(i)})
		if err != nil {
			t.Logf("txID: %s, error: %s", txID, err)
			require.Contains(t, err.Error(), "Redirect blocked in test client: url:")
			require.Contains(t, err.Error(), fmt.Sprintf("referrer: '%s/data/tx'", srv.URL()))
			txRedirected++
		} else {
			t.Logf("tx submitted: %s, %+v", txID, rcpt)
			txOK++
			leaderValue = i
			t.Logf("Leader is: %s", srv.URL())
		}
	}

	require.Equal(t, 1, txOK)
	require.Equal(t, 2, txRedirected)

	time.Sleep(time.Second)

	for _, srv := range c.Servers {
		dataEnv, err := srv.QueryData(t, worldstate.DefaultDBName, "john", "admin")
		require.NoError(t, err)
		dataResp := dataEnv.GetResponse()
		t.Logf("data: %+v", dataResp)
		require.Equal(t, []byte{uint8(leaderValue), uint8(leaderValue)}, dataResp.Value)
		require.Equal(t, uint64(2), dataResp.Metadata.Version.BlockNum)
		require.Equal(t, uint64(0), dataResp.Metadata.Version.TxNum)
	}
}

// Scenario:
// - start 3 servers in a cluster.
// - wait for one to be the leader.
// - stop the leader/follower.
// - submit a tx to the two remaining servers.
// - start the stopped server.
// - wait for one to be the new leader.
// - make sure the stopped server is in sync with the transaction made while it was stopped.
func NodeRecovery(t *testing.T, victimIsLeader bool) {
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

	keys := []string{"alice", "charlie", "dan"}
	for _, key := range keys {
		txID, rcpt, _, err := c.Servers[leaderIndex].WriteDataTx(t, worldstate.DefaultDBName, key, []byte(key+"-data"))
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}

	var dataEnv *types.GetDataResponseEnvelope
	for _, key := range keys {
		require.Eventually(t, func() bool {
			dataEnv, err = c.Servers[leaderIndex].QueryData(t, worldstate.DefaultDBName, key, "admin")
			return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
		}, 30*time.Second, 100*time.Millisecond)

		dataResp := dataEnv.GetResponse().GetValue()
		require.Equal(t, dataResp, []byte(key+"-data"))
		t.Logf("data: %+v", string(dataResp))
	}
	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 4, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	followerServer := (leaderIndex + 1) % 3
	victimServer, activeServer := followerServer, leaderIndex
	if victimIsLeader {
		victimServer, activeServer = leaderIndex, followerServer
	}
	require.NoError(t, c.ShutdownServer(c.Servers[victimServer]))

	//find the new leader
	newLeader := -1
	require.Eventually(t, func() bool {
		newLeader = c.AgreedLeader(t, activeServer, (leaderIndex+2)%3)
		return newLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

	txID, rcpt, _, err := c.Servers[newLeader].WriteDataTx(t, worldstate.DefaultDBName, "bob", []byte("bob-data"))
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	require.NoError(t, c.StartServer(c.Servers[victimServer]))

	require.Eventually(t, func() bool {
		dataEnv, err = c.Servers[victimServer].QueryData(t, worldstate.DefaultDBName, "bob", "admin")
		return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
	}, 30*time.Second, 100*time.Millisecond)

	dataResp := dataEnv.GetResponse().GetValue()
	require.Equal(t, dataResp, []byte("bob-data"))
	t.Logf("data: %+v", string(dataResp))

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 5, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)
}

func TestNodeRecoveryFollower(t *testing.T) {
	NodeRecovery(t, false)
}

func TestNodeRecoveryLeader(t *testing.T) {
	NodeRecovery(t, true)
}

//Scenario:
// round 1:
// - start 3 servers in a cluster.
// - wait for one to be the leader.
// - submit a tx.
// - stop the leader.
// round 2:
// - find leader2.
// - shut down the follower/leader.
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
func StopServerNoMajorityToChooseLeader(t *testing.T, victimIsLeader bool) {
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

	txID, rcpt, _, err := c.Servers[leaderRound1].WriteDataTx(t, worldstate.DefaultDBName, "alice", []byte("alice-data"))
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	var dataEnv *types.GetDataResponseEnvelope
	require.Eventually(t, func() bool {
		dataEnv, err = c.Servers[leaderRound1].QueryData(t, worldstate.DefaultDBName, "alice", "admin")
		return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
	}, 30*time.Second, 100*time.Millisecond)
	dataResp := dataEnv.GetResponse().GetValue()
	require.Equal(t, dataResp, []byte("alice-data"))
	t.Logf("data: %+v", string(dataResp))

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 2, 0, 1, 2)
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
		_, _, _, err = c.Servers[activeServer].WriteDataTx(t, worldstate.DefaultDBName, "bob", []byte("bob-data"))
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

	txID, rcpt, _, err = c.Servers[leaderRound3].WriteDataTx(t, worldstate.DefaultDBName, "charlie", []byte("charlie-data"))
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	require.Eventually(t, func() bool {
		dataEnv, err = c.Servers[leaderRound3].QueryData(t, worldstate.DefaultDBName, "charlie", "admin")
		return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
	}, 30*time.Second, 100*time.Millisecond)

	dataResp = dataEnv.GetResponse().GetValue()
	require.Equal(t, dataResp, []byte("charlie-data"))
	t.Logf("data: %+v", string(dataResp))

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

	txID, rcpt, _, err = c.Servers[leaderRound4].WriteDataTx(t, worldstate.DefaultDBName, "dan", []byte("dan-data"))
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	require.Eventually(t, func() bool {
		dataEnv, err = c.Servers[leaderRound4].QueryData(t, worldstate.DefaultDBName, "dan", "admin")
		return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
	}, 30*time.Second, 100*time.Millisecond)

	dataResp = dataEnv.GetResponse().GetValue()
	require.Equal(t, dataResp, []byte("dan-data"))
	t.Logf("data: %+v", string(dataResp))

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, rcpt.Header.BaseHeader.Number, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)
}

func TestStopFollowerNoMajorityToChooseLeader(t *testing.T) {
	StopServerNoMajorityToChooseLeader(t, false)
}

func TestStopLeaderNoMajorityToChooseLeader(t *testing.T) {
	StopServerNoMajorityToChooseLeader(t, true)
}

// Scenario:
// - start 3 servers in a cluster.
// - wait for one to be the leader.
// - submit 2 txs.
// - shutting down and starting all servers in the cluster.
// - make sure the servers are in sync with the txs made before they were shut down.
func TestClusterRestart(t *testing.T) {
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

	//find the leader
	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1, 2)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	keys := []string{"alice", "bob"}
	for _, key := range keys {
		txID, rcpt, _, err := c.Servers[leaderIndex].WriteDataTx(t, worldstate.DefaultDBName, key, []byte(key+"-data"))
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}

	require.NoError(t, c.Restart())

	//find the new leader
	newLeader := -1
	require.Eventually(t, func() bool {
		newLeader = c.AgreedLeader(t, 0, 1, 2)
		return newLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

	//make sure all 3 nodes are active
	require.Eventually(t, func() bool {
		clusterStatusResEnv, err := c.Servers[newLeader].QueryClusterStatus(t)
		return err == nil && clusterStatusResEnv != nil && len(clusterStatusResEnv.GetResponse().GetActive()) == 3
	}, 30*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 3, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	for _, key := range keys {
		require.Eventually(t, func() bool {
			dataEnv, err := c.Servers[newLeader].QueryData(t, worldstate.DefaultDBName, key, "admin")
			if dataEnv != nil && dataEnv.GetResponse().GetValue() != nil {
				dataVal := dataEnv.GetResponse().GetValue()
				require.Equal(t, dataVal, []byte(key+"-data"))
				t.Logf("data: %+v", dataVal)
			}
			return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
		}, 30*time.Second, 100*time.Millisecond)
	}
}

// Scenario:
// - start 3 servers in a cluster.
// - wait for one to be the leader.
// - stop the leader.
// - wait for new server to be the leader.
// - the leader submit a tx.
// - repeat until each server was a leader at least once.
// - make sure each node submitted a valid tx as a leader.
func TestAllNodesGetLeadership(t *testing.T) {
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

	leaders := [3]bool{false, false, false}

	currentLeader := -1
	require.Eventually(t, func() bool {
		currentLeader = c.AgreedLeader(t, 0, 1, 2)
		return currentLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

	txsCount := 0
	oldLeader := -1
	for !leaders[0] || !leaders[1] || !leaders[2] {
		leaders[currentLeader] = true
		txID, rcpt, _, err := c.Servers[currentLeader].WriteDataTx(t, worldstate.DefaultDBName, strconv.Itoa(currentLeader), []byte{uint8(currentLeader)})
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
		txsCount++

		require.NoError(t, c.ShutdownServer(c.Servers[currentLeader]))

		if oldLeader != -1 {
			require.NoError(t, c.StartServer(c.Servers[oldLeader]))
		}
		oldLeader = currentLeader
		require.Eventually(t, func() bool {
			currentLeader = c.AgreedLeader(t, (oldLeader+1)%3, (oldLeader+2)%3)
			return currentLeader >= 0
		}, 30*time.Second, 100*time.Millisecond)
	}

	require.NoError(t, c.StartServer(c.Servers[oldLeader]))
	require.Eventually(t, func() bool {
		currentLeader = c.AgreedLeader(t, 0, 1, 2)
		return currentLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

	//make sure all 3 nodes are active
	require.Eventually(t, func() bool {
		clusterStatusResEnv, err := c.Servers[currentLeader].QueryClusterStatus(t)
		return err == nil && clusterStatusResEnv != nil && len(clusterStatusResEnv.GetResponse().GetActive()) == 3
	}, 30*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, uint64(txsCount+1), 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	for i := 0; i < 3; i++ {
		require.Eventually(t, func() bool {
			dataEnv, err := c.Servers[currentLeader].QueryData(t, worldstate.DefaultDBName, strconv.Itoa(i), "admin")
			if dataEnv != nil && dataEnv.GetResponse().GetValue() != nil {
				dataVal := dataEnv.GetResponse().GetValue()
				require.Equal(t, []byte{uint8(i)}, dataVal)
				t.Logf("data: %+v", dataVal)
			}
			return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
		}, 30*time.Second, 100*time.Millisecond)

	}
}
