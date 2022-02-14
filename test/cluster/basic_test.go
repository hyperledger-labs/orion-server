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
		txID, rcpt, err := srv.WriteDataTx(t, worldstate.DefaultDBName, "john", []byte{uint8(i), uint8(i)})
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
		dataEnv, err := srv.QueryData(t, worldstate.DefaultDBName, "john")
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
// - stop one follower.
// - submit a tx to the two remaining servers.
// - start the stopped server.
// - wait for one to be the new leader.
// - make sure the stopped server is in sync with the transaction made while it was stopped.
func TestNodeRecoveryFollower(t *testing.T) {
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
		txID, rcpt, err := c.Servers[leaderIndex].WriteDataTx(t, worldstate.DefaultDBName, key, []byte(key+"-data"))
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
			dataEnv, err = c.Servers[leaderIndex].QueryData(t, worldstate.DefaultDBName, key)
			return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
		}, 30*time.Second, 100*time.Millisecond)

		dataResp := dataEnv.GetResponse().GetValue()
		require.Equal(t, dataResp, []byte(key+"-data"))
		t.Logf("data: %+v", string(dataResp))
	}
	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 4, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	followerServer := c.Servers[(leaderIndex+1)%3]
	require.NoError(t, c.ShutdownServer(followerServer))

	//find the new leader
	newLeader := -1
	require.Eventually(t, func() bool {
		newLeader = c.AgreedLeader(t, leaderIndex, (leaderIndex+2)%3)
		return newLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

	txID, rcpt, err := c.Servers[newLeader].WriteDataTx(t, worldstate.DefaultDBName, "bob", []byte("bob-data"))
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	require.NoError(t, c.StartServer(followerServer))

	require.Eventually(t, func() bool {
		dataEnv, err = followerServer.QueryData(t, worldstate.DefaultDBName, "bob")
		return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
	}, 30*time.Second, 100*time.Millisecond)

	dataResp := dataEnv.GetResponse().GetValue()
	require.Equal(t, dataResp, []byte("bob-data"))
	t.Logf("data: %+v", string(dataResp))

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 5, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)
}

// Scenario:
// - start 3 servers in a cluster.
// - wait for one to be the leader.
// - stop the leader.
// - wait for new server to be the leader.
// - submit a tx to the two remaining servers.
// - start the stopped server.
// - make sure the stopped server is in sync with the transaction made while it was stopped.
func TestNodeRecoveryLeader(t *testing.T) {
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
		txID, rcpt, err := c.Servers[leaderIndex].WriteDataTx(t, worldstate.DefaultDBName, key, []byte(key+"-data"))
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
			dataEnv, err = c.Servers[leaderIndex].QueryData(t, worldstate.DefaultDBName, key)
			return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
		}, 30*time.Second, 100*time.Millisecond)
		dataResp := dataEnv.GetResponse().GetValue()
		require.Equal(t, dataResp, []byte(key+"-data"))
		t.Logf("data: %+v", string(dataResp))
	}

	require.NoError(t, c.ShutdownServer(c.Servers[leaderIndex]))

	//find the new leader
	newLeader := -1
	require.Eventually(t, func() bool {
		newLeader = c.AgreedLeader(t, (leaderIndex+1)%3, (leaderIndex+2)%3)
		return newLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

	txID, rcpt, err := c.Servers[newLeader].WriteDataTx(t, worldstate.DefaultDBName, "bob", []byte("bob-data"))
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	require.NoError(t, c.StartServer(c.Servers[leaderIndex]))

	require.Eventually(t, func() bool {
		dataEnv, err = c.Servers[leaderIndex].QueryData(t, worldstate.DefaultDBName, "bob")
		return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
	}, 30*time.Second, 100*time.Millisecond)

	dataResp := dataEnv.GetResponse().GetValue()
	require.Equal(t, dataResp, []byte("bob-data"))
	t.Logf("data: %+v", string(dataResp))

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 5, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)
}

//Scenario:
// - start 3 servers in a cluster.
// - wait for one to be the leader.
// - stop the leader and one follower.
// - submit a tx to the remaining server.
// - there is no majority to pick leader => tx fails.
// - restart one server so now there will be a majority to pick a leader.
// - wait for one to be the leader.
// - submit a tx => tx accepts.
func TestNoMajorityToChooseLeader(t *testing.T) {
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

	txID, rcpt, err := c.Servers[leaderIndex].WriteDataTx(t, worldstate.DefaultDBName, "alice", []byte("alice-data"))
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	var dataEnv *types.GetDataResponseEnvelope
	require.Eventually(t, func() bool {
		dataEnv, err = c.Servers[leaderIndex].QueryData(t, worldstate.DefaultDBName, "alice")
		return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
	}, 30*time.Second, 100*time.Millisecond)
	dataResp := dataEnv.GetResponse().GetValue()
	require.Equal(t, dataResp, []byte("alice-data"))
	t.Logf("data: %+v", string(dataResp))

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 2, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	follower1 := (leaderIndex + 1) % 3
	follower2 := (leaderIndex + 2) % 3

	require.NoError(t, c.ShutdownServer(c.Servers[leaderIndex]))

	//find new leader and make sure there are only 2 active nodes
	newLeader := -1
	require.Eventually(t, func() bool {
		newLeader = c.AgreedLeader(t, follower1, follower2)
		return newLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		clusterStatusResEnv, err := c.Servers[newLeader].QueryClusterStatus(t)
		return err == nil && clusterStatusResEnv != nil && len(clusterStatusResEnv.GetResponse().GetActive()) == 2
	}, 30*time.Second, 100*time.Millisecond)

	require.NoError(t, c.ShutdownServer(c.Servers[follower1]))

	//only one server is active now => no majority to pick leader
	require.Eventually(t, func() bool {
		_, _, err = c.Servers[follower2].WriteDataTx(t, worldstate.DefaultDBName, "bob", []byte("bob-data"))
		return err != nil && err.Error() == "failed to submit transaction, server returned: status: 503 Service Unavailable, message: Cluster leader unavailable"
	}, 60*time.Second, 100*time.Millisecond)

	//restart one server => 2 servers are active
	require.NoError(t, c.StartServer(c.Servers[leaderIndex]))

	//find the new leader
	newLeader = -1
	require.Eventually(t, func() bool {
		newLeader = c.AgreedLeader(t, leaderIndex, follower2)
		return newLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

	txID, rcpt, err = c.Servers[newLeader].WriteDataTx(t, worldstate.DefaultDBName, "charlie", []byte("charlie-data"))
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	require.Eventually(t, func() bool {
		dataEnv, err = c.Servers[newLeader].QueryData(t, worldstate.DefaultDBName, "charlie")
		return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
	}, 30*time.Second, 100*time.Millisecond)

	dataResp = dataEnv.GetResponse().GetValue()
	require.Equal(t, dataResp, []byte("charlie-data"))
	t.Logf("data: %+v", string(dataResp))

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 3, leaderIndex, follower2)
	}, 30*time.Second, 100*time.Millisecond)

	//restart the third node => all 3 nodes are active
	require.NoError(t, c.StartServer(c.Servers[follower1]))

	//find the new leader
	newLeader = -1
	require.Eventually(t, func() bool {
		newLeader = c.AgreedLeader(t, leaderIndex, follower2)
		return newLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

	txID, rcpt, err = c.Servers[newLeader].WriteDataTx(t, worldstate.DefaultDBName, "dan", []byte("dan-data"))
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	require.Eventually(t, func() bool {
		dataEnv, err = c.Servers[newLeader].QueryData(t, worldstate.DefaultDBName, "dan")
		return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
	}, 30*time.Second, 100*time.Millisecond)

	dataResp = dataEnv.GetResponse().GetValue()
	require.Equal(t, dataResp, []byte("dan-data"))
	t.Logf("data: %+v", string(dataResp))

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 4, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)
}

//////=============================================================================================================
//////=============================================================================================================

// Scenario:
// - start 3 servers in a cluster.
// - wait for one to be the leader.
// - submit 2 txs
// - shutting down and starting all servers in the cluster.
// - make sure the servers is in sync with the txs made before they were shut down.
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
		txID, rcpt, err := c.Servers[leaderIndex].WriteDataTx(t, worldstate.DefaultDBName, key, []byte(key+"-data"))
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

	for _, key := range keys {
		require.Eventually(t, func() bool {
			dataEnv, err := c.Servers[newLeader].QueryData(t, worldstate.DefaultDBName, key)
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
// - the leader submit a tx
// - repeat until each server was a leader at least once.
// - make sure each node submitted a valid tx as a leader
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

	oldLeader := -1
	for !leaders[0] || !leaders[1] || !leaders[2] {
		leaders[currentLeader] = true
		txID, rcpt, err := c.Servers[currentLeader].WriteDataTx(t, worldstate.DefaultDBName, strconv.Itoa(currentLeader), []byte{uint8(currentLeader)})
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		t.Logf("tx submitted: %s, %+v", txID, rcpt)

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
		currentLeader = c.AgreedLeader(t, (oldLeader+1)%3, (oldLeader+2)%3)
		return currentLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

	//make sure all 3 nodes are active
	require.Eventually(t, func() bool {
		clusterStatusResEnv, err := c.Servers[currentLeader].QueryClusterStatus(t)
		return err == nil && clusterStatusResEnv != nil && len(clusterStatusResEnv.GetResponse().GetActive()) == 3
	}, 30*time.Second, 100*time.Millisecond)

	for i := 0; i < 3; i++ {
		require.Eventually(t, func() bool {
			dataEnv, err := c.Servers[currentLeader].QueryData(t, worldstate.DefaultDBName, strconv.Itoa(i))
			if dataEnv != nil && dataEnv.GetResponse().GetValue() != nil {
				dataVal := dataEnv.GetResponse().GetValue()
				require.Equal(t, []byte{uint8(i)}, dataVal)
				t.Logf("data: %+v", dataVal)
			}
			return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
		}, 30*time.Second, 100*time.Millisecond)

	}
}
