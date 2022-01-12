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

func agreedLeader(t *testing.T, c *setup.Cluster, activeServers ...int) int {
	var leaders [3]int
	var leader int

	for _, srvVal := range activeServers {
		require.Eventually(t, func() bool {
			clusterStatusResEnv, err := c.Servers[srvVal].QueryClusterStatus(t)
			if clusterStatusResEnv != nil {
				_, leader = c.GetServerByID(clusterStatusResEnv.GetResponse().GetLeader())
				leaders[srvVal] = leader
			}
			return err == nil && clusterStatusResEnv != nil && clusterStatusResEnv.GetResponse().GetLeader() != ""
		}, 30*time.Second, 100*time.Millisecond)
	}

	for _, srvVal := range activeServers {
		if leaders[srvVal] != leader {
			return -1
		}
	}
	return leader
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

	leaderVal := agreedLeader(t, c, 0, 1, 2)
	require.NotEqual(t, leaderVal, -1)

	followerServer := c.Servers[(leaderVal+1)%3]

	require.NoError(t, c.ShutdownServer(followerServer))

	//find the new leader
	newLeader := agreedLeader(t, c, leaderVal, (leaderVal+2)%3)
	require.NotEqual(t, newLeader, -1)

	txID, rcpt, err := c.Servers[newLeader].WriteDataTx(t, worldstate.DefaultDBName, "bob", []byte{uint8(newLeader)})
	if err != nil {
		t.Logf("txID: %s, error: %s", txID, err)
	} else {
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}

	require.NoError(t, c.StartServer(followerServer))

	var dataEnv *types.GetDataResponseEnvelope
	require.Eventually(t, func() bool {
		dataEnv, err = followerServer.QueryData(t, worldstate.DefaultDBName, "bob")
		return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
	}, 30*time.Second, 100*time.Millisecond)

	dataResp := dataEnv.GetResponse().GetValue()
	require.Equal(t, dataResp, []byte{uint8(newLeader)})
	t.Logf("data: %+v", dataResp)
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

	leaderVal := agreedLeader(t, c, 0, 1, 2)
	require.NotEqual(t, leaderVal, -1)

	require.NoError(t, c.ShutdownServer(c.Servers[leaderVal]))

	//find the new leader
	newLeader := agreedLeader(t, c, (leaderVal+1)%3, (leaderVal+2)%3)
	require.NotEqual(t, newLeader, -1)

	txID, rcpt, err := c.Servers[newLeader].WriteDataTx(t, worldstate.DefaultDBName, "bob", []byte{uint8(newLeader)})
	require.NoError(t, err)
	if err != nil {
		t.Logf("txID: %s, error: %s", txID, err)
	} else {
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}

	require.NoError(t, c.StartServer(c.Servers[leaderVal]))

	var dataEnv *types.GetDataResponseEnvelope
	require.Eventually(t, func() bool {
		dataEnv, err = c.Servers[leaderVal].QueryData(t, worldstate.DefaultDBName, "bob")
		return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
	}, 30*time.Second, 100*time.Millisecond)

	dataResp := dataEnv.GetResponse().GetValue()
	require.Equal(t, dataResp, []byte{uint8(newLeader)})
	t.Logf("data: %+v", dataResp)
}

//Scenario:
// - start 3 servers in a cluster.
// - wait for one to be the leader.
// - stop the leader and one follower.
// - submit a tx to the remaining server.
// - there is no majority to peak leader => tx fails.
// - restart one server so now there will be a majority to peak a leader.
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

	leaderVal := agreedLeader(t, c, 0, 1, 2)
	require.NotEqual(t, leaderVal, -1)

	follower1 := (leaderVal + 1) % 3
	follower2 := (leaderVal + 2) % 3

	require.NoError(t, c.ShutdownServer(c.Servers[leaderVal]))
	require.NoError(t, c.ShutdownServer(c.Servers[follower1]))

	//only one server is active now => no majority to peak leader
	require.Eventually(t, func() bool {
		_, _, err = c.Servers[follower2].WriteDataTx(t, worldstate.DefaultDBName, "bob", []byte{uint8(follower2)})
		return err.Error() == "failed to submit transaction, server returned: status: 503 Service Unavailable, message: Cluster leader unavailable"
	}, 30*time.Second, 100*time.Millisecond)

	//restart one server => 2 servers are active
	require.NoError(t, c.StartServer(c.Servers[leaderVal]))

	//find the new leader
	newLeader := agreedLeader(t, c, leaderVal, follower2)
	require.NotEqual(t, newLeader, -1)

	txID, rcpt, err := c.Servers[newLeader].WriteDataTx(t, worldstate.DefaultDBName, "bob", []byte{uint8(newLeader)})
	if err != nil {
		t.Logf("txID: %s, error: %s", txID, err)
	} else {
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}

	var dataEnv *types.GetDataResponseEnvelope
	require.Eventually(t, func() bool {
		dataEnv, err = c.Servers[newLeader].QueryData(t, worldstate.DefaultDBName, "bob")
		return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
	}, 30*time.Second, 100*time.Millisecond)

	dataResp := dataEnv.GetResponse().GetValue()
	require.Equal(t, dataResp, []byte{uint8(newLeader)})
	t.Logf("data: %+v", dataResp)

}

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
	leaderVal := agreedLeader(t, c, 0, 1, 2)
	require.NotEqual(t, leaderVal, -1)

	keys := []string{"alice", "bob"}
	for _, key := range keys {
		txID, _, err := c.Servers[leaderVal].WriteDataTx(t, worldstate.DefaultDBName, key, []byte{uint8(leaderVal)})
		require.NoError(t, err)
		t.Logf("txID: %s, error: %s", txID, err)
	}

	require.NoError(t, c.Restart())

	//find the new leader
	newLeader := agreedLeader(t, c, 0, 1, 2)
	require.NotEqual(t, newLeader, -1)

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
				require.Equal(t, dataVal, []byte{uint8(leaderVal)})
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
	currentLeader := agreedLeader(t, c, 0, 1, 2)
	oldLeader := -1
	for !leaders[0] || !leaders[1] || !leaders[2] {
		leaders[currentLeader] = true
		txID, rcpt, err := c.Servers[currentLeader].WriteDataTx(t, worldstate.DefaultDBName, strconv.Itoa(currentLeader), []byte{uint8(currentLeader)})
		require.NoError(t, err)
		if err != nil {
			t.Logf("txID: %s, error: %s", txID, err)
		} else {
			t.Logf("tx submitted: %s, %+v", txID, rcpt)
		}

		require.NoError(t, c.ShutdownServer(c.Servers[currentLeader]))
		if oldLeader != -1 {
			require.NoError(t, c.StartServer(c.Servers[oldLeader]))

		}
		oldLeader = currentLeader
		currentLeader = agreedLeader(t, c, (currentLeader+1)%3, (currentLeader+2)%3)
		require.NotEqual(t, currentLeader, -1)
	}

	require.NoError(t, c.StartServer(c.Servers[oldLeader]))
	currentLeader = agreedLeader(t, c, (currentLeader+1)%3, (currentLeader+2)%3)
	require.NotEqual(t, currentLeader, -1)

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
