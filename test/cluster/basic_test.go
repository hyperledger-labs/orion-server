// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"fmt"
	"io/ioutil"
	"net/http"
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
