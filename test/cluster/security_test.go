// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/replication"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// Scenario:
// - start 3 servers in a cluster with TLS enabled.
// - stop the leader.
// - submit multiple txs to the two remaining servers.
// - start the stopped server.
// - wait for one to be the new leader.
// - make sure the stopped server is in sync with the txs made while it was stopped.
func TestNodeRecoveryWithTLS(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(3)
	setupConfig := &setup.Config{
		NumberOfServers:     3,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		ClusterTLSEnabled:   true,
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

	follower1 := (leaderIndex + 1) % 3
	follower2 := (leaderIndex + 2) % 3

	require.NoError(t, c.ShutdownServer(c.Servers[leaderIndex]))

	//find the new leader
	newLeader := -1
	require.Eventually(t, func() bool {
		newLeader = c.AgreedLeader(t, follower1, follower2)
		return newLeader >= 0
	}, 30*time.Second, 100*time.Millisecond)

	for i := 0; i < 20; i++ {
		txID, rcpt, _, err := c.Servers[newLeader].WriteDataTx(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), []byte{uint8(i)})
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}

	require.NoError(t, c.StartServer(c.Servers[leaderIndex]))

	var dataEnv *types.GetDataResponseEnvelope
	for i := 0; i < 20; i++ {
		require.Eventually(t, func() bool {
			dataEnv, err = c.Servers[leaderIndex].QueryData(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), "admin")
			return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
		}, 30*time.Second, 100*time.Millisecond)

		dataResp := dataEnv.GetResponse().GetValue()
		require.Equal(t, dataResp, []byte{uint8(i)})
		t.Logf("data: %+v", string(dataResp))
	}

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 21, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)
}

// Scenario:
// - start 3 servers in a cluster with TLS enabled and change SnapshotIntervalSize to 4K.
// - submit data txs.
// - shutdown the leader/follower.
// - submit data txs.
// - restart the server.
// - make sure the server is in sync with previous txs.
func TestNodeRecoveryWithCatchupAndTLS(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(3)
	setupConfig := &setup.Config{
		NumberOfServers:     3,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		ClusterTLSEnabled:   true,
		CheckRedirectFunc: func(req *http.Request, via []*http.Request) error {
			return errors.Errorf("Redirect blocked in test client: url: '%s', referrer: '%s', #via: %d", req.URL, req.Referer(), len(via))
		},
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())
	clusterLogger := c.GetLogger()

	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1, 2)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	snapList := replication.ListSnapshots(c.GetLogger(), filepath.Join(c.Servers[leaderIndex].ConfigDir(), "etcdraft", "snap"))
	require.Equal(t, 0, len(snapList))
	clusterLogger.Info("Snap list: ", snapList)

	//get current cluster config
	configEnv, err := c.Servers[leaderIndex].QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)

	newConfig := configEnv.GetResponse().GetConfig()
	newConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize = 4 * 1024
	version := configEnv.GetResponse().GetMetadata().GetVersion()
	txsCount := 0

	//change SnapshotIntervalSize to 4K
	txID, rcpt, err := c.Servers[leaderIndex].SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner(), "admin")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	txsCount++
	clusterLogger.Info("tx submitted: "+txID+", ", rcpt)

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, uint64(txsCount+1), 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	//restart the cluster so that the SnapshotIntervalSize will update
	require.NoError(t, c.Restart())

	leaderIndex = -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1, 2)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	keys := createKeys(100)
	data := make([]byte, 1024)
	for _, key := range keys {
		txID, rcpt, _, err := c.Servers[leaderIndex].WriteDataTx(t, worldstate.DefaultDBName, strconv.Itoa(key), data)
		clusterLogger.Info("key-", key)
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		txsCount++
		clusterLogger.Info("tx submitted: "+txID+", ", rcpt)
	}

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, uint64(txsCount+1), 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	follower1 := (leaderIndex + 1) % 3
	follower2 := (leaderIndex + 2) % 3

	require.NoError(t, c.ShutdownServer(c.Servers[leaderIndex]))

	//find new leader
	newLeaderIndex := -1
	require.Eventually(t, func() bool {
		newLeaderIndex = c.AgreedLeader(t, follower1, follower2)
		return newLeaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	//submit txs with data size > SnapshotIntervalSize
	for _, key := range keys {
		txID, rcpt, _, err := c.Servers[newLeaderIndex].WriteDataTx(t, worldstate.DefaultDBName, strconv.Itoa(key+100), data)
		clusterLogger.Info("key-", key+100)
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		txsCount++
		clusterLogger.Info("tx submitted: "+txID+", ", rcpt)
	}

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, uint64(txsCount+1), follower1, follower2)
	}, 30*time.Second, 100*time.Millisecond)

	//restart the victim
	require.NoError(t, c.StartServer(c.Servers[leaderIndex]))

	//make sure the server is in sync with previous txs
	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, uint64(txsCount+1), 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	keys = createKeys(200)
	var dataEnv *types.GetDataResponseEnvelope
	for _, key := range keys {
		require.Eventually(t, func() bool {
			dataEnv, err = c.Servers[follower1].QueryData(t, worldstate.DefaultDBName, strconv.Itoa(key), "admin")
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

	require.NoError(t, c.Shutdown())

	snapList = replication.ListSnapshots(clusterLogger, filepath.Join(c.Servers[newLeaderIndex].ConfigDir(), "etcdraft", "snap"))
	require.NotEqual(t, 0, len(snapList))
	clusterLogger.Info("Snap list: ", snapList)
}

// Scenario:
// - start 3 servers in a cluster with TLS enabled.
// - create server with TLS disabled, fail to start the new server.
// - create server with TLS enabled but a key-cert pair not from the cluster's CA, fail to start the new server.
func TestTLSAddInvalidNodes(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(3)
	setupConfig := &setup.Config{
		NumberOfServers:     3,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		ClusterTLSEnabled:   true,
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

	// create server with TLS disabled
	t.Logf("create node-4 with TLS disabled")
	configEnv, err := c.Servers[leaderIndex].QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newServer := addServerTx(t, c, setupConfig, configEnv, leaderIndex, 3, &config.LocalConfiguration{})
	require.NotNil(t, newServer)
	require.EqualError(t, c.StartServer(newServer), "failed to start the server: node-4")

	// create server with TLS enabled but a key-cert pair not from the cluster's CA
	t.Logf("create node-5 with TLS enabled")
	server5, peer5, node5, err := createNewServer(c, setupConfig, 4)
	require.NoError(t, err)
	require.NotNil(t, server5)
	require.NotNil(t, peer5)
	require.NotNil(t, node5)

	// create new key-cert
	certRootCA, rootCAPrivKey, err := testutils.GenerateRootCA("Orion RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, certRootCA)
	require.NotNil(t, rootCAPrivKey)

	keyPath, certPath, rootCACertPath, err := server5.CreateNewCryptoMaterials(certRootCA, rootCAPrivKey)
	localConfig := &config.LocalConfiguration{
		Replication: config.ReplicationConf{
			TLS: config.TLSConf{
				Enabled:               true,
				ServerCertificatePath: certPath,
				ServerKeyPath:         keyPath,
				ClientCertificatePath: certPath,
				ClientKeyPath:         keyPath,
				CaConfig: config.CAConfiguration{
					RootCACertsPath: []string{rootCACertPath},
				},
			},
		},
	}
	require.NoError(t, server5.CreateConfigFile(localConfig))

	configEnv, err = c.Servers[leaderIndex].QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig := configEnv.GetResponse().GetConfig()
	newConfig.ConsensusConfig.Members = append(newConfig.ConsensusConfig.Members, peer5)
	newConfig.Nodes = append(newConfig.Nodes, node5)

	// add server with config-tx
	txID, rcpt, err := c.Servers[leaderIndex].SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	configBlockResponseEnvelope, err := c.Servers[leaderIndex].QueryConfigBlockStatus(t)
	require.NoError(t, err)
	require.NotNil(t, configBlockResponseEnvelope)

	err = ioutil.WriteFile(server5.BootstrapFilePath(), configBlockResponseEnvelope.Response.Block, 0666)
	require.NoError(t, err)

	config, err := config.Read(server5.ConfigFilePath())
	require.NoError(t, err)
	require.NotNil(t, config.LocalConfig)
	require.Nil(t, config.SharedConfig)
	require.NotNil(t, config.JoinBlock)

	c.AddNewServerToCluster(server5)
	require.EqualError(t, c.StartServer(server5), "failed to start the server: node-5")
}

// Scenario:
// - start a 3 servers cluster with TLS enabled.
// - change server-2 config file.
func TestTLSChangeNodeCA(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(3)
	setupConfig := &setup.Config{
		NumberOfServers:     3,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		ClusterTLSEnabled:   true,
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

	s2 := c.Servers[2]
	require.NoError(t, c.ShutdownServer(s2))

	leaderIndex = -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	certRootCA, rootCAPrivKey, err := testutils.GenerateRootCA("Orion RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, certRootCA)
	require.NotNil(t, rootCAPrivKey)

	keyPath, certPath, rootCACertPath, err := s2.CreateNewCryptoMaterials(certRootCA, rootCAPrivKey)
	localConfig := &config.LocalConfiguration{
		Replication: config.ReplicationConf{
			TLS: config.TLSConf{
				Enabled:               true,
				ServerCertificatePath: certPath,
				ServerKeyPath:         keyPath,
				ClientCertificatePath: certPath,
				ClientKeyPath:         keyPath,
				CaConfig: config.CAConfiguration{
					RootCACertsPath: []string{rootCACertPath},
				},
			},
		},
	}
	require.NoError(t, s2.CreateConfigFile(localConfig))
	require.NoError(t, c.StartServer(s2))

	// submit tx
	txID, rcpt, _, err := c.Servers[leaderIndex].WriteDataTx(t, worldstate.DefaultDBName, "key1", []byte("key1-data"))
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 2, 0, 1)
	}, 30*time.Second, 100*time.Millisecond)

	// server2 is not sync with the last tx, because it is unable to connect to the cluster
	dataEnv, err := c.Servers[2].QueryData(t, worldstate.DefaultDBName, "key1", "admin")
	require.NoError(t, err)
	require.NotNil(t, dataEnv)
	require.Equal(t, []byte(nil), dataEnv.GetResponse().GetValue())

	require.Eventually(t, func() bool {
		statusResponseEnvelope, err := c.Servers[2].QueryClusterStatus(t)
		return err == nil && statusResponseEnvelope.GetResponse().Leader == "" // server 2 has no leader
	}, 60*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		statusResponseEnvelope, err := c.Servers[leaderIndex].QueryClusterStatus(t)
		return err == nil && len(statusResponseEnvelope.GetResponse().Active) == 2
	}, 60*time.Second, 100*time.Millisecond)
}
