// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"encoding/pem"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func createNewServer(c *setup.Cluster, conf *setup.Config, serverNum int) (*setup.Server, *types.PeerConfig, *types.NodeConfig, error) {
	newServer, err := setup.NewServer(uint64(serverNum), conf.TestDirAbsolutePath, conf.BaseNodePort, conf.BasePeerPort, conf.CheckRedirectFunc, c.GetLogger(), "join", math.MaxUint64)
	if err != nil {
		return nil, nil, nil, err
	}
	clusterCaPrivKey, clusterRootCAPemCert := c.GetKeyAndCA()
	decca, _ := pem.Decode(clusterRootCAPemCert)
	newServer.CreateCryptoMaterials(clusterRootCAPemCert, clusterCaPrivKey)
	if err != nil {
		return nil, nil, nil, err
	}
	server0 := c.Servers[0]
	newServer.SetAdmin(server0.AdminID(), server0.AdminCertPath(), server0.AdminKeyPath(), server0.AdminSigner())

	newPeer := &types.PeerConfig{
		NodeId:   "node-" + strconv.Itoa(serverNum+1),
		RaftId:   uint64(serverNum + 1),
		PeerHost: "127.0.0.1",
		PeerPort: conf.BasePeerPort + uint32(serverNum),
	}

	newNode := &types.NodeConfig{
		Id:          "node-" + strconv.Itoa(serverNum+1),
		Address:     "127.0.0.1",
		Port:        conf.BaseNodePort + uint32(serverNum),
		Certificate: decca.Bytes,
	}

	return newServer, newPeer, newNode, nil
}

func addServerTx(t *testing.T, c *setup.Cluster, setupConfig *setup.Config, configEnv *types.GetConfigResponseEnvelope, leaderIndex int, serverNum int, conf *config.LocalConfiguration) *setup.Server {
	newServer, newPeer, newNode, err := createNewServer(c, setupConfig, serverNum)
	require.NoError(t, err)
	require.NotNil(t, newServer)
	require.NotNil(t, newPeer)
	require.NotNil(t, newNode)
	require.NoError(t, newServer.CreateConfigFile(conf))

	newConfig := configEnv.GetResponse().GetConfig()
	newConfig.ConsensusConfig.Members = append(newConfig.ConsensusConfig.Members, newPeer)
	newConfig.Nodes = append(newConfig.Nodes, newNode)

	// add a server with config-tx
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

	err = ioutil.WriteFile(newServer.BootstrapFilePath(), configBlockResponseEnvelope.Response.Block, 0666)
	require.NoError(t, err)

	config, err := config.Read(newServer.ConfigFilePath())
	require.NoError(t, err)
	require.NotNil(t, config.LocalConfig)
	require.Nil(t, config.SharedConfig)
	require.NotNil(t, config.JoinBlock)

	c.AddNewServerToCluster(newServer)

	return newServer
}

// Scenario:
// - Start a 3 node cluster
// - add a new server (node-4)
// - start the new server
func TestBasicAddServer(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(4)
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
	leaderServer := c.Servers[leaderIndex]

	statusResponseEnvelope, err := leaderServer.QueryClusterStatus(t)
	require.Equal(t, 3, len(statusResponseEnvelope.GetResponse().Nodes))
	require.Equal(t, 3, len(statusResponseEnvelope.GetResponse().Active))

	//get current cluster config
	configEnv, err := leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)

	// adding node-4
	newServer := addServerTx(t, c, setupConfig, configEnv, leaderIndex, 3, &config.LocalConfiguration{})

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 2, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	t.Logf("starting node-4")
	require.NoError(t, c.StartServer(newServer))

	leaderIndex = -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1, 2, 3)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	statusResponseEnvelope, err = c.Servers[leaderIndex].QueryClusterStatus(t)
	require.Equal(t, 4, len(statusResponseEnvelope.GetResponse().Nodes))
	require.Equal(t, 4, len(statusResponseEnvelope.GetResponse().Active))
}

// Scenario:
// - Start a 1 node cluster
// - add 2 new servers
func TestAddFrom1To3(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(3)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
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
		leaderIndex = c.AgreedLeader(t, 0)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)
	leaderServer := c.Servers[leaderIndex]

	statusResponseEnvelope, err := leaderServer.QueryClusterStatus(t)
	require.Equal(t, 1, len(statusResponseEnvelope.GetResponse().Nodes))
	require.Equal(t, 1, len(statusResponseEnvelope.GetResponse().Active))

	configEnv, err := leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)

	t.Logf("adding node-2")
	server2 := addServerTx(t, c, setupConfig, configEnv, leaderIndex, 1, &config.LocalConfiguration{})

	t.Logf("starting node-2")
	require.NoError(t, c.StartServer(server2))

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 2, 0, 1)
	}, 30*time.Second, 100*time.Millisecond)

	// find new leader
	leaderIndex = -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)
	leaderServer = c.Servers[leaderIndex]

	configEnv, err = leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)

	require.Eventually(t, func() bool {
		statusResponseEnvelope, err = c.Servers[leaderIndex].QueryClusterStatus(t)
		return len(statusResponseEnvelope.GetResponse().Nodes) == 2 && len(statusResponseEnvelope.GetResponse().Active) == 2
	}, 30*time.Second, 100*time.Millisecond)

	t.Logf("adding node-3")
	server3 := addServerTx(t, c, setupConfig, configEnv, leaderIndex, 2, &config.LocalConfiguration{})

	t.Logf("starting node-3")
	require.NoError(t, c.StartServer(server3))

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 3, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

	leaderIndex = -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1, 2)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	t.Logf("submit data tx and verify it is received by all 3 nodes")

	txID, rcpt, _, err := c.Servers[leaderIndex].WriteDataTx(t, worldstate.DefaultDBName, "alice", []byte("alice-data"))
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 4, 0, 1, 2)
	}, 30*time.Second, 100*time.Millisecond)

}

// Scenario:
// - Start a 3 node cluster
// - failed to submit transactions:
//  1. add 2 nodes in one tx
//  2. add node to Members only without also adding to Nodes
//  3. add node with invalid port
//  4. add node with invalid cert
//  5. add node with invalid raft-id
func TestInvalidAdd(t *testing.T) {
	dir := t.TempDir()

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

	statusResponseEnvelope, err := c.Servers[leaderIndex].QueryClusterStatus(t)
	require.Equal(t, 3, len(statusResponseEnvelope.GetResponse().Nodes))
	require.Equal(t, 3, len(statusResponseEnvelope.GetResponse().Active))

	// adding nodes 4 and 5 in one tx
	configEnv, err := c.Servers[leaderIndex].QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)

	t.Logf("create node-4")
	server4, peer4, node4, err := createNewServer(c, setupConfig, 3)
	require.NoError(t, err)
	require.NotNil(t, server4)
	require.NotNil(t, peer4)
	require.NotNil(t, node4)
	require.NoError(t, server4.CreateConfigFile(&config.LocalConfiguration{}))

	t.Logf("create node-5")
	server5, peer5, node5, err := createNewServer(c, setupConfig, 4)
	require.NoError(t, err)
	require.NotNil(t, server5)
	require.NotNil(t, peer5)
	require.NotNil(t, node5)
	require.NoError(t, server5.CreateConfigFile(&config.LocalConfiguration{}))

	t.Logf("add nodes 4-5 in one tx")
	newConfig := configEnv.GetResponse().GetConfig()
	newConfig.ConsensusConfig.Members = append(newConfig.ConsensusConfig.Members, peer4, peer5)
	newConfig.Nodes = append(newConfig.Nodes, node4, node5)

	_, _, err = c.Servers[leaderIndex].SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: "+
		"Invalid config tx, reason: error in ConsensusConfig: cannot make more than one membership change at a time: 2 added, 0 removed")

	t.Logf("add nodes 4 to Members only")
	configEnv, err = c.Servers[leaderIndex].QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig = configEnv.GetResponse().GetConfig()
	newConfig.ConsensusConfig.Members = append(newConfig.ConsensusConfig.Members, peer4)

	_, _, err = c.Servers[leaderIndex].SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: "+
		"Invalid config tx, reason: ClusterConfig.Nodes must be the same length as ClusterConfig.ConsensusConfig.Members, and Nodes set must include all Members")

	t.Logf("add node with invalid port")
	configEnv, err = c.Servers[leaderIndex].QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)

	peer4.PeerPort = setupConfig.BaseNodePort + uint32(1)
	node4.Port = setupConfig.BaseNodePort + uint32(1)

	newConfig = configEnv.GetResponse().GetConfig()
	newConfig.ConsensusConfig.Members = append(newConfig.ConsensusConfig.Members, peer4)
	newConfig.Nodes = append(newConfig.Nodes, node4)

	_, _, err = c.Servers[leaderIndex].SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, "+
		"reason: there are two nodes with the same Host:Port [127.0.0.1:"+strconv.Itoa(int(node4.Port))+"] in the node config. Endpoints must be unique")

	t.Logf("add node with invalid cert")
	configEnv, err = c.Servers[leaderIndex].QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)

	node5.Certificate = []byte("invalidCert")
	newConfig = configEnv.GetResponse().GetConfig()
	newConfig.ConsensusConfig.Members = append(newConfig.ConsensusConfig.Members, peer5)
	newConfig.Nodes = append(newConfig.Nodes, node5)

	_, _, err = c.Servers[leaderIndex].SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx,"+
		" reason: the node [node-5] has an invalid certificate: error parsing certificate: x509: malformed certificate")

	t.Logf("create node-6")
	_, peer6, node6, err := createNewServer(c, setupConfig, 5)
	require.NoError(t, err)
	require.NotNil(t, peer6)
	require.NotNil(t, node6)

	t.Logf("add node-6 with invalid RaftId")

	configEnv, err = c.Servers[leaderIndex].QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)

	peer6.RaftId = 3
	newConfig = configEnv.GetResponse().GetConfig()
	newConfig.ConsensusConfig.Members = append(newConfig.ConsensusConfig.Members, peer6)
	newConfig.Nodes = append(newConfig.Nodes, node6)
	_, _, err = c.Servers[leaderIndex].SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), c.Servers[leaderIndex].AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, "+
		"reason: Consensus config has two members with the same Raft ID [3], Raft IDs must be unique.")
}

// Scenario:
// - Start a 3 node cluster
// - add 3 new servers (4,5,6)
// - remove servers 1,2,3
// - shutdown and remove node 4
// - restart nodes 5,6
// - add node 7
// - restart nodes 5,6
// - start node 7
func TestAddAndRemove(t *testing.T) {
	dir := t.TempDir()

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

	statusResponseEnvelope, err := c.Servers[leaderIndex].QueryClusterStatus(t)
	require.Equal(t, 3, len(statusResponseEnvelope.GetResponse().Nodes))
	require.Equal(t, 3, len(statusResponseEnvelope.GetResponse().Active))

	// add nodes 4-5-6
	activeServers := []int{0, 1, 2}
	for i := 3; i < 6; i++ {
		configEnv, err := c.Servers[leaderIndex].QueryConfig(t, "admin")
		require.NoError(t, err)
		require.NotNil(t, configEnv)

		t.Logf("adding node-%d", i+1)
		newServer := addServerTx(t, c, setupConfig, configEnv, leaderIndex, i, &config.LocalConfiguration{})

		t.Logf("starting node-%d", i+1)
		require.NoError(t, c.StartServer(newServer))

		activeServers = append(activeServers, i)

		leaderIndex = -1
		require.Eventually(t, func() bool {
			leaderIndex = c.AgreedLeader(t, activeServers...)
			return leaderIndex >= 0
		}, 30*time.Second, 100*time.Millisecond)
	}

	require.Eventually(t, func() bool {
		return c.AgreedHeight(t, 4, activeServers...)
	}, 30*time.Second, 100*time.Millisecond)

	// remove nodes 1-2-3
	leaderIndex = removeServer(t, c.Servers[leaderIndex], c, 0, []int{1, 2, 3, 4, 5}, true, true)
	leaderIndex = removeServer(t, c.Servers[leaderIndex], c, 1, []int{2, 3, 4, 5}, true, true)
	removeServer(t, c.Servers[leaderIndex], c, 2, []int{3, 4, 5}, true, true)

	// shutdown and remove node 4
	require.NoError(t, c.ShutdownServer(c.Servers[3]))
	leaderIndex = -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, []int{4, 5}...)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)
	leaderIndex = removeServer(t, c.Servers[leaderIndex], c, 3, []int{4, 5}, true, false)

	// nodes in cluster - 5,6
	require.NoError(t, c.ShutdownServer(c.Servers[4]))
	require.NoError(t, c.ShutdownServer(c.Servers[5]))
	require.NoError(t, c.StartServer(c.Servers[4]))
	require.NoError(t, c.StartServer(c.Servers[5]))

	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, []int{4, 5}...)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	// add node 7
	configEnv, err := c.Servers[leaderIndex].QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	t.Logf("adding node-7")
	newServer := addServerTx(t, c, setupConfig, configEnv, leaderIndex, 6, &config.LocalConfiguration{})

	require.NoError(t, c.ShutdownServer(c.Servers[4]))
	require.NoError(t, c.ShutdownServer(c.Servers[5]))
	require.NoError(t, c.StartServer(c.Servers[4]))
	require.NoError(t, c.StartServer(c.Servers[5]))

	t.Logf("starting node-7")
	require.NoError(t, c.StartServer(newServer))
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, []int{4, 5, 6}...)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)
}
