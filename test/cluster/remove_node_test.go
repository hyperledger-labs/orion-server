package cluster

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func getServerIndexMembers(members []*types.PeerConfig, serverID int) int {
	for i := 0; i < len(members); i++ {
		if members[i].GetNodeId() == ("node-" + strconv.Itoa(serverID+1)) {
			return i
		}
	}
	return -1
}

func getServerIndexNodes(nodes []*types.NodeConfig, serverID int) int {
	for i := 0; i < len(nodes); i++ {
		if nodes[i].Id == ("node-" + strconv.Itoa(serverID+1)) {
			return i
		}
	}
	return -1
}

func removeServerFromMembers(members []*types.PeerConfig, serverID int) []*types.PeerConfig {
	switch serverID {
	case 0:
		members = members[1:]
	case len(members) - 1:
		members = members[:len(members)-1]
	default:
		members = append(members[:serverID], members[serverID+1:]...)
	}
	return members
}

func removeServerFromNodes(nodes []*types.NodeConfig, serverID int) []*types.NodeConfig {
	switch serverID {
	case 0:
		nodes = nodes[1:]
	case len(nodes) - 1:
		nodes = nodes[:len(nodes)-1]
	default:
		nodes = append(nodes[:serverID], nodes[serverID+1:]...)
	}
	return nodes
}

func removeServer(t *testing.T, leaderServer *setup.Server, c *setup.Cluster, removeIndex int, activeServersAfterRemove []int, majority bool, removedAlive bool) int {
	configEnv, err := leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig := configEnv.GetResponse().GetConfig()

	membersIndex := getServerIndexMembers(newConfig.ConsensusConfig.Members, removeIndex)
	require.NotEqual(t, -1, membersIndex)
	nodesIndex := getServerIndexNodes(newConfig.Nodes, removeIndex)
	require.NotEqual(t, -1, nodesIndex)
	newConfig.ConsensusConfig.Members = removeServerFromMembers(newConfig.ConsensusConfig.Members, membersIndex)
	newConfig.Nodes = removeServerFromNodes(newConfig.Nodes, nodesIndex)

	txID, rcpt, err := leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), leaderServer.AdminSigner(), "admin")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("Removed server index: %d, tx submitted: %s, %+v", removeIndex, txID, rcpt)

	leaderIndex := -1
	if majority {
		require.Eventually(t, func() bool {
			leaderIndex = c.AgreedLeader(t, activeServersAfterRemove...)
			return leaderIndex >= 0
		}, 60*time.Second, 100*time.Millisecond)

		statusResponseEnvelope, err := c.Servers[leaderIndex].QueryClusterStatus(t)
		require.NoError(t, err)
		require.Equal(t, len(activeServersAfterRemove), len(statusResponseEnvelope.GetResponse().Nodes))
		require.Equal(t, len(activeServersAfterRemove), len(statusResponseEnvelope.GetResponse().Active))
	}
	if removedAlive {
		require.Eventually(t, func() bool {
			removedServerResponse, err := c.Servers[removeIndex].QueryClusterStatus(t)
			return err == nil && removedServerResponse.GetResponse().Leader == ""
		}, 60*time.Second, 100*time.Millisecond)
	}

	return leaderIndex
}

// Scenario:
// - Start a 3 node cluster
// - remove leader
// - remove follower
func TestRemoveLeaderAndFollower(t *testing.T) {
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
	leaderServer := c.Servers[leaderIndex]

	statusResponseEnvelope, err := leaderServer.QueryClusterStatus(t)
	require.Equal(t, 3, len(statusResponseEnvelope.GetResponse().Nodes))
	require.Equal(t, 3, len(statusResponseEnvelope.GetResponse().Active))

	// remove leader
	follower1 := (leaderIndex + 1) % 3
	follower2 := (leaderIndex + 2) % 3
	newLeaderIndex := removeServer(t, leaderServer, c, leaderIndex, []int{follower1, follower2}, true, true)

	//remove follower1
	if follower1 == newLeaderIndex {
		follower1 = follower2
	}
	removeServer(t, c.Servers[newLeaderIndex], c, follower1, []int{newLeaderIndex}, true, true)
}

// Scenario:
// - Start a 5 node cluster
// - shut down nodes 2,3
// - remove 5 (1,4,5 accept this TX and commit it, 5 stops itself).
// - nodes 1,4 up, 2,3, down => no quorum.
// - start 2 => 3 nodes are active => there is a majority to choose a leader
// - start 3
func TestRemoveNodeLossOfQuorum(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(5)
	setupConfig := &setup.Config{
		NumberOfServers:     5,
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
		leaderIndex = c.AgreedLeader(t, 0, 1, 2, 3, 4)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	statusResponseEnvelope, err := c.Servers[leaderIndex].QueryClusterStatus(t)
	require.Equal(t, 5, len(statusResponseEnvelope.GetResponse().Nodes))
	require.Equal(t, 5, len(statusResponseEnvelope.GetResponse().Active))

	// shut down 2,3
	require.NoError(t, c.ShutdownServer(c.Servers[1]))
	require.NoError(t, c.ShutdownServer(c.Servers[2]))

	//find new leader
	leaderIndex = -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 3, 4)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	// remove node 5 => no leader
	leaderIndex = removeServer(t, c.Servers[leaderIndex], c, 4, []int{0, 3}, false, true)
	require.Eventually(t, func() bool {
		statusResponseEnvelope, err = c.Servers[0].QueryClusterStatus(t)
		return statusResponseEnvelope.GetResponse().Leader == ""
	}, 30*time.Second, 100*time.Millisecond)

	//start node 2
	require.NoError(t, c.StartServer(c.Servers[1]))
	require.Eventually(t, func() bool {
		statusResponseEnvelope, err = c.Servers[1].QueryClusterStatus(t)
		return len(statusResponseEnvelope.GetResponse().Active) == 3 && statusResponseEnvelope.GetResponse().Leader != ""
	}, 30*time.Second, 100*time.Millisecond)

	//start node 3
	require.NoError(t, c.StartServer(c.Servers[2]))
	require.Eventually(t, func() bool {
		statusResponseEnvelope, err = c.Servers[2].QueryClusterStatus(t)
		return len(statusResponseEnvelope.GetResponse().Active) == 4 && statusResponseEnvelope.GetResponse().Leader != ""
	}, 30*time.Second, 100*time.Millisecond)
}

// Scenario:
// - try to remove node from Members only without also removing from Nodes
// - try to remove 2 servers in one tx
func TestInvalidRemove(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(5)
	setupConfig := &setup.Config{
		NumberOfServers:     5,
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
		leaderIndex = c.AgreedLeader(t, 0, 1, 2, 3, 4)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)
	leaderServer := c.Servers[leaderIndex]

	statusResponseEnvelope, err := leaderServer.QueryClusterStatus(t)
	require.Equal(t, 5, len(statusResponseEnvelope.GetResponse().Nodes))
	require.Equal(t, 5, len(statusResponseEnvelope.GetResponse().Active))

	// try to remove node from Members only without also removing from Nodes
	configEnv, err := leaderServer.QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	newConfig := configEnv.GetResponse().GetConfig()
	newConfig.ConsensusConfig.Members = newConfig.ConsensusConfig.Members[:4]

	_, _, err = leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), leaderServer.AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx, "+
		"reason: ClusterConfig.Nodes must be the same length as ClusterConfig.ConsensusConfig.Members, and Nodes set must include all Members")

	// try to remove 2 servers in one tx
	newConfig.ConsensusConfig.Members = newConfig.ConsensusConfig.Members[:3]
	newConfig.Nodes = newConfig.Nodes[:3]
	_, _, err = leaderServer.SetConfigTx(t, newConfig, configEnv.GetResponse().GetMetadata().GetVersion(), leaderServer.AdminSigner(), "admin")
	require.EqualError(t, err, "failed to submit transaction, server returned: status: 400 Bad Request, message: Invalid config tx,"+
		" reason: error in ConsensusConfig: cannot make more than one membership change at a time: 0 added, 2 removed")
}
