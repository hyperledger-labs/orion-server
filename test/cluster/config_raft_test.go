// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"net/http"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/types"

	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// Scenario:
// - change the ticks, heartbeat interval, and election timeout (200ms, 4, 40)
// - stop the leader and make sure a new leader is elected.
func TestUpdateRaft(t *testing.T) {
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
	//	leaderServer := c.Servers[leaderIndex]

	//get current cluster config
	configEnv, err := c.Servers[leaderIndex].QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)

	newConfig := configEnv.GetResponse().GetConfig()
	newConfig.ConsensusConfig.RaftConfig.TickInterval = "200ms"
	newConfig.ConsensusConfig.RaftConfig.HeartbeatTicks = 4
	newConfig.ConsensusConfig.RaftConfig.ElectionTicks = 40
	version := configEnv.GetResponse().GetMetadata().GetVersion()

	//Change the ticks, heartbeat interval, and election timeout (200ms, 4, 40)
	txID, rcpt, err := c.Servers[leaderIndex].SetConfigTx(t, newConfig, version, c.Servers[leaderIndex].AdminSigner(), "admin")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	require.True(t, txID != "")
	require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	//restart the cluster so that Raft parameters will update
	require.NoError(t, c.Restart())

	leaderIndex = -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1, 2)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	//find new leader
	c.ShutdownServer(c.Servers[leaderIndex])
	newLeaderIndex := -1
	require.Eventually(t, func() bool {
		newLeaderIndex = c.AgreedLeader(t, (leaderIndex+1)%3, (leaderIndex+2)%3)
		return newLeaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	configEnv, err = c.Servers[newLeaderIndex].QueryConfig(t, "admin")
	require.NoError(t, err)
	require.NotNil(t, configEnv)
	raftConfig := configEnv.GetResponse().GetConfig().GetConsensusConfig().GetRaftConfig()
	require.Equal(t, "200ms", raftConfig.GetTickInterval())
	require.Equal(t, uint32(4), raftConfig.GetHeartbeatTicks())
	require.Equal(t, uint32(40), raftConfig.GetElectionTicks())
}
