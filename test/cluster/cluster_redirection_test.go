// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package cluster

import (
	"strconv"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
)

// Scenario:
// - Start a 3 node cluster with redirection allowed
// - Submit txs to all nodes
// - Tx redirection from follower to leader
func TestRedirection(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     3,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		CheckRedirectFunc:   nil,
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())
	// wait for leader
	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0, 1, 2)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	for i := 0; i < 3; i++ {
		txID, rcpt, _, err := c.Servers[i].WriteDataTx(t, worldstate.DefaultDBName, "key-"+strconv.Itoa(i), []byte("data-"+strconv.Itoa(i)))
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}

	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			require.Eventually(t, func() bool {
				dataEnv, err := c.Servers[i].QueryData(t, worldstate.DefaultDBName, "key-"+strconv.Itoa(j), "admin")
				if dataEnv != nil && dataEnv.GetResponse().GetValue() != nil {
					dataVal := dataEnv.GetResponse().GetValue()
					require.Equal(t, dataVal, []byte("data-"+strconv.Itoa(j)))
					t.Logf("data: %+v", dataVal)
				}
				return dataEnv != nil && dataEnv.GetResponse().GetValue() != nil && err == nil
			}, 30*time.Second, 100*time.Millisecond)
		}
	}
}
