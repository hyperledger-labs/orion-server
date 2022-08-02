// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queries

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// - Start a cluster with provenance switched off on server-2.
// - provenance queries to server-2 return error.
// - trying to restart server-2 with provenance on fails.
// - switching provenance off in server-0 that had it on is supported.
// - trying to restart server-0 with provenance on fails.
func TestProvenanceSwitchOff(t *testing.T) {
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
		DisableProvenanceServers: []int{2},
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

	insertData(t, c.Servers[leaderIndex])

	// provenance queries to node-3 return error
	s2 := c.Servers[2]
	_, err = s2.GetAllValues(t, worldstate.DefaultDBName, "key-1", "admin")
	require.EqualError(t, err, "error while processing 'GET /provenance/data/history/bdb/key-1' because provenance store is disabled on this server")

	_, err = s2.GetValuesReadByUser(t, "admin", "admin")
	require.EqualError(t, err, "error while processing 'GET /provenance/data/read/admin' because provenance store is disabled on this server")

	// provenance queries to other servers work
	historyRes, err := c.Servers[0].GetAllValues(t, worldstate.DefaultDBName, "key-1", "admin")
	require.NoError(t, err)
	require.NotNil(t, historyRes)
	require.Equal(t, []byte{uint8(1)}, historyRes.GetResponse().GetValues()[0].GetValue())

	historyRes, err = c.Servers[1].GetAllValues(t, worldstate.DefaultDBName, "key-2", "admin")
	require.NoError(t, err)
	require.NotNil(t, historyRes)
	require.Equal(t, []byte{uint8(2)}, historyRes.GetResponse().GetValues()[0].GetValue())

	// trying to restart node-3 with provenance on fails
	require.NoError(t, c.ShutdownServer(s2))
	conf := &config.LocalConfiguration{
		Server: config.ServerConf{
			Provenance: config.ProvenanceConf{
				Disabled: false,
			},
		},
	}
	require.NoError(t, s2.CreateConfigFile(conf))
	require.EqualError(t, c.StartServer(s2), "failed to start the server: node-3")

	// switching provenance off in node-1
	s0 := c.Servers[0]
	require.NoError(t, c.ShutdownServer(s0))
	conf.Server.Provenance.Disabled = true
	require.NoError(t, s0.CreateConfigFile(conf))
	require.NoError(t, c.StartServer(s0))

	// trying to restart node-1 provenance on fails
	conf.Server.Provenance.Disabled = false
	require.NoError(t, s0.CreateConfigFile(conf))
	require.EqualError(t, c.StartServer(s0), "failed to start the server: node-1")

}

func insertData(t *testing.T, s *setup.Server) {
	for i := 1; i < 5; i++ {
		txID, rcpt, _, err := s.WriteDataTx(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), []byte{uint8(i)})
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		t.Logf("tx submitted: %s, %+v", txID, rcpt)
	}
}
