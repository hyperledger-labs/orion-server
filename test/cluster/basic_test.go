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
