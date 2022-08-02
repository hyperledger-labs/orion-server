// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queries

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
)

// check that the provenance API enforces the data and user ACLs.
// only the admin user can read historical data.
// only the user and admin can fetch all operations performed by the user.
func TestProvenanceACL(t *testing.T) {
	dir, err := ioutil.TempDir("", "int-test")
	require.NoError(t, err)

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
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

	s := c.Servers[0]

	// create dbs
	setup.CreateDatabases(t, s, []string{"db1"}, nil)

	//create users
	aliceCert, _ := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "alice")
	bobCert, _ := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "bob")
	users := []*types.UserWrite{
		{
			User: &types.User{
				Id:          "alice",
				Certificate: aliceCert.Raw,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_ReadWrite,
					},
				},
			},
		},
		{
			User: &types.User{
				Id:          "bob",
				Certificate: bobCert.Raw,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_Read,
					},
				},
			},
		},
	}
	setup.CreateUsers(t, s, users)

	// alice write to db1
	txID, rcpt, _, err := s.UserWriteDataTx(t, "db1", "key1", []byte("data1"), "alice")
	require.NoError(t, err)
	require.NotNil(t, rcpt)
	t.Logf("tx submitted: %s, %+v", txID, rcpt)

	// only alice and admin can fetch values written by alice
	provRes, err := s.GetValuesWrittenByUser(t, "alice", "alice")
	require.NoError(t, err)
	require.NotNil(t, provRes)

	provRes, err = s.GetValuesWrittenByUser(t, "admin", "alice")
	require.NoError(t, err)
	require.NotNil(t, provRes)

	provRes, err = s.GetValuesWrittenByUser(t, "bob", "alice")
	require.EqualError(t, err, "error while processing 'GET /provenance/data/written/alice' because The querier [bob] is neither an admin nor requesting operations performed by [bob]. Only an admin can query operations performed by other users.")
	require.Nil(t, provRes)

	// only admin can read historical data
	statusRes, err := s.QueryClusterStatus(t)
	require.NoError(t, err)
	require.NotNil(t, statusRes)

	historyResp, err := s.GetValueAt(t, "db1", "key1", "admin", statusRes.GetResponse().GetVersion())
	require.NoError(t, err)
	require.NotNil(t, historyResp)

	historyResp, err = s.GetValueAt(t, "db1", "key1", "bob", statusRes.GetResponse().GetVersion())
	require.EqualError(t, err, "error while processing 'GET /provenance/data/history/db1/key1?blocknumber=1&transactionnumber=0' because The querier [bob] is not an admin. Only an admin can query historical data")
	require.Nil(t, historyResp)

	historyResp, err = s.GetPreviousValues(t, "db1", "key1", "alice", statusRes.GetResponse().GetVersion())
	require.EqualError(t, err, "error while processing 'GET /provenance/data/history/db1/key1?blocknumber=1&transactionnumber=0&direction=previous' because The querier [alice] is not an admin. Only an admin can query historical data")
	require.Nil(t, historyResp)
}
