package user

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
)

func TestUserUpdateACL(t *testing.T) {
	dir, err := ioutil.TempDir("", "int-test")
	require.NoError(t, err)

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort + 5,
		BasePeerPort:        pPort + 5,
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
	charlieCert, _ := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "charlie")

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
						"db1": types.Privilege_ReadWrite,
					},
				},
			},
		},
		{
			User: &types.User{
				Id:          "charlie",
				Certificate: charlieCert.Raw,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_ReadWrite,
					},
				},
			},
		},
	}
	setup.CreateUsers(t, s, users)

	// Test 1: check admin permissions - admin has the permission to read info of all users,
	// hence it should return no error
	_, err = s.QueryUser(t, "admin", "alice")
	require.NoError(t, err)

	_, err = s.QueryUser(t, "admin", "bob")
	require.NoError(t, err)

	_, err = s.QueryUser(t, "admin", "charlie")
	require.NoError(t, err)

	// Test 2: check user permissions to access other user records
	// charlie has no permission to read info of user alice, hence it should return an error
	_, err = s.QueryUser(t, "charlie", "alice")
	require.Error(t, err)

	// bob has no permission to read info of user charlie, hence it should return with an error
	_, err = s.QueryUser(t, "bob", "charlie")
	require.Error(t, err)

	// alice has no permission to read info of user bob, hence it should return an error
	_, err = s.QueryUser(t, "alice", "bob")
	require.Error(t, err)
}
