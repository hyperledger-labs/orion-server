package user

import (
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
)

func TestUserUpdateACL(t *testing.T) {
	dir := t.TempDir()

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
	_, err1 := s.QueryUser(t, "admin", "alice")
	require.NoError(t, err1)
	respEnv2, err2 := s.QueryUser(t, "admin", "bob")
	require.NoError(t, err2)
	_, err3 := s.QueryUser(t, "admin", "charlie")
	require.NoError(t, err3)

	// Test 2: check user permissions to access other user records
	// charlie has no permission to read info of user alice, hence it should return an error
	_, err = s.QueryUser(t, "charlie", "alice")
	require.Error(t, err)
	require.EqualError(t, err, "error while processing 'GET /user/alice' because the user [charlie] has no permission to read info of user [alice]")

	// bob has no permission to read info of user charlie, hence it should return with an error
	_, err = s.QueryUser(t, "bob", "charlie")
	require.Error(t, err)
	require.EqualError(t, err, "error while processing 'GET /user/charlie' because the user [bob] has no permission to read info of user [charlie]")

	// alice has no permission to read info of user bob, hence it should return an error
	_, err = s.QueryUser(t, "alice", "bob")
	require.Error(t, err)
	require.EqualError(t, err, "error while processing 'GET /user/bob' because the user [alice] has no permission to read info of user [bob]")

	// Test 3: check user permissions to access other user records
	// operating user can access target user records if the operating user is included in the target user acl
	// admin can access target user records even if not included in the acl of the operating user

	// 3.1
	// update bob acl to include charlie as a read user
	users[1].Acl = &types.AccessControl{
		ReadUsers: map[string]bool{"charlie": true},
	}

	bobRead := &types.UserRead{
		UserId:  respEnv2.Response.User.Id,
		Version: respEnv2.Response.Metadata.Version,
	}

	err = setup.UpdateUser(t, s, "admin", bobRead, users[1], nil)
	require.NoError(t, err)

	// Now charlie can read bob's info, hence it should return no error
	respEnv, err := s.QueryUser(t, "charlie", "bob")
	require.NoError(t, err)
	require.Equal(t, users[1].User, respEnv.GetResponse().User)

	// charlie still cannot read info of the user alice, hence it should return with an error
	_, err = s.QueryUser(t, "charlie", "alice")
	require.Error(t, err)
	require.EqualError(t, err, "error while processing 'GET /user/alice' because the user [charlie] has no permission to read info of user [alice]")

	// 3.2
	// admin was not added to the acl of bob, but has the permission to access bob record and modify it
	// admin update bob acl to include alice as a read user - should end with no error
	respEnv2, err2 = s.QueryUser(t, "admin", "bob")
	bobRead = &types.UserRead{
		UserId:  respEnv2.Response.User.Id,
		Version: respEnv2.Response.Metadata.Version,
	}
	users[1].Acl = &types.AccessControl{
		ReadUsers: map[string]bool{"charlie": true, "alice": true},
	}
	err = setup.UpdateUser(t, s, "admin", nil, users[1], nil)
	require.NoError(t, err)

	// 3.3
	// update bob acl to include only charlie as a read user, do it with an old version of bob should end with an error
	users[1].Acl = &types.AccessControl{
		ReadUsers: map[string]bool{"charlie": true},
	}
	err = setup.UpdateUser(t, s, "admin", bobRead, users[1], nil)
	require.Error(t, err)
	require.EqualError(t, err, "TxValidation: Flag: INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, Reason: mvcc conflict has occurred as the committed state for the user [bob] has changed")

	// Test 4: check user permissions to modify other user records
	// only admin can modify user records (write / delete), regular user cannot
	// hence, user record should not allow RW access to anyone but an admin.

	// update bob acl to include alice as a read-write user
	users[1].Acl = &types.AccessControl{
		ReadWriteUsers: map[string]bool{"alice": true},
	}

	err = setup.UpdateUser(t, s, "admin", nil, users[1], nil)
	require.Error(t, err)
	require.EqualError(t, err, "TxValidation: Flag: INVALID_INCORRECT_ENTRIES, Reason: adding users to Acl.ReadWriteUsers is not supported")

	// Test 5: update alice version to be random, so writing alice with outdated version should end with mvcc conflict
	aliceRead := &types.UserRead{
		UserId: "alice",
		Version: &types.Version{
			BlockNum: 1,
			TxNum:    1,
		},
	}

	err = setup.UpdateUser(t, s, "admin", aliceRead, users[0], nil)
	require.Error(t, err)
	require.EqualError(t, err, "TxValidation: Flag: INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, Reason: mvcc conflict has occurred as the committed state for the user [alice] has changed")

	// Test 6: admin cannot write another admin user in a userTX, only a config-tx.
	adminWrite := &types.UserWrite{
		User: &types.User{
			Id: "admin",
			Privilege: &types.Privilege{
				Admin: true,
			},
		},
	}

	err = setup.UpdateUser(t, s, "admin", nil, adminWrite, nil)
	require.Error(t, err)
	require.EqualError(t, err, "TxValidation: Flag: INVALID_NO_PERMISSION, Reason: the user [admin] is marked as admin user. Only via a cluster configuration transaction, the [admin] can be added as admin")

	// Test 7: deleting alice record by an admin should end with no error
	userDelete := &types.UserDelete{
		UserId: "alice",
	}
	err = setup.UpdateUser(t, s, "admin", nil, nil, userDelete)
	require.NoError(t, err)

	// verify alice record is no loner exist
	respEnv, err = s.QueryUser(t, "admin", "alice")
	actualRespEnv := &types.GetUserResponse{
		User:     respEnv.GetResponse().GetUser(),
		Metadata: respEnv.GetResponse().GetMetadata(),
	}
	respEnvExpected := &types.GetUserResponse{
		User:     nil,
		Metadata: nil,
	}
	require.NoError(t, err)
	require.Equal(t, respEnvExpected, actualRespEnv)

	// Test 8: deleting bob record by charlie should end with error, charlie has no permission to delete user records
	userDelete = &types.UserDelete{
		UserId: "bob",
	}
	err = setup.UpdateUser(t, s, "charlie", bobRead, nil, userDelete)
	require.Error(t, err)
	require.EqualError(t, err, "TxValidation: Flag: INVALID_NO_PERMISSION, Reason: the user [charlie] has no privilege to perform user administrative operations")

	// Test 9: deleting bob record which have an acl by admin should end with no error
	err = setup.UpdateUser(t, s, "admin", nil, nil, userDelete)
	require.NoError(t, err)

	// Test 10: deleting charlie record by admin should end with no error
	userDelete = &types.UserDelete{
		UserId: "charlie",
	}
	err = setup.UpdateUser(t, s, "admin", nil, nil, userDelete)
	require.NoError(t, err)
}
