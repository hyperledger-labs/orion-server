// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package user

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
)

var baseNodePort uint32
var basePeerPort uint32
var portMutex sync.Mutex

func init() {
	baseNodePort = 8123
	basePeerPort = 9134
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

func TestUserACLOnDatabase(t *testing.T) {
	dir := t.TempDir()

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

	createDatabases(t, s, []string{"db1", "db2", "db3"})

	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "alice")
	createUsers(t, s, []*types.UserWrite{
		{
			User: &types.User{
				Id:          "alice",
				Certificate: aliceCert.Raw,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_Read,
						"db2": types.Privilege_ReadWrite,
					},
				},
			},
		},
	})

	client, err := s.NewRESTClient(nil)
	require.NoError(t, err)

	t.Run("read from db1 would succeed", func(t *testing.T) {
		query := &types.GetDataQuery{
			UserId: "alice",
			DbName: "db1",
			Key:    "key1",
		}

		response, err := client.GetData(
			&types.GetDataQueryEnvelope{
				Payload:   query,
				Signature: testutils.SignatureFromQuery(t, aliceSigner, query),
			},
		)
		require.NoError(t, err)
		require.Nil(t, response.GetResponse().GetValue())
		require.Nil(t, response.GetResponse().GetMetadata())
	})

	t.Run("write to db1 would fail", func(t *testing.T) {
		dataTx := sampleDataTx("alice", "db1")
		_, err := s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_NO_PERMISSION, Reason: none of the user in [alice] has read-write permission on the database [db1]")
	})

	t.Run("read from db2 would succeed", func(t *testing.T) {
		query := &types.GetDataQuery{
			UserId: "alice",
			DbName: "db1",
			Key:    "key1",
		}

		response, err := client.GetData(
			&types.GetDataQueryEnvelope{
				Payload:   query,
				Signature: testutils.SignatureFromQuery(t, aliceSigner, query),
			},
		)
		require.NoError(t, err)
		require.Nil(t, response.GetResponse().GetValue())
		require.Nil(t, response.GetResponse().GetMetadata())
	})

	t.Run("write to db2 would succeed", func(t *testing.T) {
		dataTx := sampleDataTx("alice", "db2")
		receipt, err := s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.NoError(t, err)
		require.NotNil(t, receipt)
	})

	t.Run("read from db3 would fail", func(t *testing.T) {
		query := &types.GetDataQuery{
			UserId: "alice",
			DbName: "db3",
			Key:    "key1",
		}

		_, err := client.GetData(
			&types.GetDataQueryEnvelope{
				Payload:   query,
				Signature: testutils.SignatureFromQuery(t, aliceSigner, query),
			},
		)
		require.EqualError(t, err, "error while processing 'GET /data/db3/a2V5MQ' because the user [alice] has no permission to read from database [db3]")
	})

	t.Run("write to db3 would fail", func(t *testing.T) {
		dataTx := sampleDataTx("alice", "db3")
		_, err := s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_NO_PERMISSION, Reason: none of the user in [alice] has read-write permission on the database [db3]")
	})

	privilege := &types.Privilege{
		DbPermission: map[string]types.Privilege_Access{
			"db1": types.Privilege_ReadWrite,
			"db2": types.Privilege_Read,
			"db3": types.Privilege_ReadWrite,
		},
	}
	updateUser(t, s, "alice", nil, privilege)

	t.Run("write to db1 would succeed", func(t *testing.T) {
		dataTx := sampleDataTx("alice", "db1")
		receipt, err := s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.NoError(t, err)
		require.NotNil(t, receipt)
	})

	t.Run("write to db2 would fail", func(t *testing.T) {
		dataTx := sampleDataTx("alice", "db2")
		_, err := s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_NO_PERMISSION, Reason: none of the user in [alice] has read-write permission on the database [db2]")
	})

	t.Run("write to db3 would succeed", func(t *testing.T) {
		dataTx := sampleDataTx("alice", "db3")
		receipt, err := s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.NoError(t, err)
		require.NotNil(t, receipt)
	})
}

// Scenario:
func TestUserCertificateUpdate(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort + 100,
		BasePeerPort:        pPort + 100,
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

	createDatabases(t, s, []string{"db1"})

	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "alice")
	createUsers(t, s, []*types.UserWrite{
		{
			User: &types.User{
				Id:          "alice",
				Certificate: aliceCert.Raw,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_Read,
					},
				},
			},
		},
	})

	client, err := s.NewRESTClient(nil)
	require.NoError(t, err)

	query := &types.GetDataQuery{
		UserId: "alice",
		DbName: "db1",
		Key:    "key1",
	}
	signature := testutils.SignatureFromQuery(t, aliceSigner, query)

	t.Run("read from db1 would succeed", func(t *testing.T) {
		response, err := client.GetData(
			&types.GetDataQueryEnvelope{
				Payload:   query,
				Signature: signature,
			},
		)
		require.NoError(t, err)
		require.Nil(t, response.GetResponse().GetValue())
		require.Nil(t, response.GetResponse().GetMetadata())
	})

	keyPair, err := c.GetX509KeyPair()
	require.NoError(t, err)
	require.NoError(t, c.CreateUserCerts("alice", keyPair))
	aliceCert, aliceSigner = testutils.LoadTestCrypto(t, c.GetUserCertDir(), "alice")
	updateUser(t, s, "alice", aliceCert.Raw, nil)

	t.Run("read from db1 using old signature would fail", func(t *testing.T) {
		response, err := client.GetData(
			&types.GetDataQueryEnvelope{
				Payload:   query,
				Signature: signature,
			},
		)
		require.EqualError(t, err, "signature verification failed")
		require.Nil(t, response.GetResponse().GetValue())
		require.Nil(t, response.GetResponse().GetMetadata())
	})

	newSignature := testutils.SignatureFromQuery(t, aliceSigner, query)
	t.Run("read from db1 using new signature would succeed", func(t *testing.T) {
		response, err := client.GetData(
			&types.GetDataQueryEnvelope{
				Payload:   query,
				Signature: newSignature,
			},
		)
		require.NoError(t, err)
		require.Nil(t, response.GetResponse().GetValue())
		require.Nil(t, response.GetResponse().GetMetadata())
	})

	deleteUsers(t, s, []*types.UserDelete{{
		UserId: "alice",
	}})
	t.Run("read from db1 fails as the user has been deleted", func(t *testing.T) {
		response, err := client.GetData(
			&types.GetDataQueryEnvelope{
				Payload:   query,
				Signature: newSignature,
			},
		)
		require.EqualError(t, err, "signature verification failed")
		require.Nil(t, response.GetResponse().GetValue())
		require.Nil(t, response.GetResponse().GetMetadata())
	})
}

func createDatabases(t *testing.T, s *setup.Server, dbNames []string) {
	dbAdminTx := &types.DBAdministrationTx{
		UserId:    s.AdminID(),
		TxId:      uuid.New().String(),
		CreateDbs: dbNames,
	}

	receipt, err := s.SubmitTransaction(t, constants.PostDBTx, &types.DBAdministrationTxEnvelope{
		Payload:   dbAdminTx,
		Signature: testutils.SignatureFromTx(t, s.AdminSigner(), dbAdminTx),
	})
	require.NoError(t, err)
	require.NotNil(t, receipt)

	for _, dbName := range dbNames {
		response, err := s.GetDBStatus(t, dbName)
		require.NoError(t, err)
		require.Equal(t, true, response.GetResponse().Exist)
	}
}

func createUsers(t *testing.T, s *setup.Server, users []*types.UserWrite) {
	receipt, err := s.CreateUsers(t, users)
	require.NoError(t, err)
	require.NotNil(t, receipt)

	for _, user := range users {
		respEnv, err := s.QueryUser(t, "admin", user.GetUser().GetId())
		require.NoError(t, err)
		require.Equal(t, user.User, respEnv.GetResponse().User)
	}
}

func deleteUsers(t *testing.T, s *setup.Server, users []*types.UserDelete) {
	userTx := &types.UserAdministrationTx{
		UserId:      "admin",
		TxId:        uuid.New().String(),
		UserDeletes: users,
	}

	receipt, err := s.SubmitTransaction(t, constants.PostUserTx, &types.UserAdministrationTxEnvelope{
		Payload:   userTx,
		Signature: testutils.SignatureFromTx(t, s.AdminSigner(), userTx),
	})
	require.NoError(t, err)
	require.NotNil(t, receipt)

	for _, user := range users {
		respEnv, err := s.QueryUser(t, "admin", user.GetUserId())
		require.NoError(t, err)
		require.Nil(t, respEnv.GetResponse().GetUser())
	}
}

func updateUser(t *testing.T, s *setup.Server, userID string, newCert []byte, newPrivilege *types.Privilege) {
	respEnv, err := s.QueryUser(t, "admin", userID)
	require.NoError(t, err)
	user := respEnv.GetResponse().GetUser()
	require.NotNil(t, user)

	if newCert != nil {
		user.Certificate = newCert
	}

	if newPrivilege != nil {
		user.Privilege = newPrivilege
	}

	userTx := &types.UserAdministrationTx{
		UserId: "admin",
		TxId:   uuid.New().String(),
		UserWrites: []*types.UserWrite{
			{
				User: user,
			},
		},
	}

	receipt, err := s.SubmitTransaction(t, constants.PostUserTx, &types.UserAdministrationTxEnvelope{
		Payload:   userTx,
		Signature: testutils.SignatureFromTx(t, s.AdminSigner(), userTx),
	})
	require.NoError(t, err)
	require.NotNil(t, receipt)

	respEnv, err = s.QueryUser(t, "admin", user.GetId())
	require.NoError(t, err)
	require.Equal(t, user, respEnv.GetResponse().User)
}

func sampleDataTx(userID string, dbName string) *types.DataTx {
	return &types.DataTx{
		MustSignUserIds: []string{userID},
		TxId:            uuid.New().String(),
		DbOperations: []*types.DBOperation{
			{
				DbName: dbName,
				DataWrites: []*types.DataWrite{
					{
						Key:   "key1",
						Value: []byte("value"),
					},
				},
			},
		},
	}
}
