// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package user

import (
	"encoding/json"
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
	baseNodePort = 10123
	basePeerPort = 10134
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
func TestDBManagement(t *testing.T) {
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

	// create database without index
	t.Run("create db1 and db2 without index", func(t *testing.T) {
		createDBs := []string{"db1", "db2"}
		receipt, err := createAndSubmitDBAdminTx(t, s, createDBs, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, receipt)

		aliceCert, _ := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "alice")
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

		for _, dbName := range createDBs {
			response, err := s.GetDBStatus(t, dbName)
			require.NoError(t, err)
			require.True(t, response.GetResponse().Exist)

			index, err := s.GetDBIndex(t, dbName, "alice")
			require.NoError(t, err)
			require.Empty(t, index.GetResponse().GetIndex())
		}
	})

	// update database db1 with an index
	t.Run("update db1 with an index would fail", func(t *testing.T) {
		db1Index := &types.DBIndex{
			AttributeAndType: map[string]types.IndexAttributeType{
				"attr1": types.IndexAttributeType_NUMBER,
				"attr2": types.IndexAttributeType_BOOLEAN,
			},
		}
		_, err := createAndSubmitDBAdminTx(t, s, nil, nil, map[string]*types.DBIndex{
			"db1": db1Index,
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_INCORRECT_ENTRIES, Reason: index update to an existing database is not allowed")
	})

	// create database with index
	t.Run("create db3 with an index", func(t *testing.T) {
		db3Index := &types.DBIndex{
			AttributeAndType: map[string]types.IndexAttributeType{
				"attr1": types.IndexAttributeType_STRING,
				"attr2": types.IndexAttributeType_BOOLEAN,
				"attr3": types.IndexAttributeType_NUMBER,
			},
		}
		receipt, err := createAndSubmitDBAdminTx(t, s, []string{"db3"}, nil, map[string]*types.DBIndex{
			"db3": db3Index,
		})
		require.NoError(t, err)
		require.NotNil(t, receipt)

		response, err := s.GetDBStatus(t, "db3")
		require.NoError(t, err)
		require.True(t, response.GetResponse().Exist)

		bobCert, _ := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "bob")
		createUsers(t, s, []*types.UserWrite{
			{
				User: &types.User{
					Id:          "bob",
					Certificate: bobCert.Raw,
					Privilege: &types.Privilege{
						DbPermission: map[string]types.Privilege_Access{
							"db3": types.Privilege_Read,
						},
					},
				},
			},
		})

		index, err := s.GetDBIndex(t, "db3", "bob")
		require.NoError(t, err)

		expectedIndex, err := json.Marshal(db3Index.AttributeAndType)
		require.NoError(t, err)
		require.Equal(t, string(expectedIndex), index.GetResponse().GetIndex())
	})

	t.Run("delete db1 and db3", func(t *testing.T) {
		// for now, any admin can delete databases which contain states. When we
		// restrict the db delete operation, we need to update this test.
		deleteDBs := []string{"db1", "db3"}
		createAndSubmitDBAdminTx(t, s, nil, deleteDBs, nil)

		for _, dbName := range deleteDBs {
			response, err := s.GetDBStatus(t, dbName)
			require.NoError(t, err)
			require.False(t, response.GetResponse().Exist)
		}
	})
}

func createAndSubmitDBAdminTx(t *testing.T, s *setup.Server, createDBs, deleteDBs []string, dbsIndex map[string]*types.DBIndex) (*types.TxReceipt, error) {
	dbAdminTx := &types.DBAdministrationTx{
		UserId:    s.AdminID(),
		TxId:      uuid.New().String(),
		CreateDbs: createDBs,
		DeleteDbs: deleteDBs,
		DbsIndex:  dbsIndex,
	}

	return s.SubmitTransaction(t, constants.PostDBTx, &types.DBAdministrationTxEnvelope{
		Payload:   dbAdminTx,
		Signature: testutils.SignatureFromTx(t, s.AdminSigner(), dbAdminTx),
	})
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
