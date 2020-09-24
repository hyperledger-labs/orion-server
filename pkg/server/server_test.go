package server

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/server/mock"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type serverTestEnv struct {
	server  *DBAndHTTPServer
	client  *mock.Client
	cleanup func(t *testing.T)
	conf    *config.Configurations
}

func newServerTestEnv(t *testing.T) *serverTestEnv {
	conf := testConfiguration(t)
	server, err := New(conf)
	require.NoError(t, err)

	go func() {
		if err := server.Start(); err != nil {
			t.Errorf("error while starting the server, %v", err)
			t.Fail()
		}
	}()

	cleanup := func(t *testing.T) {
		if err := server.Stop(); err != nil {
			t.Errorf("Warning: failed to stop the server: %v\n", err)
		}

		ledgerDir := conf.Node.Database.LedgerDirectory
		if err := os.RemoveAll(ledgerDir); err != nil {
			t.Errorf("Warning: failed to remove %s: %v\n", ledgerDir, err)
		}
	}

	var port string
	isPortAllocated := func() bool {
		_, port, err = net.SplitHostPort(server.listen.Addr().String())
		if err != nil {
			return false
		}
		return port != "0"
	}
	require.Eventually(t, isPortAllocated, 2*time.Second, 100*time.Millisecond)

	url := fmt.Sprintf("http://%s:%s", conf.Node.Network.Address, port)
	client, err := mock.NewRESTClient(url)
	require.NoError(t, err)
	require.NotNil(t, client)

	assertTxInBlockStore := func() bool {
		block, err := server.dbServ.blockStore.Get(1)
		if err != nil {
			return false
		}

		return block.GetConfigTxEnvelope() != nil
	}

	// once the genesis block is committed, we can be sure that the server is
	// ready to accept transactions
	require.Eventually(t, assertTxInBlockStore, 2*time.Second, 200*time.Millisecond)

	return &serverTestEnv{
		server:  server,
		client:  client,
		cleanup: cleanup,
		conf:    conf,
	}
}

func TestStart(t *testing.T) {
	t.Parallel()

	t.Run("server-starts-successfully", func(t *testing.T) {
		t.Parallel()
		env := newServerTestEnv(t)
		defer os.RemoveAll(env.conf.Node.Database.LedgerDirectory)

		valEnv, err := env.client.GetData(
			&types.GetDataQueryEnvelope{
				Payload: &types.GetDataQuery{
					UserID: "admin",
					DBName: "db1",
					Key:    "key1",
				},
				Signature: []byte("hello"),
			},
		)
		require.Nil(t, valEnv)
		require.Contains(t, err.Error(), "the user [admin] has no permission to read from database [db1]")

		configSerialized, err := env.client.GetData(
			&types.GetDataQueryEnvelope{
				Payload: &types.GetDataQuery{
					UserID: "admin",
					DBName: worldstate.ConfigDBName,
					Key:    worldstate.ConfigKey,
				},
				Signature: []byte("hello"),
			},
		)
		require.NoError(t, err)

		config := &types.ClusterConfig{}
		proto.Unmarshal(configSerialized.Payload.Value, config)

		configTx, err := prepareConfigTx(env.conf)
		require.NoError(t, err)
		require.Equal(t, configTx.Payload.NewConfig, config)

		blockStore := env.server.dbServ.blockStore
		height, err := blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(1), height)

		configBlock, err := blockStore.Get(1)
		require.NoError(t, err)
		configTx.Payload.TxID = configBlock.GetConfigTxEnvelope().Payload.GetTxID()
		require.True(t, proto.Equal(configTx, configBlock.GetConfigTxEnvelope()))

		dbAdminTx := &types.DBAdministrationTxEnvelope{
			Payload: &types.DBAdministrationTx{
				UserID:    "admin",
				TxID:      "tx1",
				CreateDBs: []string{"db1", "db2"},
				DeleteDBs: []string{"db3", "db4"},
			},
		}

		resp, err := env.client.SubmitTransaction(constants.PostDBTx, dbAdminTx)
		require.NoError(t, err)
		require.Nil(t, resp)

		assertTxInBlockStore := func() bool {
			block, err := env.server.dbServ.blockStore.Get(2)
			if err != nil {
				return false
			}

			return proto.Equal(dbAdminTx, block.GetDBAdministrationTxEnvelope())
		}

		require.Eventually(t, assertTxInBlockStore, 2*time.Second, 200*time.Millisecond)

		// restart the server
		require.NoError(t, env.server.listen.Close())
		require.NoError(t, env.server.Start())
		defer env.server.Stop()

		blkHeight, err := blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(2), blkHeight)

		dbAdminTx = &types.DBAdministrationTxEnvelope{
			Payload: &types.DBAdministrationTx{
				UserID:    "admin",
				TxID:      "tx1",
				DeleteDBs: []string{"db3", "db4"},
			},
		}

		resp, err = env.client.SubmitTransaction(constants.PostDBTx, dbAdminTx)
		require.NoError(t, err)
		require.Nil(t, resp)

		assertTxInBlockStore = func() bool {
			block, err := env.server.dbServ.blockStore.Get(3)
			if err != nil {
				return false
			}

			return proto.Equal(dbAdminTx, block.GetDBAdministrationTxEnvelope())
		}
		require.Eventually(t, assertTxInBlockStore, 2*time.Second, 200*time.Millisecond)
	})
}

func TestHandleDBStatusQuery(t *testing.T) {
	t.Parallel()

	setup := func(db worldstate.DB, userID string) {
		user := &types.User{
			ID: userID,
		}

		u, err := proto.Marshal(user)
		require.NoError(t, err)

		createUser := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + userID,
						Value: u,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    1,
							},
						},
					},
				},
			},
		}
		require.NoError(t, db.Commit(createUser))
	}

	t.Run("GetDBStatus-Returns-True", func(t *testing.T) {
		t.Parallel()
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		setup(env.server.dbServ.db, "testUser")

		req := &types.GetDBStatusQueryEnvelope{
			Payload: &types.GetDBStatusQuery{
				UserID: "testUser",
				DBName: worldstate.DefaultDBName,
			},
			Signature: []byte("signature"),
		}
		resp, err := env.client.GetDBStatus(req)
		require.NoError(t, err)
		require.True(t, resp.Payload.Exist)
	})

	t.Run("GetDBStatus-Returns-Error", func(t *testing.T) {
		t.Parallel()
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		setup(env.server.dbServ.db, "testUser")

		testCases := []struct {
			name          string
			request       *types.GetDBStatusQueryEnvelope
			expectedError string
		}{
			{
				name: "signature is missing",
				request: &types.GetDBStatusQueryEnvelope{
					Payload: &types.GetDBStatusQuery{
						UserID: "testUser",
						DBName: worldstate.DefaultDBName,
					},
				},
				expectedError: "Signature is not set in the http request header",
			},
			{
				name: "userID is missing",
				request: &types.GetDBStatusQueryEnvelope{
					Payload: &types.GetDBStatusQuery{
						UserID: "",
						DBName: worldstate.DefaultDBName,
					},
					Signature: []byte("signature"),
				},
				expectedError: "UserID is not set in the http request header",
			},
		}

		for _, testCase := range testCases {
			resp, err := env.client.GetDBStatus(testCase.request)
			require.Contains(t, err.Error(), testCase.expectedError)
			require.Nil(t, resp)
		}
	})
}

func TestHandleDataQuery(t *testing.T) {
	t.Parallel()

	setup := func(env *serverTestEnv, userID, dbName string) {
		user := &types.User{
			ID: userID,
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					dbName: types.Privilege_ReadWrite,
				},
			},
		}

		u, err := proto.Marshal(user)
		require.NoError(t, err)

		createUser := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + userID,
						Value: u,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    1,
							},
						},
					},
				},
			},
		}
		require.NoError(t, env.server.dbServ.db.Commit(createUser))
	}

	t.Run("GetData-Returns-Data", func(t *testing.T) {
		t.Parallel()
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		setup(env, "testUser", worldstate.DefaultDBName)

		val := []byte("Value1")
		metadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
		}
		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: worldstate.DefaultDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      "key1",
						Value:    val,
						Metadata: metadata,
					},
				},
			},
		}
		require.NoError(t, env.server.dbServ.db.Commit(dbsUpdates))

		testCases := []struct {
			key              string
			expectedValue    []byte
			expectedMetadata *types.Metadata
		}{
			{
				key:              "key1",
				expectedValue:    val,
				expectedMetadata: metadata,
			},
			{
				key:           "key2",
				expectedValue: nil,
			},
		}

		for _, testCase := range testCases {
			req := &types.GetDataQueryEnvelope{
				Payload: &types.GetDataQuery{
					UserID: "testUser",
					DBName: worldstate.DefaultDBName,
					Key:    testCase.key,
				},
				Signature: []byte("signature"),
			}
			resp, err := env.client.GetData(req)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedValue, resp.Payload.Value)
			require.True(t, proto.Equal(testCase.expectedMetadata, resp.Payload.Metadata))
		}
	})

	t.Run("GetData-Returns-Error", func(t *testing.T) {
		t.Parallel()
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		testCases := []struct {
			request       *types.GetDataQueryEnvelope
			expectedError string
		}{
			{
				request: &types.GetDataQueryEnvelope{
					Payload: &types.GetDataQuery{
						UserID: "testUser",
						DBName: worldstate.DefaultDBName,
						Key:    "key1",
					},
				},
				expectedError: "Signature is not set in the http request header",
			},
			{
				request: &types.GetDataQueryEnvelope{
					Payload: &types.GetDataQuery{
						UserID: "",
						DBName: worldstate.DefaultDBName,
						Key:    "key1",
					},
					Signature: []byte("signature"),
				},
				expectedError: "UserID is not set in the http request header",
			},
		}

		for _, testCase := range testCases {
			resp, err := env.client.GetData(testCase.request)
			require.Contains(t, err.Error(), testCase.expectedError)
			require.Nil(t, resp)
		}
	})
}

func TestHandleUserQuery(t *testing.T) {
	t.Parallel()

	metadata := &types.Metadata{
		Version: &types.Version{
			BlockNum: 2,
			TxNum:    1,
		},
	}

	targetUser := &types.User{
		ID:          "targetUser",
		Certificate: []byte("cert"),
	}
	targetUserSerialized, err := proto.Marshal(targetUser)
	require.NoError(t, err)

	commonSetup := func(db worldstate.DB) {
		querierUser := &types.User{
			ID:          "querierUser",
			Certificate: []byte("cert"),
		}
		querierUserSerialized, err := proto.Marshal(querierUser)
		require.NoError(t, err)

		dbUpdates := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      string(identity.UserNamespace) + "querierUser",
						Value:    querierUserSerialized,
						Metadata: metadata,
					},
				},
			},
		}

		require.NoError(t, db.Commit(dbUpdates))
	}

	t.Run("getUser returns user", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name            string
			querierUserID   string
			targetUserID    string
			setup           func(db worldstate.DB)
			expectedRespose *types.GetUserResponseEnvelope
		}{
			{
				name:          "targetUser does not exist",
				querierUserID: "querierUser",
				targetUserID:  "targetUser",
				setup: func(db worldstate.DB) {
					commonSetup(db)
				},
				expectedRespose: &types.GetUserResponseEnvelope{
					Payload: &types.GetUserResponse{
						Header: &types.ResponseHeader{
							NodeID: "bdb-node-1",
						},
						User:     nil,
						Metadata: nil,
					},
				},
			},
			{
				name:          "targetUser",
				querierUserID: "querierUser",
				targetUserID:  "targetUser",
				setup: func(db worldstate.DB) {
					commonSetup(db)
					dbsUpdates := []*worldstate.DBUpdates{
						{
							DBName: worldstate.UsersDBName,
							Writes: []*worldstate.KVWithMetadata{
								{
									Key:      string(identity.UserNamespace) + "targetUser",
									Value:    targetUserSerialized,
									Metadata: metadata,
								},
							},
						},
					}
					require.NoError(t, db.Commit(dbsUpdates))
				},
				expectedRespose: &types.GetUserResponseEnvelope{
					Payload: &types.GetUserResponse{
						Header: &types.ResponseHeader{
							NodeID: "bdb-node-1",
						},
						User:     targetUser,
						Metadata: metadata,
					},
				},
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				env := newServerTestEnv(t)
				defer env.cleanup(t)

				tt.setup(env.server.dbServ.db)

				req := &types.GetUserQueryEnvelope{
					Payload: &types.GetUserQuery{
						UserID:       "querierUser",
						TargetUserID: "targetUser",
					},
					Signature: []byte("signature"),
				}
				resp, err := env.client.GetUser(req)
				require.NoError(t, err)
				require.True(t, proto.Equal(tt.expectedRespose, resp))
			})
		}
	})

	t.Run("getUser returns error", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name          string
			querierUserID string
			targetUserID  string
			setup         func(db worldstate.DB)
			expectedError string
		}{
			{
				name:          "targetUser does not exist",
				querierUserID: "querierUser",
				targetUserID:  "targetUser",
				setup:         func(db worldstate.DB) {},
				expectedError: "/user/targetUser query is rejected as the submitting user [querierUser] does not exist in the cluster",
			},
			{
				name:          "targetUser",
				querierUserID: "querierUser",
				targetUserID:  "targetUser",
				setup: func(db worldstate.DB) {
					commonSetup(db)
					dbsUpdates := []*worldstate.DBUpdates{
						{
							DBName: worldstate.UsersDBName,
							Writes: []*worldstate.KVWithMetadata{
								{
									Key:   string(identity.UserNamespace) + "targetUser",
									Value: targetUserSerialized,
									Metadata: &types.Metadata{
										Version: metadata.Version,
										AccessControl: &types.AccessControl{
											ReadUsers: map[string]bool{
												"user1": true,
											},
										},
									},
								},
							},
						},
					}
					require.NoError(t, db.Commit(dbsUpdates))
				},
				expectedError: "error while processing [/user/targetUser] because the user [querierUser] has no permission to read info of user [targetUser]",
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				env := newServerTestEnv(t)
				defer env.cleanup(t)

				tt.setup(env.server.dbServ.db)

				req := &types.GetUserQueryEnvelope{
					Payload: &types.GetUserQuery{
						UserID:       "querierUser",
						TargetUserID: "targetUser",
					},
					Signature: []byte("signature"),
				}
				resp, err := env.client.GetUser(req)
				require.EqualError(t, err, tt.expectedError)
				require.Nil(t, resp)
			})
		}
	})
}

func TestHandleConfigQuery(t *testing.T) {
	t.Parallel()

	commonSetup := func(db worldstate.DB) {
		querierUser := &types.User{
			ID:          "querierUser",
			Certificate: []byte("cert"),
		}
		querierUserSerialized, err := proto.Marshal(querierUser)
		require.NoError(t, err)

		dbUpdates := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + "querierUser",
						Value: querierUserSerialized,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    5,
							},
						},
					},
				},
			},
		}

		require.NoError(t, db.Commit(dbUpdates))
	}

	t.Run("getConfig returns config", func(t *testing.T) {
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		commonSetup(env.server.dbServ.db)

		conf := testConfiguration(t)
		confEnvp, err := prepareConfigTx(conf)
		require.NoError(t, err)
		expectedConfig := confEnvp.Payload.NewConfig

		e := &types.GetConfigQueryEnvelope{
			Payload: &types.GetConfigQuery{
				UserID: "querierUser",
			},
			Signature: []byte("signature"),
		}

		confResp, err := env.client.GetConfig(e)
		require.NoError(t, err)
		require.Equal(t, expectedConfig, confResp.Payload.Config)
	})

	t.Run("getConfig returns error", func(t *testing.T) {
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		e := &types.GetConfigQueryEnvelope{
			Payload: &types.GetConfigQuery{
				UserID: "querierUser",
			},
			Signature: []byte("signature"),
		}

		confResp, err := env.client.GetConfig(e)
		require.EqualError(t, err, "/config query is rejected as the submitting user [querierUser] does not exist in the cluster")
		require.Nil(t, confResp)
	})
}

func TestHandleTransaction(t *testing.T) {
	t.Parallel()

	t.Run("submit a data transaction", func(t *testing.T) {
		t.Parallel()

		env := newServerTestEnv(t)
		defer env.cleanup(t)

		dataTx := &types.DataTxEnvelope{
			Payload: &types.DataTx{
				UserID: "admin",
				TxID:   "tx1",
				DBName: "db1",
				DataReads: []*types.DataRead{
					{
						Key: "key1",
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    2,
						},
					},
				},
				DataWrites: []*types.DataWrite{
					{
						Key:   "key2",
						Value: []byte("value2"),
						ACL: &types.AccessControl{
							ReadUsers: map[string]bool{
								"user1": true,
							},
							ReadWriteUsers: map[string]bool{
								"user2": true,
							},
						},
					},
				},
				DataDeletes: []*types.DataDelete{
					{
						Key: "key1",
					},
				},
			},
		}

		resp, err := env.client.SubmitTransaction(constants.PostDataTx, dataTx)
		require.NoError(t, err)
		require.Nil(t, resp)

		assertTxInBlockStore := func() bool {
			block, err := env.server.dbServ.blockStore.Get(2)
			if err != nil {
				return false
			}

			if proto.Equal(dataTx, block.GetDataTxEnvelopes().GetEnvelopes()[0]) {
				return true
			}
			return false
		}

		require.Eventually(t, assertTxInBlockStore, 2*time.Second, 200*time.Millisecond)
	})

	t.Run("submit a user admin transaction", func(t *testing.T) {
		t.Parallel()

		env := newServerTestEnv(t)
		defer env.cleanup(t)

		userAdminTx := &types.UserAdministrationTxEnvelope{
			Payload: &types.UserAdministrationTx{
				UserID: "admin",
				TxID:   "tx1",
				UserReads: []*types.UserRead{
					{
						UserID: "user1",
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    2,
						},
					},
				},
				UserWrites: []*types.UserWrite{
					{
						User: &types.User{
							ID:          "user2",
							Certificate: []byte("certificate"),
							Privilege: &types.Privilege{
								DBPermission: map[string]types.Privilege_Access{
									"db1": types.Privilege_Read,
									"db2": types.Privilege_ReadWrite,
								},
								DBAdministration:      true,
								ClusterAdministration: true,
								UserAdministration:    true,
							},
						},
						ACL: &types.AccessControl{
							ReadUsers: map[string]bool{
								"user3": true,
							},
							ReadWriteUsers: map[string]bool{
								"user4": true,
							},
						},
					},
				},
				UserDeletes: []*types.UserDelete{
					{
						UserID: "user5",
					},
				},
			},
		}

		resp, err := env.client.SubmitTransaction(constants.PostUserTx, userAdminTx)
		require.NoError(t, err)
		require.Nil(t, resp)

		assertTxInBlockStore := func() bool {
			block, err := env.server.dbServ.blockStore.Get(2)
			if err != nil {
				return false
			}

			return proto.Equal(userAdminTx, block.GetUserAdministrationTxEnvelope())
		}

		require.Eventually(t, assertTxInBlockStore, 2*time.Second, 200*time.Millisecond)
	})

	t.Run("submit a db admin transaction", func(t *testing.T) {
		t.Parallel()

		env := newServerTestEnv(t)
		defer env.cleanup(t)

		dbAdminTx := &types.DBAdministrationTxEnvelope{
			Payload: &types.DBAdministrationTx{
				UserID:    "admin",
				TxID:      "tx1",
				CreateDBs: []string{"db1", "db2"},
				DeleteDBs: []string{"db3", "db4"},
			},
		}

		resp, err := env.client.SubmitTransaction(constants.PostDBTx, dbAdminTx)
		require.NoError(t, err)
		require.Nil(t, resp)

		assertTxInBlockStore := func() bool {
			block, err := env.server.dbServ.blockStore.Get(2)
			if err != nil {
				return false
			}

			return proto.Equal(dbAdminTx, block.GetDBAdministrationTxEnvelope())
		}

		require.Eventually(t, assertTxInBlockStore, 2*time.Second, 200*time.Millisecond)
	})

	t.Run("submit a cluster config transaction", func(t *testing.T) {
		t.Parallel()

		env := newServerTestEnv(t)
		defer env.cleanup(t)

		configTx := &types.ConfigTxEnvelope{
			Payload: &types.ConfigTx{
				UserID: "admin",
				TxID:   "tx1",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
				NewConfig: &types.ClusterConfig{
					Nodes: []*types.NodeConfig{
						{
							ID:          "bdb-node-1",
							Certificate: []byte("node-cert"),
							Address:     "127.0.0.1",
							Port:        5000,
						},
					},
					Admins: []*types.Admin{
						{
							ID:          "admin",
							Certificate: []byte("admin-cert"),
						},
					},
					RootCACertificate: []byte("root-cert"),
				},
			},
		}

		resp, err := env.client.SubmitTransaction(constants.PostConfigTx, configTx)
		require.NoError(t, err)
		require.Nil(t, resp)

		assertTxInBlockStore := func() bool {
			block, err := env.server.dbServ.blockStore.Get(2)
			if err != nil {
				return false
			}

			return proto.Equal(configTx, block.GetConfigTxEnvelope())
		}

		require.Eventually(t, assertTxInBlockStore, 2*time.Second, 200*time.Millisecond)
	})

	t.Run("error while submitting a wrong data tx", func(t *testing.T) {
		t.Parallel()

		env := newServerTestEnv(t)
		defer env.cleanup(t)

		configTx := &types.ConfigTxEnvelope{
			Payload: &types.ConfigTx{
				UserID: "admin",
				TxID:   "tx1",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
			},
		}

		resp, err := env.client.SubmitTransaction(constants.PostDataTx, configTx)
		require.Contains(t, err.Error(), "json: unknown field \"read_old_config_version\"")
		require.Nil(t, resp)
	})

	t.Run("error while submitting a wrong config tx", func(t *testing.T) {
		t.Parallel()

		env := newServerTestEnv(t)
		defer env.cleanup(t)

		dataTx := &types.DataTxEnvelope{
			Payload: &types.DataTx{
				UserID: "admin",
				TxID:   "tx1",
				DataDeletes: []*types.DataDelete{
					{
						Key: "key1",
					},
				},
			},
		}

		resp, err := env.client.SubmitTransaction(constants.PostConfigTx, dataTx)
		require.Contains(t, err.Error(), "json: unknown field \"data_deletes\"")
		require.Nil(t, resp)
	})

	t.Run("error while submitting a wrong db admin tx", func(t *testing.T) {
		t.Parallel()

		env := newServerTestEnv(t)
		defer env.cleanup(t)

		dataTx := &types.DataTxEnvelope{
			Payload: &types.DataTx{
				UserID: "admin",
				TxID:   "tx1",
				DataDeletes: []*types.DataDelete{
					{
						Key: "key1",
					},
				},
			},
		}

		resp, err := env.client.SubmitTransaction(constants.PostDBTx, dataTx)
		require.Contains(t, err.Error(), "json: unknown field \"data_deletes\"")
		require.Nil(t, resp)
	})

	t.Run("error while submitting a wrong user admin tx", func(t *testing.T) {
		t.Parallel()

		env := newServerTestEnv(t)
		defer env.cleanup(t)

		dataTx := &types.DataTxEnvelope{
			Payload: &types.DataTx{
				UserID: "admin",
				TxID:   "tx1",
				DataDeletes: []*types.DataDelete{
					{
						Key: "key1",
					},
				},
			},
		}

		resp, err := env.client.SubmitTransaction(constants.PostUserTx, dataTx)
		require.Contains(t, err.Error(), "json: unknown field \"data_deletes\"")
		require.Nil(t, resp)
	})

	t.Run("error as the submitting user does not exist", func(t *testing.T) {
		t.Parallel()

		env := newServerTestEnv(t)
		defer env.cleanup(t)

		dataTx := &types.DataTxEnvelope{
			Payload: &types.DataTx{
				UserID: "querierUser",
				TxID:   "tx1",
				DataDeletes: []*types.DataDelete{
					{
						Key: "key1",
					},
				},
			},
		}

		resp, err := env.client.SubmitTransaction(constants.PostDataTx, dataTx)
		require.EqualError(t, err, "transaction is rejected as the submitting user [querierUser] does not exist in the cluster")
		require.Nil(t, resp)
	})
}

func TestPrepareConfigTransaction(t *testing.T) {
	t.Parallel()

	t.Run("successfully-returns", func(t *testing.T) {
		t.Parallel()
		nodeCert, err := ioutil.ReadFile("./testdata/node.cert")
		require.NoError(t, err)

		adminCert, err := ioutil.ReadFile("./testdata/admin.cert")
		require.NoError(t, err)

		rootCACert, err := ioutil.ReadFile("./testdata/rootca.cert")
		require.NoError(t, err)

		expectedClusterConfig := &types.ClusterConfig{
			Nodes: []*types.NodeConfig{
				{
					ID:          "bdb-node-1",
					Certificate: nodeCert,
					Address:     "127.0.0.1",
					Port:        0,
				},
			},
			Admins: []*types.Admin{
				{
					ID:          "admin",
					Certificate: adminCert,
				},
			},
			RootCACertificate: rootCACert,
		}

		expectedConfigTx := &types.ConfigTxEnvelope{
			Payload: &types.ConfigTx{
				NewConfig: expectedClusterConfig,
			},
		}

		conf := testConfiguration(t)
		defer os.RemoveAll(conf.Node.Database.LedgerDirectory)
		configTx, err := prepareConfigTx(conf)
		require.NoError(t, err)
		require.NotEmpty(t, configTx.Payload.TxID)
		configTx.Payload.TxID = ""
		require.True(t, proto.Equal(expectedConfigTx, configTx))
	})
}

func testConfiguration(t *testing.T) *config.Configurations {
	ledgerDir, err := ioutil.TempDir("/tmp", "server")
	require.NoError(t, err)

	return &config.Configurations{
		Node: config.NodeConf{
			Identity: config.IdentityConf{
				ID:              "bdb-node-1",
				CertificatePath: "./testdata/node.cert",
				KeyPath:         "./testdata/node.key",
			},
			Network: config.NetworkConf{
				Address: "127.0.0.1",
				Port:    0,
			},
			Database: config.DatabaseConf{
				Name:            "leveldb",
				LedgerDirectory: ledgerDir,
			},
			QueueLength: config.QueueLengthConf{
				Transaction:               1000,
				ReorderedTransactionBatch: 100,
				Block:                     100,
			},
			LogLevel: "debug",
		},
		Consensus: config.ConsensusConf{
			Algorithm:                   "raft",
			MaxBlockSize:                2,
			MaxTransactionCountPerBlock: 1,
			BlockTimeout:                50 * time.Millisecond,
		},
		Admin: config.AdminConf{
			ID:              "admin",
			CertificatePath: "./testdata/admin.cert",
		},
		RootCA: config.RootCAConf{
			CertificatePath: "./testdata/rootca.cert",
		},
	}
}
