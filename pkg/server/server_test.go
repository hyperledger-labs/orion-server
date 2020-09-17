package server

import (
	"context"
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
		defer env.cleanup(t)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		valEnv, err := env.client.GetState(
			ctx,
			&types.GetStateQueryEnvelope{
				Payload: &types.GetStateQuery{
					UserID: "admin",
					DBName: "db1",
					Key:    "key1",
				},
				Signature: []byte("hello"),
			},
		)
		require.Nil(t, valEnv)
		require.Contains(t, err.Error(), "the user [admin] has no permission to read from database [db1]")

		configSerialized, err := env.client.GetState(
			ctx,
			&types.GetStateQueryEnvelope{
				Payload: &types.GetStateQuery{
					UserID: "admin",
					DBName: "_config",
					Key:    "config",
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
	})
}

func TestHandleStatusQuery(t *testing.T) {
	t.Parallel()

	t.Run("GetStatus-Returns-True", func(t *testing.T) {
		t.Parallel()
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		req := &types.GetStatusQueryEnvelope{
			Payload: &types.GetStatusQuery{
				UserID: "testUser",
				DBName: worldstate.DefaultDBName,
			},
			Signature: []byte("signature"),
		}
		resp, err := env.client.GetStatus(context.Background(), req)
		require.NoError(t, err)
		require.True(t, resp.Payload.Exist)
	})

	t.Run("GetStatus-Returns-Error", func(t *testing.T) {
		t.Parallel()
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		testCases := []struct {
			request       *types.GetStatusQueryEnvelope
			expectedError string
		}{
			{
				request: &types.GetStatusQueryEnvelope{
					Payload: &types.GetStatusQuery{
						UserID: "testUser",
						DBName: worldstate.DefaultDBName,
					},
				},
				expectedError: "Signature is not set in the http request header",
			},
			{
				request: &types.GetStatusQueryEnvelope{
					Payload: &types.GetStatusQuery{
						UserID: "",
						DBName: worldstate.DefaultDBName,
					},
					Signature: []byte("signature"),
				},
				expectedError: "UserID is not set in the http request header",
			},
		}

		for _, testCase := range testCases {
			resp, err := env.client.GetStatus(context.Background(), testCase.request)
			require.Contains(t, err.Error(), testCase.expectedError)
			require.Nil(t, resp)
		}
	})
}

func TestHandleStateQuery(t *testing.T) {
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

	t.Run("GetState-Returns-State", func(t *testing.T) {
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
			req := &types.GetStateQueryEnvelope{
				Payload: &types.GetStateQuery{
					UserID: "testUser",
					DBName: worldstate.DefaultDBName,
					Key:    testCase.key,
				},
				Signature: []byte("signature"),
			}
			resp, err := env.client.GetState(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedValue, resp.Payload.Value)
			require.True(t, proto.Equal(testCase.expectedMetadata, resp.Payload.Metadata))
		}
	})

	t.Run("GetState-Returns-Error", func(t *testing.T) {
		t.Parallel()
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		testCases := []struct {
			request       *types.GetStateQueryEnvelope
			expectedError string
		}{
			{
				request: &types.GetStateQueryEnvelope{
					Payload: &types.GetStateQuery{
						UserID: "testUser",
						DBName: worldstate.DefaultDBName,
						Key:    "key1",
					},
				},
				expectedError: "Signature is not set in the http request header",
			},
			{
				request: &types.GetStateQueryEnvelope{
					Payload: &types.GetStateQuery{
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
			resp, err := env.client.GetState(context.Background(), testCase.request)
			require.Contains(t, err.Error(), testCase.expectedError)
			require.Nil(t, resp)
		}
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
				UserID: "user1",
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

		resp, err := env.client.SubmitTransaction(context.Background(), constants.PostDataTx, dataTx)
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

		resp, err := env.client.SubmitTransaction(context.Background(), constants.PostUserTx, userAdminTx)
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

		resp, err := env.client.SubmitTransaction(context.Background(), constants.PostDBTx, dbAdminTx)
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

		resp, err := env.client.SubmitTransaction(context.Background(), constants.PostConfigTx, configTx)
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

		resp, err := env.client.SubmitTransaction(context.Background(), constants.PostDataTx, configTx)
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

		resp, err := env.client.SubmitTransaction(context.Background(), constants.PostConfigTx, dataTx)
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

		resp, err := env.client.SubmitTransaction(context.Background(), constants.PostDBTx, dataTx)
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

		resp, err := env.client.SubmitTransaction(context.Background(), constants.PostUserTx, dataTx)
		require.Contains(t, err.Error(), "json: unknown field \"data_deletes\"")
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
