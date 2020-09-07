package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
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

		config, err := env.client.GetState(
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
		require.NotNil(t, config)

		configTx, err := prepareConfigTx(env.conf)
		require.NoError(t, err)
		require.Equal(t, configTx.Payload.Writes[0].Value, config.Payload.Value)

		blockStore := env.server.dbServ.blockStore
		height, err := blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(1), height)

		configBlock, err := blockStore.Get(1)
		require.NoError(t, err)
		configTx.Payload.TxID = configBlock.TransactionEnvelopes[0].Payload.TxID
		require.True(t, proto.Equal(configTx, configBlock.TransactionEnvelopes[0]))
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
				expectedError: "X-BLockchain-DB-Signature is not set in the http request header",
			},
			{
				request: &types.GetStatusQueryEnvelope{
					Payload: &types.GetStatusQuery{
						UserID: "",
						DBName: worldstate.DefaultDBName,
					},
					Signature: []byte("signature"),
				},
				expectedError: "X-BLockchain-DB-User-ID is not set in the http request header",
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
				expectedError: "X-BLockchain-DB-Signature is not set in the http request header",
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
				expectedError: "X-BLockchain-DB-User-ID is not set in the http request header",
			},
		}

		for _, testCase := range testCases {
			resp, err := env.client.GetState(context.Background(), testCase.request)
			require.Contains(t, err.Error(), testCase.expectedError)
			require.Nil(t, resp)
		}
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

		expectedConfigValue, err := json.Marshal(expectedClusterConfig)
		require.NoError(t, err)

		expectedConfigTx := &types.TransactionEnvelope{
			Payload: &types.Transaction{
				Type:      1,
				DBName:    "_config",
				DataModel: 0,
				Writes: []*types.KVWrite{
					{
						Key:   "config", // TODO: need to define a constant and put in library package
						Value: expectedConfigValue,
					},
				},
			},
		}

		configTx, err := prepareConfigTx(testConfiguration(t))
		require.NoError(t, err)
		require.NotEmpty(t, configTx.Payload.TxID)
		configTx.Payload.TxID = []byte{}
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
