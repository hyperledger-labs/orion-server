// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package server

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/config"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/certificateauthority"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/crypto"
	"github.com/IBM-Blockchain/bcdb-server/pkg/cryptoservice"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server/mock"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server/testutils"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var basePort uint32 = 6090
var basePortMutex sync.Mutex

type serverTestEnv struct {
	serverConfig   *config.Configurations
	bcdbHTTPServer *BCDBHTTPServer
	caKeys         tls.Certificate
	testDataPath   string
	adminSigner    crypto.Signer
	client         *mock.Client
	certCol        *certificateauthority.CACertCollection
}

func (env *serverTestEnv) restart(t *testing.T) {
	t.Log("Stopping server")
	err := env.bcdbHTTPServer.Stop()
	require.NoError(t, err)

	localConfigOnly := &config.Configurations{
		LocalConfig:  env.serverConfig.LocalConfig,
		SharedConfig: nil,
	}

	env.bcdbHTTPServer, err = New(localConfigOnly)
	require.NoError(t, err)

	err = env.bcdbHTTPServer.Start()
	require.NoError(t, err)

	isLeaderCond := func () bool {
		return env.bcdbHTTPServer.IsLeader() == nil
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	port, err := env.bcdbHTTPServer.Port()
	require.NoError(t, err)
	client, err := mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port))
	require.NoError(t, err)

	env.client = client
}

func (env *serverTestEnv) getNodeSigVerifier(t *testing.T) (*crypto.Verifier, error) {
	configQuery := &types.GetNodeConfigQuery{
		NodeId: env.bcdbHTTPServer.conf.LocalConfig.Server.Identity.ID,
		UserId: "admin",
	}

	cfg, err := env.client.GetNodeConfig(&types.GetNodeConfigQueryEnvelope{
		Payload:   configQuery,
		Signature: testutils.SignatureFromQuery(t, env.adminSigner, configQuery),
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.NotNil(t, cfg.Response)

	err = env.certCol.VerifyLeafCert(cfg.GetResponse().GetNodeConfig().GetCertificate())
	require.NoError(t, err)

	return crypto.NewVerifier(cfg.GetResponse().GetNodeConfig().GetCertificate())
}

func newServerTestEnv(t *testing.T) *serverTestEnv {
	tempDir, err := ioutil.TempDir("/tmp", "serverTest")
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Errorf("error while removing test directory: %v", err)
		}
	})

	rootCAPemCert, caPrivKey, err := testutils.GenerateRootCA("BCDB RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, rootCAPemCert)
	require.NotNil(t, caPrivKey)

	keyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
	require.NoError(t, err)
	require.NotNil(t, keyPair)

	block, _ := pem.Decode(rootCAPemCert)
	certsCollection, err := certificateauthority.NewCACertCollection([][]byte{block.Bytes}, nil)
	require.NoError(t, err)

	err = certsCollection.VerifyCollection()
	require.NoError(t, err)

	serverRootCACertFile, err := os.Create(path.Join(tempDir, "serverRootCACert.pem"))
	require.NoError(t, err)
	_, err = serverRootCACertFile.Write(rootCAPemCert)
	require.NoError(t, err)
	require.NoError(t, serverRootCACertFile.Close())

	pemCert, privKey, err := testutils.IssueCertificate("BCDB Instance", "127.0.0.1", keyPair)
	require.NoError(t, err)

	pemCertFile, err := os.Create(path.Join(tempDir, "server.pem"))
	require.NoError(t, err)
	_, err = pemCertFile.Write(pemCert)
	require.NoError(t, err)
	require.NoError(t, pemCertFile.Close())

	pemPrivKeyFile, err := os.Create(path.Join(tempDir, "server.key"))
	require.NoError(t, err)
	_, err = pemPrivKeyFile.Write(privKey)
	require.NoError(t, err)
	require.NoError(t, pemPrivKeyFile.Close())

	pemAdminCert, pemAdminKey, err := testutils.IssueCertificate("BCDB Admin", "127.0.0.1", keyPair)
	require.NoError(t, err)
	pemAdminCertFile, err := os.Create(path.Join(tempDir, "admin.pem"))
	require.NoError(t, err)
	_, err = pemAdminCertFile.Write(pemAdminCert)
	require.NoError(t, err)
	require.NoError(t, pemAdminCertFile.Close())

	pemAdminKeyFile, err := os.Create(path.Join(tempDir, "admin.key"))
	require.NoError(t, err)
	_, err = pemAdminKeyFile.Write(pemAdminKey)
	require.NoError(t, err)
	require.NoError(t, pemAdminKeyFile.Close())

	adminSigner, err := crypto.NewSigner(
		&crypto.SignerOptions{
			Identity:    "admin",
			KeyFilePath: path.Join(tempDir, "admin.key"),
		},
	)
	require.NoError(t, err)

	basePortMutex.Lock()
	nodePort := basePort
	peerPort := basePort + 10000
	basePort += 1
	basePortMutex.Unlock()

	nodeID := "testNode" + uuid.New().String()
	serverConfig := &config.Configurations{
		LocalConfig: &config.LocalConfiguration{
			Server: config.ServerConf{
				Identity: config.IdentityConf{
					ID:              nodeID,
					CertificatePath: path.Join(tempDir, "server.pem"),
					KeyPath:         path.Join(tempDir, "server.key"),
				},
				Database: config.DatabaseConf{
					Name:            "leveldb",
					LedgerDirectory: path.Join(tempDir, "ledger"),
				},
				Network: config.NetworkConf{
					Address: "127.0.0.1",
					Port:    nodePort,
				},
				QueueLength: config.QueueLengthConf{
					Block:                     1,
					Transaction:               1,
					ReorderedTransactionBatch: 1,
				},

				LogLevel: "debug",
			},
			BlockCreation: config.BlockCreationConf{
				BlockTimeout:                500 * time.Millisecond,
				MaxBlockSize:                1,
				MaxTransactionCountPerBlock: 1,
			},
			Replication: config.ReplicationConf{
				WALDir:  path.Join(tempDir, "raft", "wal"),
				SnapDir: path.Join(tempDir, "raft", "snap"),
				Network: config.NetworkConf{Address: "127.0.0.1", Port: peerPort},
				TLS:     config.TLSConf{Enabled: false},
			},
		},
		SharedConfig: &config.SharedConfiguration{
			Nodes: []config.NodeConf{
				{
					NodeID:          nodeID,
					Host:            "127.0.0.1",
					Port:            nodePort,
					CertificatePath: path.Join(tempDir, "server.pem"),
				},
			},
			Admin: config.AdminConf{
				ID:              "admin",
				CertificatePath: path.Join(tempDir, "admin.pem"),
			},
			CAConfig: config.CAConfiguration{
				RootCACertsPath: []string{path.Join(tempDir, "serverRootCACert.pem")},
			},
			Consensus: &config.ConsensusConf{
				Algorithm: "raft",
				Members: []*config.PeerConf{
					{
						NodeId:   nodeID,
						RaftId:   1,
						PeerHost: "127.0.0.1",
						PeerPort: peerPort,
					},
				},
				RaftConfig: &config.RaftConf{
					TickInterval:         "100ms",
					ElectionTicks:        10,
					HeartbeatTicks:       1,
					MaxInflightBlocks:    50,
					SnapshotIntervalSize: math.MaxUint64,
				},
			},
		},
	}
	server, err := New(serverConfig)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)

	isLeaderCond := func () bool {
		return server.IsLeader() == nil
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	env := &serverTestEnv{
		serverConfig:   serverConfig,
		bcdbHTTPServer: server,
		caKeys:         keyPair,
		testDataPath:   tempDir,
		adminSigner:    adminSigner,
		certCol:        certsCollection,
	}

	port, err := server.Port()
	require.NoError(t, err)
	client, err := mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port))
	require.NoError(t, err)
	env.client = client

	return env
}

func (env *serverTestEnv) cleanup(t *testing.T) {
	nBefore := runtime.NumGoroutine()
	t.Logf("Stopping server, test name: %s, #goroutines: %d", t.Name(), nBefore)
	err := env.bcdbHTTPServer.Stop()
	nAfter := runtime.NumGoroutine()
	t.Logf("Stopped server, test name: %s, #goroutines: %d", t.Name(), nAfter)
	if nAfter > 2 {
		buf := make([]byte, 10000000)
		n := runtime.Stack(buf, true)
		t.Logf("Stack after stopping server, test name: %s\n%s", t.Name(), buf[:n])
	}
	require.NoError(t, err)
}

func TestServerWithDataRequestAndProvenanceQueries(t *testing.T) {
	// Scenario: we instantiate a server, trying to query for key,
	// making sure key does not exist and then posting it into DB
	env := newServerTestEnv(t)
	defer env.cleanup(t)

	verifier, err := env.getNodeSigVerifier(t)
	require.NoError(t, err)

	dataQuery := &types.GetDataQuery{
		DbName: worldstate.DefaultDBName,
		UserId: "admin",
		Key:    "foo",
	}
	data, err := env.client.GetData(
		&types.GetDataQueryEnvelope{
			Payload:   dataQuery,
			Signature: testutils.SignatureFromQuery(t, env.adminSigner, dataQuery),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, data)
	require.NotNil(t, data.Response)

	resp, err := json.Marshal(data.GetResponse())
	require.NoError(t, err)
	err = verifier.Verify(resp, data.GetSignature())
	require.NoError(t, err)

	require.Nil(t, data.GetResponse().GetValue())

	dataTx := &types.DataTx{
		MustSignUserIds: []string{"admin"},
		TxId:            uuid.New().String(),
		DbOperations: []*types.DBOperation{
			{
				DbName: worldstate.DefaultDBName,
				DataWrites: []*types.DataWrite{
					{
						Key:   "foo",
						Value: []byte("bar"),
					},
				},
			},
		},
	}

	_, err = env.client.SubmitTransaction(constants.PostDataTx,
		&types.DataTxEnvelope{
			Payload: dataTx,
			Signatures: map[string][]byte{
				"admin": testutils.SignatureFromTx(t, env.adminSigner, dataTx),
			},
		})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		data, err := env.client.GetData(&types.GetDataQueryEnvelope{
			Payload: &types.GetDataQuery{
				DbName: worldstate.DefaultDBName,
				UserId: "admin",
				Key:    "foo",
			},
			Signature: testutils.SignatureFromQuery(t, env.adminSigner, dataQuery),
		})
		if err != nil {
			return false
		}

		if data.GetResponse() == nil {
			return false
		}

		dataB, err := json.Marshal(data.GetResponse())
		require.NoError(t, err)

		err = verifier.Verify(dataB, data.GetSignature())
		if err != nil {
			t.Fatal(err)
		}

		return data.GetResponse().GetValue() != nil &&
			bytes.Equal(data.GetResponse().GetValue(), []byte("bar"))
	}, time.Minute, 100*time.Millisecond)

	provenanceQuery := &types.GetHistoricalDataQuery{
		UserId: "admin",
		DbName: worldstate.DefaultDBName,
		Key:    "foo",
	}

	values, err := env.client.GetHistoricalData(
		&types.GetHistoricalDataQueryEnvelope{
			Payload:   provenanceQuery,
			Signature: testutils.SignatureFromQuery(t, env.adminSigner, provenanceQuery),
		},
	)
	require.NoError(t, err)

	valuesB, err := json.Marshal(values.GetResponse())
	require.NoError(t, err)

	err = verifier.Verify(valuesB, values.GetSignature())
	require.NoError(t, err)

	require.Len(t, values.GetResponse().GetValues(), 1)
	require.Equal(t, values.GetResponse().Values[0].GetValue(), []byte("bar"))
}

func TestServerWithUserAdminRequest(t *testing.T) {
	env := newServerTestEnv(t)
	defer env.cleanup(t)

	userCert, _, err := testutils.IssueCertificate("BCDB User", "127.0.0.1", env.caKeys)
	require.NoError(t, err)
	certBlock, _ := pem.Decode(userCert)

	userTx := &types.UserAdministrationTx{
		TxId:   uuid.New().String(),
		UserId: "admin",
		UserWrites: []*types.UserWrite{
			{
				User: &types.User{
					Id: "testUser",
					Privilege: &types.Privilege{
						DbPermission: map[string]types.Privilege_Access{
							worldstate.DefaultDBName: types.Privilege_ReadWrite,
						},
					},
					Certificate: certBlock.Bytes,
				},
			},
		},
	}
	_, err = env.client.SubmitTransaction(constants.PostUserTx,
		&types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, userTx),
		})
	require.NoError(t, err)

	verifier, err := env.getNodeSigVerifier(t)
	require.NoError(t, err)

	query := &types.GetUserQuery{UserId: "admin", TargetUserId: "testUser"}
	querySig, err := cryptoservice.SignQuery(env.adminSigner, query)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		user, err := env.client.GetUser(&types.GetUserQueryEnvelope{
			Payload:   query,
			Signature: querySig,
		})

		if err != nil {
			return false
		}

		userB, err := json.Marshal(user.GetResponse())
		require.NoError(t, err)

		err = verifier.Verify(userB, user.GetSignature())
		if err != nil {
			t.Fatal(err)
		}

		if user.GetResponse() == nil {
			return false
		}

		return user.GetResponse().GetUser() != nil &&
			user.GetResponse().GetUser().GetId() == "testUser"
	}, time.Minute, 100*time.Millisecond)
}

func TestServerWithDBAdminRequest(t *testing.T) {
	env := newServerTestEnv(t)
	defer env.cleanup(t)

	dbTx := &types.DBAdministrationTx{
		TxId:      uuid.New().String(),
		UserId:    "admin",
		CreateDbs: []string{"testDB"},
	}
	// Create new database
	_, err := env.client.SubmitTransaction(constants.PostDBTx,
		&types.DBAdministrationTxEnvelope{
			Payload:   dbTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, dbTx),
		})
	require.NoError(t, err)

	verifier, err := env.getNodeSigVerifier(t)
	require.NoError(t, err)

	dbStatusQuery := &types.GetDBStatusQuery{UserId: "admin", DbName: "testDB"}
	require.Eventually(t, func() bool {
		db, err := env.client.GetDBStatus(&types.GetDBStatusQueryEnvelope{
			Payload:   dbStatusQuery,
			Signature: testutils.SignatureFromQuery(t, env.adminSigner, dbStatusQuery),
		})
		if err != nil {
			return false
		}

		dbB, err := json.Marshal(db.GetResponse())
		require.NoError(t, err)

		err = verifier.Verify(dbB, db.GetSignature())
		if err != nil {
			t.Fatal(err)
		}

		return db.GetResponse().GetExist()
	}, time.Minute, 100*time.Millisecond)

	dataTx := &types.DataTx{
		MustSignUserIds: []string{"admin"},
		TxId:            uuid.New().String(),
		DbOperations: []*types.DBOperation{
			{
				DbName: "testDB",
				DataWrites: []*types.DataWrite{
					{
						Key:   "foo",
						Value: []byte("bar"),
					},
				},
			},
		},
	}

	// Post transaction into new database
	_, err = env.client.SubmitTransaction(constants.PostDataTx,
		&types.DataTxEnvelope{
			Payload: dataTx,
			Signatures: map[string][]byte{
				"admin": testutils.SignatureFromTx(t, env.adminSigner, dataTx),
			},
		})
	require.NoError(t, err)

	// Make sure key was created and we can query it
	dataQuery := &types.GetDataQuery{DbName: "testDB", UserId: "admin", Key: "foo"}
	dataQuerySig, err := cryptoservice.SignQuery(env.adminSigner, dataQuery)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		data, err := env.client.GetData(&types.GetDataQueryEnvelope{
			Payload:   dataQuery,
			Signature: dataQuerySig,
		})

		if err != nil {
			return false
		}

		dataB, err := json.Marshal(data.GetResponse())
		require.NoError(t, err)

		err = verifier.Verify(dataB, data.GetSignature())
		if err != nil {
			t.Fatal(err)
		}

		return data.GetResponse().GetValue() != nil &&
			bytes.Equal(data.GetResponse().GetValue(), []byte("bar"))
	}, time.Minute, 100*time.Millisecond)
}

func TestServerWithFailureScenarios(t *testing.T) {
	testCases := []struct {
		testName         string
		envelopeProvider func(signer crypto.Signer) *types.GetDataQueryEnvelope
		expectedError    string
		skip             bool
	}{
		{
			testName: "bad signature",
			envelopeProvider: func(_ crypto.Signer) *types.GetDataQueryEnvelope {
				getKeyQuery := &types.GetDataQuery{
					UserId: "admin",
					DbName: worldstate.DefaultDBName,
					Key:    "test",
				}

				return &types.GetDataQueryEnvelope{
					Payload:   getKeyQuery,
					Signature: []byte{0},
				}
			},
			expectedError: "signature verification failed",
		},
		{
			testName: "missing database",
			envelopeProvider: func(signer crypto.Signer) *types.GetDataQueryEnvelope {
				getKeyQuery := &types.GetDataQuery{
					UserId: "admin",
					DbName: "testDB",
					Key:    "test",
				}

				sig, err := cryptoservice.SignQuery(signer, getKeyQuery)
				require.NoError(t, err)

				return &types.GetDataQueryEnvelope{
					Payload:   getKeyQuery,
					Signature: sig,
				}
			},
			expectedError: "error db 'testDB' doesn't exist",
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.testName, func(t *testing.T) {
			t.Parallel()

			env := newServerTestEnv(t)
			defer env.cleanup(t)

			envelope := tt.envelopeProvider(env.adminSigner)
			resp, err := env.client.GetData(envelope)
			require.Error(t, err, "resp: %+v", resp)
			require.Contains(t, err.Error(), tt.expectedError)
		})
	}
}

func TestServerWithRestart(t *testing.T) {
	env := newServerTestEnv(t)
	defer env.cleanup(t)

	userCert, _, err := testutils.IssueCertificate("BCDB User", "127.0.0.1", env.caKeys)
	require.NoError(t, err)
	certBlock, _ := pem.Decode(userCert)

	userTx := &types.UserAdministrationTx{
		TxId:   uuid.New().String(),
		UserId: "admin",
		UserWrites: []*types.UserWrite{
			{
				User: &types.User{
					Id: "testUser",
					Privilege: &types.Privilege{
						DbPermission: map[string]types.Privilege_Access{
							worldstate.DefaultDBName: types.Privilege_ReadWrite,
						},
					},
					Certificate: certBlock.Bytes,
				},
			},
		},
	}
	_, err = env.client.SubmitTransaction(constants.PostUserTx,
		&types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, userTx),
		})
	require.NoError(t, err)

	verifier, err := env.getNodeSigVerifier(t)
	require.NoError(t, err)

	query := &types.GetUserQuery{UserId: "admin", TargetUserId: "testUser"}
	querySig, err := cryptoservice.SignQuery(env.adminSigner, query)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		user, err := env.client.GetUser(&types.GetUserQueryEnvelope{
			Payload:   query,
			Signature: querySig,
		})

		if err != nil {
			return false
		}

		userB, err := json.Marshal(user.GetResponse())
		require.NoError(t, err)

		err = verifier.Verify(userB, user.GetSignature())
		if err != nil {
			t.Fatal(err)
		}

		if user.GetResponse() == nil {
			return false
		}

		return user.GetResponse().GetUser() != nil &&
			user.GetResponse().GetUser().GetId() == "testUser"
	}, time.Minute, 100*time.Millisecond)

	t.Log("Before restart")
	env.restart(t)

	t.Log("After restart")

	querySig, err = cryptoservice.SignQuery(env.adminSigner, query)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		user, err := env.client.GetUser(&types.GetUserQueryEnvelope{
			Payload:   query,
			Signature: querySig,
		})

		if err != nil {
			t.Logf("query error: %s", err)
			return false
		}

		userB, err := json.Marshal(user.GetResponse())
		require.NoError(t, err)

		err = verifier.Verify(userB, user.GetSignature())
		if err != nil {
			t.Fatal(err)
		}

		if user.GetResponse() == nil {
			t.Log("nil payload")
			return false
		}

		return user.GetResponse().GetUser() != nil &&
			user.GetResponse().GetUser().GetId() == "testUser"
	}, 10*time.Second, 100*time.Millisecond)
}
