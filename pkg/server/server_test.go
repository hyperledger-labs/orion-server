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
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/certificateauthority"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/server/mock"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

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

	port, err := env.bcdbHTTPServer.Port()
	require.NoError(t, err)
	client, err := mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port))
	require.NoError(t, err)
	env.client = client
}

func (env *serverTestEnv) getNodeSigVerifier(t *testing.T) (*crypto.Verifier, error) {
	configQuery := &types.GetNodeConfigQuery{
		NodeID: env.bcdbHTTPServer.conf.LocalConfig.Node.Identity.ID,
		UserID: "admin",
	}

	cfg, err := env.client.GetNodeConfig(&types.GetNodeConfigQueryEnvelope{
		Payload:   configQuery,
		Signature: testutils.SignatureFromQuery(t, env.adminSigner, configQuery),
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.NotNil(t, cfg.Payload)

	configPayload := &types.Payload{}
	err = json.Unmarshal(cfg.Payload, configPayload)
	require.NoError(t, err)

	nodeConfig := &types.GetNodeConfigResponse{}
	err = json.Unmarshal(configPayload.Response, nodeConfig)
	require.NoError(t, err)

	err = env.certCol.VerifyLeafCert(nodeConfig.GetNodeConfig().GetCertificate())
	require.NoError(t, err)

	return crypto.NewVerifier(nodeConfig.GetNodeConfig().GetCertificate())
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

	adminSigner, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: path.Join(tempDir, "admin.key")})
	require.NoError(t, err)

	nodeID := "testNode" + uuid.New().String()
	serverConfig := &config.Configurations{
		LocalConfig: &config.LocalConfiguration{
			Node: config.NodeConf{
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
					Port:    0, // use ephemeral port for testing
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
		},
		SharedConfig: &config.SharedConfiguration{
			Nodes: []config.DBNodeConf{
				{
					NodeID:          nodeID,
					Host:            "127.0.0.1",
					Port:            0,
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
			Consensus: config.ConsensusConf{
				Algorithm: "solo",
			},
		},
	}
	server, err := New(serverConfig)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	env := &serverTestEnv{
		serverConfig:   serverConfig,
		bcdbHTTPServer: server,
		caKeys:         keyPair,
		testDataPath:   tempDir,
		adminSigner:    adminSigner,
		certCol:        certsCollection,
	}

	t.Cleanup(func() {
		if err := env.bcdbHTTPServer.Stop(); err != nil {
			t.Errorf("error while stopping the server: %v", err)
		}
	})

	port, err := server.Port()
	require.NoError(t, err)
	client, err := mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port))
	require.NoError(t, err)
	env.client = client

	return env
}

func TestServerWithDataRequestAndProvenanceQueries(t *testing.T) {
	// Scenario: we instantiate a server, trying to query for key,
	// making sure key does not exist and then posting it into DB
	t.Parallel()
	env := newServerTestEnv(t)

	verifier, err := env.getNodeSigVerifier(t)
	require.NoError(t, err)

	dataQuery := &types.GetDataQuery{
		DBName: worldstate.DefaultDBName,
		UserID: "admin",
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
	require.NotNil(t, data.Payload)

	err = verifier.Verify(data.GetPayload(), data.GetSignature())
	require.NoError(t, err)

	payload := &types.Payload{}
	err = json.Unmarshal(data.GetPayload(), payload)
	require.NoError(t, err)

	response := &types.GetDataResponse{}
	err = json.Unmarshal(payload.GetResponse(), response)
	if err != nil {
		t.Fatal(err)
	}

	require.Nil(t, response.Value)

	dataTx := &types.DataTx{
		UserID: "admin",
		TxID:   uuid.New().String(),
		DBOperations: []*types.DBOperation{
			{
				DBName: worldstate.DefaultDBName,
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
			Payload:   dataTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, dataTx),
		})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		data, err := env.client.GetData(&types.GetDataQueryEnvelope{
			Payload: &types.GetDataQuery{
				DBName: worldstate.DefaultDBName,
				UserID: "admin",
				Key:    "foo",
			},
			Signature: testutils.SignatureFromQuery(t, env.adminSigner, dataQuery),
		})
		if err != nil {
			return false
		}

		if data.GetPayload() == nil {
			return false
		}

		err = verifier.Verify(data.GetPayload(), data.GetSignature())
		if err != nil {
			t.Fatal(err)
		}

		payload := &types.Payload{}
		err = json.Unmarshal(data.Payload, payload)
		if err != nil {
			t.Fatal(err)
		}

		response := &types.GetDataResponse{}
		err = json.Unmarshal(payload.Response, response)
		if err != nil {
			t.Fatal(err)
		}

		return response.GetValue() != nil &&
			bytes.Equal(response.GetValue(), []byte("bar"))
	}, time.Minute, 100*time.Millisecond)

	provenanceQuery := &types.GetHistoricalDataQuery{
		UserID: "admin",
		DBName: worldstate.DefaultDBName,
		Key:    "foo",
	}

	values, err := env.client.GetHistoricalData(
		&types.GetHistoricalDataQueryEnvelope{
			Payload:   provenanceQuery,
			Signature: testutils.SignatureFromQuery(t, env.adminSigner, provenanceQuery),
		},
	)
	require.NoError(t, err)

	err = verifier.Verify(data.GetPayload(), data.GetSignature())
	require.NoError(t, err)

	payload = &types.Payload{}
	err = json.Unmarshal(values.GetPayload(), payload)
	require.NoError(t, err)

	historyResponse := &types.GetHistoricalDataResponse{}
	err = json.Unmarshal(payload.GetResponse(), historyResponse)
	require.NoError(t, err)

	require.Len(t, historyResponse.GetValues(), 1)
	require.Equal(t, historyResponse.Values[0].GetValue(), []byte("bar"))
}

func TestServerWithUserAdminRequest(t *testing.T) {
	t.Parallel()
	env := newServerTestEnv(t)

	userCert, _, err := testutils.IssueCertificate("BCDB User", "127.0.0.1", env.caKeys)
	require.NoError(t, err)
	certBlock, _ := pem.Decode(userCert)

	userTx := &types.UserAdministrationTx{
		TxID:   uuid.New().String(),
		UserID: "admin",
		UserWrites: []*types.UserWrite{
			{
				User: &types.User{
					ID: "testUser",
					Privilege: &types.Privilege{
						DBPermission: map[string]types.Privilege_Access{
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

	query := &types.GetUserQuery{UserID: "admin", TargetUserID: "testUser"}
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

		err = verifier.Verify(user.GetPayload(), user.GetSignature())
		if err != nil {
			t.Fatal(err)
		}

		if user.GetPayload() == nil {
			return false
		}

		payload := &types.Payload{}
		err = json.Unmarshal(user.GetPayload(), payload)
		require.NoError(t, err)

		response := &types.GetUserResponse{}
		err = json.Unmarshal(payload.GetResponse(), response)

		return response.GetUser() != nil &&
			response.GetUser().GetID() == "testUser"
	}, time.Minute, 100*time.Millisecond)
}

func TestServerWithDBAdminRequest(t *testing.T) {
	t.Parallel()
	env := newServerTestEnv(t)

	dbTx := &types.DBAdministrationTx{
		TxID:      uuid.New().String(),
		UserID:    "admin",
		CreateDBs: []string{"testDB"},
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

	dbStatusQuery := &types.GetDBStatusQuery{UserID: "admin", DBName: "testDB"}
	require.Eventually(t, func() bool {
		db, err := env.client.GetDBStatus(&types.GetDBStatusQueryEnvelope{
			Payload:   dbStatusQuery,
			Signature: testutils.SignatureFromQuery(t, env.adminSigner, dbStatusQuery),
		})
		if err != nil {
			return false
		}

		err = verifier.Verify(db.GetPayload(), db.GetSignature())
		if err != nil {
			t.Fatal(err)
		}

		payload := &types.Payload{}
		err = json.Unmarshal(db.GetPayload(), payload)
		require.NoError(t, err)

		response := &types.GetDBStatusResponse{}
		err = json.Unmarshal(payload.GetResponse(), response)

		return response.GetExist()
	}, time.Minute, 100*time.Millisecond)

	dataTx := &types.DataTx{
		UserID: "admin",
		TxID:   uuid.New().String(),
		DBOperations: []*types.DBOperation{
			{
				DBName: "testDB",
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
			Payload:   dataTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, dataTx),
		})
	require.NoError(t, err)

	// Make sure key was created and we can query it
	dataQuery := &types.GetDataQuery{DBName: "testDB", UserID: "admin", Key: "foo"}
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

		err = verifier.Verify(data.GetPayload(), data.GetSignature())
		if err != nil {
			t.Fatal(err)
		}
		payload := &types.Payload{}
		err = json.Unmarshal(data.GetPayload(), payload)
		require.NoError(t, err)

		response := &types.GetDataResponse{}
		err = json.Unmarshal(payload.GetResponse(), response)

		return response.GetValue() != nil &&
			bytes.Equal(response.GetValue(), []byte("bar"))
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
			testName: "do not have db access",
			envelopeProvider: func(signer crypto.Signer) *types.GetDataQueryEnvelope {
				getKeyQuery := &types.GetDataQuery{
					UserID: "admin",
					DBName: worldstate.DefaultDBName,
					Key:    "test",
				}

				sig, err := cryptoservice.SignQuery(signer, getKeyQuery)
				require.NoError(t, err)

				return &types.GetDataQueryEnvelope{
					Payload:   getKeyQuery,
					Signature: sig,
				}
			},
			expectedError: "[admin] has no permission to read from database [bdb]",
		},
		{
			testName: "bad signature",
			envelopeProvider: func(_ crypto.Signer) *types.GetDataQueryEnvelope {
				getKeyQuery := &types.GetDataQuery{
					UserID: "admin",
					DBName: worldstate.DefaultDBName,
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
					UserID: "admin",
					DBName: "testDB",
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
		t.Run(tt.testName, func(t *testing.T) {
			t.Parallel()
			env := newServerTestEnv(t)

			envelope := tt.envelopeProvider(env.adminSigner)
			_, err := env.client.GetData(envelope)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.expectedError)
		})
	}
}

func TestServerWithRestart(t *testing.T) {
	t.Parallel()
	env := newServerTestEnv(t)

	userCert, _, err := testutils.IssueCertificate("BCDB User", "127.0.0.1", env.caKeys)
	require.NoError(t, err)
	certBlock, _ := pem.Decode(userCert)

	userTx := &types.UserAdministrationTx{
		TxID:   uuid.New().String(),
		UserID: "admin",
		UserWrites: []*types.UserWrite{
			{
				User: &types.User{
					ID: "testUser",
					Privilege: &types.Privilege{
						DBPermission: map[string]types.Privilege_Access{
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

	query := &types.GetUserQuery{UserID: "admin", TargetUserID: "testUser"}
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

		err = verifier.Verify(user.GetPayload(), user.GetSignature())
		if err != nil {
			t.Fatal(err)
		}

		if user.GetPayload() == nil {
			return false
		}

		payload := &types.Payload{}
		err = json.Unmarshal(user.GetPayload(), payload)
		require.NoError(t, err)

		response := &types.GetUserResponse{}
		err = json.Unmarshal(payload.GetResponse(), response)

		return response.GetUser() != nil &&
			response.GetUser().GetID() == "testUser"
	}, time.Minute, 100*time.Millisecond)

	env.restart(t)

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

		err = verifier.Verify(user.GetPayload(), user.GetSignature())
		if err != nil {
			t.Fatal(err)
		}

		if user.GetPayload() == nil {
			t.Log("nil payload")
			return false
		}

		payload := &types.Payload{}
		err = json.Unmarshal(user.GetPayload(), payload)
		require.NoError(t, err)

		response := &types.GetUserResponse{}
		err = json.Unmarshal(payload.GetResponse(), response)

		return response.GetUser() != nil &&
			response.GetUser().GetID() == "testUser"
	}, 10*time.Second, 100*time.Millisecond)
}
