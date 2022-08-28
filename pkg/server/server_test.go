// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package server

import (
	"bytes"
	"crypto/tls"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/certificateauthority"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/marshal"
	"github.com/hyperledger-labs/orion-server/pkg/server/mock"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

var basePort uint32 = 5100
var basePortMutex sync.Mutex

type serverTestEnv struct {
	serverConfig                   *config.Configurations
	bcdbHTTPServer                 *BCDBHTTPServer
	caKeys                         tls.Certificate
	testDataPath                   string
	adminSigner                    crypto.Signer
	client                         *mock.Client
	signingCACertificateCollection *certificateauthority.CACertCollection
	tlsCACertificateCollection     *certificateauthority.CACertCollection
	serverTLS                      bool
	clientTLS                      bool
	tempDir                        string
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

	isLeaderCond := func() bool {
		return env.bcdbHTTPServer.IsLeader() == nil
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	port, err := env.bcdbHTTPServer.Port()
	require.NoError(t, err)
	if env.serverTLS {
		caConfig, err := certificateauthority.LoadCAConfig(&env.serverConfig.SharedConfig.CAConfig)
		require.NoError(t, err)
		caCertCollection, err := certificateauthority.NewCACertCollection(caConfig.GetRoots(), caConfig.GetIntermediates())
		require.NoError(t, err)

		tlsClientConfig := &tls.Config{
			RootCAs:    caCertCollection.GetCertPool(),
			ClientCAs:  caCertCollection.GetCertPool(),
			MinVersion: tls.VersionTLS12,
		}
		if env.clientTLS {
			clientKeyBytes, err := os.ReadFile(path.Join(env.tempDir, "tlsClient.key"))
			require.NoError(t, err)
			clientCertBytes, err := os.ReadFile(path.Join(env.tempDir, "tlsClient.pem"))
			require.NoError(t, err)
			clientKeyPair, err := tls.X509KeyPair(clientCertBytes, clientKeyBytes)
			require.NoError(t, err)
			tlsClientConfig.Certificates = []tls.Certificate{clientKeyPair}
		}

		env.client, err = mock.NewRESTClient(fmt.Sprintf("https://127.0.0.1:%s", port), nil, tlsClientConfig)
	} else {
		env.client, err = mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port), nil, nil)
	}
	require.NoError(t, err)
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

	err = env.signingCACertificateCollection.VerifyLeafCert(cfg.GetResponse().GetNodeConfig().GetCertificate())
	require.NoError(t, err)

	return crypto.NewVerifier(cfg.GetResponse().GetNodeConfig().GetCertificate())
}

func (env *serverTestEnv) getConfigResponse(t *testing.T) *types.GetConfigResponse {
	configQuery := &types.GetConfigQuery{
		UserId: "admin",
	}
	cfg, err := env.client.GetConfig(&types.GetConfigQueryEnvelope{
		Payload:   configQuery,
		Signature: testutils.SignatureFromQuery(t, env.adminSigner, configQuery),
	})
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.NotNil(t, cfg.Response)

	return cfg.Response
}

func newServerTestEnv(t *testing.T, serverTLS bool, clientTLS bool, disableProvenance bool) *serverTestEnv {
	tempDir, err := ioutil.TempDir("/tmp", "serverTest")
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Errorf("error while removing test directory: %v", err)
		}
	})

	rootCAPemCert, caPrivKey, err := testutils.GenerateRootCA("Orion RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, rootCAPemCert)
	require.NotNil(t, caPrivKey)

	caKeyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
	require.NoError(t, err)
	require.NotNil(t, caKeyPair)

	block, _ := pem.Decode(rootCAPemCert)
	certsCollection, err := certificateauthority.NewCACertCollection([][]byte{block.Bytes}, nil)
	require.NoError(t, err)

	err = certsCollection.VerifyCollection()
	require.NoError(t, err)

	err = os.WriteFile(path.Join(tempDir, "serverRootCACert.pem"), rootCAPemCert, 0666)
	require.NoError(t, err)

	pemCert, privKey, err := testutils.IssueCertificate("Orion Instance", "127.0.0.1", caKeyPair)
	require.NoError(t, err)
	err = os.WriteFile(path.Join(tempDir, "server.pem"), pemCert, 0666)
	require.NoError(t, err)
	err = os.WriteFile(path.Join(tempDir, "server.key"), privKey, 0666)
	require.NoError(t, err)

	pemAdminCert, pemAdminKey, err := testutils.IssueCertificate("Orion Admin", "127.0.0.1", caKeyPair)
	require.NoError(t, err)
	err = os.WriteFile(path.Join(tempDir, "admin.pem"), pemAdminCert, 0666)
	require.NoError(t, err)
	err = os.WriteFile(path.Join(tempDir, "admin.key"), pemAdminKey, 0666)
	require.NoError(t, err)

	adminSigner, err := crypto.NewSigner(
		&crypto.SignerOptions{
			Identity:    "admin",
			KeyFilePath: path.Join(tempDir, "admin.key"),
		},
	)
	require.NoError(t, err)

	if serverTLS {
		tlsRootCAPemCert, tlsCaPrivKey, err := testutils.GenerateRootCA("Orion TLS RootCA", "127.0.0.1")
		require.NoError(t, err)
		require.NotNil(t, tlsRootCAPemCert)
		require.NotNil(t, tlsCaPrivKey)

		tlsCAKeyPair, err := tls.X509KeyPair(tlsRootCAPemCert, tlsCaPrivKey)
		require.NoError(t, err)
		require.NotNil(t, tlsCAKeyPair)

		block, _ := pem.Decode(tlsRootCAPemCert)
		tlsCertsCollection, err := certificateauthority.NewCACertCollection([][]byte{block.Bytes}, nil)
		require.NoError(t, err)

		err = tlsCertsCollection.VerifyCollection()
		require.NoError(t, err)

		err = os.WriteFile(path.Join(tempDir, "tlsServerRootCACert.pem"), tlsRootCAPemCert, 0666)
		require.NoError(t, err)

		tlsPemCert, tlsPrivKey, err := testutils.IssueCertificate("Orion TLS Instance", "127.0.0.1", tlsCAKeyPair)
		require.NoError(t, err)
		err = os.WriteFile(path.Join(tempDir, "tlsServer.pem"), tlsPemCert, 0666)
		require.NoError(t, err)
		err = os.WriteFile(path.Join(tempDir, "tlsServer.key"), tlsPrivKey, 0666)
		require.NoError(t, err)
		if clientTLS {
			tlsPemCert, tlsPrivKey, err := testutils.IssueCertificate("Orion Client TLS Instance", "127.0.0.1", tlsCAKeyPair)
			require.NoError(t, err)
			err = os.WriteFile(path.Join(tempDir, "tlsClient.pem"), tlsPemCert, 0666)
			require.NoError(t, err)
			err = os.WriteFile(path.Join(tempDir, "tlsClient.key"), tlsPrivKey, 0666)
			require.NoError(t, err)

		}
	}

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
				Provenance: config.ProvenanceConf{
					Disabled: disableProvenance,
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
				AuxDir:  path.Join(tempDir, "aux"),
				Network: config.NetworkConf{Address: "127.0.0.1", Port: peerPort},
				TLS:     config.TLSConf{Enabled: false},
			},
		},
		SharedConfig: &config.SharedConfiguration{
			Nodes: []*config.NodeConf{
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

	if serverTLS {
		serverConfig.SharedConfig.CAConfig.RootCACertsPath = append(serverConfig.SharedConfig.CAConfig.RootCACertsPath, path.Join(tempDir, "tlsServerRootCACert.pem"))
		serverConfig.LocalConfig.Server.TLS.Enabled = true
		serverConfig.LocalConfig.Server.TLS.ServerCertificatePath = path.Join(tempDir, "tlsServer.pem")
		serverConfig.LocalConfig.Server.TLS.ServerKeyPath = path.Join(tempDir, "tlsServer.key")
		if clientTLS {
			serverConfig.LocalConfig.Server.TLS.ClientAuthRequired = true
		}
	}

	server, err := New(serverConfig)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)

	isLeaderCond := func() bool {
		return server.IsLeader() == nil
	}
	require.Eventually(t, isLeaderCond, 30*time.Second, 100*time.Millisecond)

	env := &serverTestEnv{
		serverConfig:                   serverConfig,
		bcdbHTTPServer:                 server,
		caKeys:                         caKeyPair,
		testDataPath:                   tempDir,
		adminSigner:                    adminSigner,
		signingCACertificateCollection: certsCollection,
		tempDir:                        tempDir,
		clientTLS:                      clientTLS,
		serverTLS:                      serverTLS,
	}

	port, err := server.Port()
	require.NoError(t, err)
	var client *mock.Client
	if serverTLS {
		caConfig, err := certificateauthority.LoadCAConfig(&serverConfig.SharedConfig.CAConfig)
		require.NoError(t, err)
		caCertCollection, err := certificateauthority.NewCACertCollection(caConfig.GetRoots(), caConfig.GetIntermediates())
		require.NoError(t, err)

		tlsClientConfig := &tls.Config{
			RootCAs:    caCertCollection.GetCertPool(),
			ClientCAs:  caCertCollection.GetCertPool(),
			MinVersion: tls.VersionTLS12,
		}
		if clientTLS {
			clientKeyBytes, err := os.ReadFile(path.Join(tempDir, "tlsClient.key"))
			require.NoError(t, err)
			clientCertBytes, err := os.ReadFile(path.Join(tempDir, "tlsClient.pem"))
			require.NoError(t, err)
			clientKeyPair, err := tls.X509KeyPair(clientCertBytes, clientKeyBytes)
			require.NoError(t, err)
			tlsClientConfig.Certificates = []tls.Certificate{clientKeyPair}
		}
		client, err = mock.NewRESTClient(fmt.Sprintf("https://127.0.0.1:%s", port), nil, tlsClientConfig)
		require.NoError(t, err)
	} else {
		client, err = mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port), nil, nil)
		require.NoError(t, err)
	}
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
	env := newServerTestEnv(t, false, false, false)
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

	resp, err := marshal.DefaultMarshaler().Marshal(data.GetResponse())
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

	httpResp, err := env.client.SubmitTransaction(
		constants.PostDataTx,
		&types.DataTxEnvelope{
			Payload: dataTx,
			Signatures: map[string][]byte{
				"admin": testutils.SignatureFromTx(t, env.adminSigner, dataTx),
			},
		},
		0, // async, if times out
	)
	require.NoError(t, err)
	require.True(t, httpResp.StatusCode == http.StatusAccepted || httpResp.StatusCode == http.StatusOK)
	err = httpResp.Body.Close()
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

		dataB, err := marshal.DefaultMarshaler().Marshal(data.GetResponse())
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
		constants.URLForGetHistoricalData(worldstate.DefaultDBName, "foo"),
		&types.GetHistoricalDataQueryEnvelope{
			Payload:   provenanceQuery,
			Signature: testutils.SignatureFromQuery(t, env.adminSigner, provenanceQuery),
		},
	)
	require.NoError(t, err)

	valuesB, err := marshal.DefaultMarshaler().Marshal(values.GetResponse())
	require.NoError(t, err)

	err = verifier.Verify(valuesB, values.GetSignature())
	require.NoError(t, err)

	require.Len(t, values.GetResponse().GetValues(), 1)
	require.Equal(t, values.GetResponse().Values[0].GetValue(), []byte("bar"))
}

func TestServerWithDataRequestAndProvenanceOff(t *testing.T) {
	// Scenario: we instantiate a server, trying to query for key,
	// making sure key does not exist and then posting it into DB
	env := newServerTestEnv(t, false, false, true)
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

	resp, err := marshal.DefaultMarshaler().Marshal(data.GetResponse())
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

	httpResp, err := env.client.SubmitTransaction(
		constants.PostDataTx,
		&types.DataTxEnvelope{
			Payload: dataTx,
			Signatures: map[string][]byte{
				"admin": testutils.SignatureFromTx(t, env.adminSigner, dataTx),
			},
		},
		0, // async, if times out
	)
	require.NoError(t, err)
	require.True(t, httpResp.StatusCode == http.StatusAccepted || httpResp.StatusCode == http.StatusOK)
	err = httpResp.Body.Close()
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

		dataB, err := marshal.DefaultMarshaler().Marshal(data.GetResponse())
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

	_, err = env.client.GetHistoricalData(
		constants.URLForGetHistoricalData(worldstate.DefaultDBName, "foo"),
		&types.GetHistoricalDataQueryEnvelope{
			Payload:   provenanceQuery,
			Signature: testutils.SignatureFromQuery(t, env.adminSigner, provenanceQuery),
		},
	)
	require.EqualError(t, err, "error while processing 'GET /provenance/data/history/bdb/foo' because provenance store is disabled on this server")

	env.serverConfig.LocalConfig.Server.Provenance.Disabled = false

	t.Log("Restarting with provenance enabled")
	t.Log("Stopping server")
	err = env.bcdbHTTPServer.Stop()
	require.NoError(t, err)

	localConfigOnly := &config.Configurations{
		LocalConfig:  env.serverConfig.LocalConfig,
		SharedConfig: nil,
	}

	env.bcdbHTTPServer, err = New(localConfigOnly)
	require.EqualError(t, err,
		fmt.Sprintf("error while creating the database object: error while creating the provenance store: provenance store was disabled and cannot be re-enabled: disabled flag exists: %s",
			path.Join(env.serverConfig.LocalConfig.Server.Database.LedgerDirectory, "provenancestore", "disabled")))
}

func TestServerWithUserAdminRequest(t *testing.T) {
	env := newServerTestEnv(t, false, false, false)
	defer env.cleanup(t)

	userCert, _, err := testutils.IssueCertificate("Orion User", "127.0.0.1", env.caKeys)
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
	httpResp, err := env.client.SubmitTransaction(
		constants.PostUserTx,
		&types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, userTx),
		},
		0, // async, if times out
	)
	require.NoError(t, err)
	require.True(t, httpResp.StatusCode == http.StatusAccepted || httpResp.StatusCode == http.StatusOK)
	err = httpResp.Body.Close()
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

		userB, err := marshal.DefaultMarshaler().Marshal(user.GetResponse())
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
	env := newServerTestEnv(t, false, false, false)
	defer env.cleanup(t)

	dbTx := &types.DBAdministrationTx{
		TxId:      uuid.New().String(),
		UserId:    "admin",
		CreateDbs: []string{"testDB"},
	}
	// Create new database
	httpResp, err := env.client.SubmitTransaction(
		constants.PostDBTx,
		&types.DBAdministrationTxEnvelope{
			Payload:   dbTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, dbTx),
		},
		0, // async, if times out
	)
	require.NoError(t, err)
	require.True(t, httpResp.StatusCode == http.StatusAccepted || httpResp.StatusCode == http.StatusOK)
	httpResp.Body.Close()
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

		dbB, err := marshal.DefaultMarshaler().Marshal(db.GetResponse())
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
	httpResp, err = env.client.SubmitTransaction(
		constants.PostDataTx,
		&types.DataTxEnvelope{
			Payload: dataTx,
			Signatures: map[string][]byte{
				"admin": testutils.SignatureFromTx(t, env.adminSigner, dataTx),
			},
		},
		30*time.Second, // sync
	)
	require.NoError(t, err)
	require.True(t, httpResp.StatusCode == http.StatusOK)
	httpResp.Body.Close()
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

		dataB, err := marshal.DefaultMarshaler().Marshal(data.GetResponse())
		require.NoError(t, err)

		err = verifier.Verify(dataB, data.GetSignature())
		if err != nil {
			t.Fatal(err)
		}

		return data.GetResponse().GetValue() != nil &&
			bytes.Equal(data.GetResponse().GetValue(), []byte("bar"))
	}, time.Minute, 100*time.Millisecond)
}

func TestServerWithConfigRequest(t *testing.T) {
	env := newServerTestEnv(t, false, false, false)
	defer env.cleanup(t)

	configRes := env.getConfigResponse(t)
	newConfig := configRes.Config

	//update raft parameters
	newConfig.ConsensusConfig.RaftConfig.TickInterval = "300ms"
	newConfig.ConsensusConfig.RaftConfig.HeartbeatTicks = 3
	newConfig.ConsensusConfig.RaftConfig.ElectionTicks = 30

	newConfigTx := &types.ConfigTx{
		UserId:               "admin",
		TxId:                 uuid.New().String(),
		ReadOldConfigVersion: configRes.Metadata.Version,
		NewConfig:            newConfig,
	}

	httpResp, err := env.client.SubmitTransaction(
		constants.GetConfig,
		&types.ConfigTxEnvelope{
			Payload:   newConfigTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, newConfigTx),
		},
		30*time.Second, // sync
	)
	require.NoError(t, err)
	require.True(t, httpResp.StatusCode == http.StatusOK)
	httpResp.Body.Close()
	require.NoError(t, err)

	configRes = env.getConfigResponse(t)
	require.Equal(t, "300ms", configRes.Config.ConsensusConfig.RaftConfig.TickInterval)
	require.Equal(t, uint32(0x3), configRes.Config.ConsensusConfig.RaftConfig.HeartbeatTicks)
	require.Equal(t, uint32(0x1e), configRes.Config.ConsensusConfig.RaftConfig.ElectionTicks)
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

			env := newServerTestEnv(t, false, false, false)
			defer env.cleanup(t)

			envelope := tt.envelopeProvider(env.adminSigner)
			resp, err := env.client.GetData(envelope)
			require.Error(t, err, "resp: %+v", resp)
			require.Contains(t, err.Error(), tt.expectedError)
		})
	}
}

func TestSyncTxWithServerTLS(t *testing.T) {
	env := newServerTestEnv(t, true, false, false)
	defer env.cleanup(t)

	userCert, _, err := testutils.IssueCertificate("Orion User", "127.0.0.1", env.caKeys)
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
	httpResp, err := env.client.SubmitTransaction(constants.PostUserTx,
		&types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, userTx),
		}, 30*time.Second, // Sync
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	httpResp.Body.Close()

	verifier, err := env.getNodeSigVerifier(t)
	require.NoError(t, err)

	query := &types.GetUserQuery{UserId: "admin", TargetUserId: "testUser"}
	querySig, err := cryptoservice.SignQuery(env.adminSigner, query)
	require.NoError(t, err)
	user, err := env.client.GetUser(&types.GetUserQueryEnvelope{
		Payload:   query,
		Signature: querySig,
	})
	require.NoError(t, err)
	require.NotNil(t, user.GetResponse())

	userBytes, err := marshal.DefaultMarshaler().Marshal(user.GetResponse())
	require.NoError(t, err)

	err = verifier.Verify(userBytes, user.GetSignature())
	require.NoError(t, err)

	require.True(t, user.GetResponse().GetUser() != nil &&
		user.GetResponse().GetUser().GetId() == "testUser")
}

func TestSyncTxWithServerHttpsClientHttp(t *testing.T) {
	env := newServerTestEnv(t, true, false, false)
	defer env.cleanup(t)

	// Convert https to http, should fail
	var err error
	port, err := env.bcdbHTTPServer.Port()
	require.NoError(t, err)
	env.client.RawURL = fmt.Sprintf("http://127.0.0.1:%s", port)
	env.client.BaseURL, err = url.Parse(env.client.RawURL)

	userCert, _, err := testutils.IssueCertificate("Orion User", "127.0.0.1", env.caKeys)
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
	httpResp, err := env.client.SubmitTransaction(constants.PostUserTx,
		&types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, userTx),
		},
		30*time.Second, //Sync
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, httpResp.StatusCode)
	httpResp.Body.Close()
}

func TestSyncTxWithServerTLSClientHasWrongCA(t *testing.T) {
	env := newServerTestEnv(t, true, false, false)
	defer env.cleanup(t)

	// Recreating client
	port, err := env.bcdbHTTPServer.Port()
	require.NoError(t, err)

	tlsRootCAPemCert, tlsCaPrivKey, err := testutils.GenerateRootCA("Orion TLS RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, tlsRootCAPemCert)
	require.NotNil(t, tlsCaPrivKey)

	tlsCAKeyPair, err := tls.X509KeyPair(tlsRootCAPemCert, tlsCaPrivKey)
	require.NoError(t, err)
	require.NotNil(t, tlsCAKeyPair)

	block, _ := pem.Decode(tlsRootCAPemCert)
	tlsCertsCollection, err := certificateauthority.NewCACertCollection([][]byte{block.Bytes}, nil)
	require.NoError(t, err)

	tlsClientConfig := &tls.Config{
		RootCAs:    tlsCertsCollection.GetCertPool(),
		ClientCAs:  tlsCertsCollection.GetCertPool(),
		MinVersion: tls.VersionTLS12,
	}

	env.client, err = mock.NewRESTClient(fmt.Sprintf("https://127.0.0.1:%s", port), nil, tlsClientConfig)

	userCert, _, err := testutils.IssueCertificate("Orion User", "127.0.0.1", env.caKeys)
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
	httpResp, err := env.client.SubmitTransaction(constants.PostUserTx,
		&types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, userTx),
		},
		30*time.Second, // Sync
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "certificate signed by unknown authority")
	require.Nil(t, httpResp)
}

func TestSyncTxWithServerAndClientTLS(t *testing.T) {
	env := newServerTestEnv(t, true, true, false)
	defer env.cleanup(t)

	userCert, _, err := testutils.IssueCertificate("Orion User", "127.0.0.1", env.caKeys)
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
	httpResp, err := env.client.SubmitTransaction(constants.PostUserTx,
		&types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, userTx),
		}, 30*time.Second, // Sync
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, httpResp.StatusCode)
	httpResp.Body.Close()

	verifier, err := env.getNodeSigVerifier(t)
	require.NoError(t, err)

	query := &types.GetUserQuery{UserId: "admin", TargetUserId: "testUser"}
	querySig, err := cryptoservice.SignQuery(env.adminSigner, query)
	require.NoError(t, err)
	user, err := env.client.GetUser(&types.GetUserQueryEnvelope{
		Payload:   query,
		Signature: querySig,
	})
	require.NoError(t, err)
	require.NotNil(t, user.GetResponse())

	userBytes, err := marshal.DefaultMarshaler().Marshal(user.GetResponse())
	require.NoError(t, err)

	err = verifier.Verify(userBytes, user.GetSignature())
	require.NoError(t, err)

	require.True(t, user.GetResponse().GetUser() != nil &&
		user.GetResponse().GetUser().GetId() == "testUser")
}

func TestServerWithRestart(t *testing.T) {
	env := newServerTestEnv(t, false, false, false)
	defer env.cleanup(t)

	userCert, _, err := testutils.IssueCertificate("Orion User", "127.0.0.1", env.caKeys)
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
	httpResp, err := env.client.SubmitTransaction(
		constants.PostUserTx,
		&types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, env.adminSigner, userTx),
		},
		0, // async, if times out
	)
	require.NoError(t, err)
	require.True(t, httpResp.StatusCode == http.StatusAccepted || httpResp.StatusCode == http.StatusOK)
	httpResp.Body.Close()
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

		userB, err := marshal.DefaultMarshaler().Marshal(user.GetResponse())
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

		userB, err := marshal.DefaultMarshaler().Marshal(user.GetResponse())
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
