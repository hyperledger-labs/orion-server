package server

import (
	"bytes"
	"crypto/tls"
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
	"github.ibm.com/blockchaindb/server/internal/server/mock"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func setupTestServer(t *testing.T) (*BCDBHTTPServer, tls.Certificate, string, error) {
	tempDir, err := ioutil.TempDir("/tmp", "serverTest")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	rootCAPemCert, caPrivKey, err := testutils.GenerateRootCA("BCDB RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, rootCAPemCert)
	require.NotNil(t, caPrivKey)

	keyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
	require.NoError(t, err)
	require.NotNil(t, keyPair)

	serverRootCACertFile, err := os.Create(path.Join(tempDir, "serverRootCACert.pem"))
	require.NoError(t, err)
	serverRootCACertFile.Write(rootCAPemCert)
	serverRootCACertFile.Close()

	pemCert, privKey, err := testutils.IssueCertificate("BCDB Instance", "127.0.0.1", keyPair)
	require.NoError(t, err)

	pemCertFile, err := os.Create(path.Join(tempDir, "server.pem"))
	require.NoError(t, err)
	pemCertFile.Write(pemCert)
	pemCertFile.Close()

	pemPrivKeyFile, err := os.Create(path.Join(tempDir, "server.key"))
	require.NoError(t, err)
	pemPrivKeyFile.Write(privKey)
	pemPrivKeyFile.Close()

	pemAdminCert, pemAdminKey, err := testutils.IssueCertificate("BCDB Admin", "127.0.0.1", keyPair)
	pemAdminCertFile, err := os.Create(path.Join(tempDir, "admin.pem"))
	require.NoError(t, err)
	pemAdminCertFile.Write(pemAdminCert)
	pemAdminCertFile.Close()

	pemAdminKeyFile, err := os.Create(path.Join(tempDir, "admin.key"))
	require.NoError(t, err)
	pemAdminKeyFile.Write(pemAdminKey)
	pemAdminKeyFile.Close()

	server, err := New(&config.Configurations{
		Node: config.NodeConf{
			Identity: config.IdentityConf{
				ID:              "testNode" + uuid.New().String(),
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
		Admin: config.AdminConf{
			ID:              "admin",
			CertificatePath: path.Join(tempDir, "admin.pem"),
		},
		RootCA: config.RootCAConf{
			CertificatePath: path.Join(tempDir, "serverRootCACert.pem"),
		},
		Consensus: config.ConsensusConf{
			Algorithm:                   "solo",
			BlockTimeout:                500 * time.Millisecond,
			MaxBlockSize:                1,
			MaxTransactionCountPerBlock: 1,
		},
	})
	return server, keyPair, tempDir, err
}

func TestDataQueries_CheckKeyForExistenceAndPostNew(t *testing.T) {
	// Scenario: we instantiate a server, trying to query for key,
	// making sure key does not exist and then posting it into DB
	t.Parallel()
	server, _, tempDir, err := setupTestServer(t)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	port, err := server.Port()
	require.NoError(t, err)
	client, err := mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port))
	require.NoError(t, err)

	adminSigner, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: path.Join(tempDir, "admin.key")})
	require.NoError(t, err)
	query := &types.GetUserQuery{UserID: "admin", TargetUserID: "admin"}
	sig, err := cryptoservice.SignQuery(adminSigner, query)
	require.NoError(t, err)

	adminUserRec, err := client.GetUser(&types.GetUserQueryEnvelope{
		Payload:   query,
		Signature: sig,
	})
	require.NoError(t, err)
	require.NotNil(t, adminUserRec)
	require.NotNil(t, adminUserRec.GetPayload())
	require.NotNil(t, adminUserRec.GetPayload().GetUser())

	if adminUserRec.GetPayload().GetUser().GetPrivilege().DBPermission == nil {
		adminUserRec.GetPayload().GetUser().GetPrivilege().DBPermission = map[string]types.Privilege_Access{}
	}
	adminUserRec.GetPayload().GetUser().GetPrivilege().DBPermission["bdb"] = types.Privilege_ReadWrite

	userTx := &types.UserAdministrationTx{
		TxID:   uuid.New().String(),
		UserID: "admin",
		UserWrites: []*types.UserWrite{
			{
				User: adminUserRec.GetPayload().GetUser(),
				ACL:  adminUserRec.GetPayload().GetMetadata().GetAccessControl(),
			},
		},
		UserReads: []*types.UserRead{
			{
				UserID:  adminUserRec.GetPayload().GetUser().GetID(),
				Version: adminUserRec.GetPayload().GetMetadata().GetVersion(),
			},
		},
	}

	_, err = client.SubmitTransaction(constants.PostUserTx, &types.UserAdministrationTxEnvelope{
		Payload:   userTx,
		Signature: testutils.SignatureFromTx(t, adminSigner, userTx),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		rec, err := client.GetUser(&types.GetUserQueryEnvelope{
			Payload: &types.GetUserQuery{
				UserID:       "admin",
				TargetUserID: "admin",
			},
			Signature: sig,
		})
		acl, ok := rec.GetPayload().GetUser().GetPrivilege().GetDBPermission()["bdb"]
		return err == nil && ok && acl == types.Privilege_ReadWrite
	}, time.Minute, 100*time.Millisecond)

	dataQuery := &types.GetDataQuery{
		DBName: "bdb",
		UserID: "admin",
		Key:    "foo",
	}
	data, err := client.GetData(&types.GetDataQueryEnvelope{
		Payload:   dataQuery,
		Signature: testutils.SignatureFromQuery(t, adminSigner, dataQuery),
	})
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Nil(t, data.Payload.Value)

	dataTx := &types.DataTx{
		UserID: "admin",
		DBName: "bdb",
		TxID:   uuid.New().String(),
		DataWrites: []*types.DataWrite{
			{
				Key:   "foo",
				Value: []byte("bar"),
			},
		},
	}
	// TODO (bartem): need to came up with the better way to handle
	// transaction submission and getting results back
	_, err = client.SubmitTransaction(constants.PostDataTx,
		&types.DataTxEnvelope{
			Payload:   dataTx,
			Signature: testutils.SignatureFromTx(t, adminSigner, dataTx),
		})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		data, err := client.GetData(&types.GetDataQueryEnvelope{
			Payload: &types.GetDataQuery{
				DBName: "bdb",
				UserID: "admin",
				Key:    "foo",
			},
			Signature: testutils.SignatureFromQuery(t, adminSigner, dataQuery),
		})

		return err == nil &&
			data.GetPayload().GetValue() != nil &&
			bytes.Equal(data.GetPayload().GetValue(), []byte("bar"))
	}, time.Minute, 100*time.Millisecond)
}

func TestDataQueries_ProvisionNewUser(t *testing.T) {
	t.Parallel()
	server, caKeys, tempDir, err := setupTestServer(t)
	require.NoError(t, err)

	adminSigner, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: path.Join(tempDir, "admin.key")})
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	port, err := server.Port()
	require.NoError(t, err)
	client, err := mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port))
	require.NoError(t, err)

	userCert, _, err := testutils.IssueCertificate("BCDB User", "127.0.0.1", caKeys)
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
							"bdb": types.Privilege_ReadWrite,
						},
					},
					Certificate: certBlock.Bytes,
				},
			},
		},
	}
	_, err = client.SubmitTransaction(constants.PostUserTx,
		&types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, adminSigner, userTx),
		})
	require.NoError(t, err)

	query := &types.GetUserQuery{UserID: "admin", TargetUserID: "testUser"}
	querySig, err := cryptoservice.SignQuery(adminSigner, query)
	require.Eventually(t, func() bool {
		user, err := client.GetUser(&types.GetUserQueryEnvelope{
			Payload:   query,
			Signature: querySig,
		})
		return err == nil &&
			user.GetPayload().GetUser() != nil &&
			user.GetPayload().GetUser().GetID() == "testUser"
	}, time.Minute, 100*time.Millisecond)
}

func TestDataQueries_CreateNewDB(t *testing.T) {
	t.Parallel()
	server, _, tempDir, err := setupTestServer(t)
	require.NoError(t, err)

	adminSigner, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: path.Join(tempDir, "admin.key")})
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	port, err := server.Port()
	require.NoError(t, err)
	client, err := mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port))
	require.NoError(t, err)

	dbTx := &types.DBAdministrationTx{
		TxID:      uuid.New().String(),
		UserID:    "admin",
		CreateDBs: []string{"testDB"},
	}
	// Create new database
	_, err = client.SubmitTransaction(constants.PostDBTx,
		&types.DBAdministrationTxEnvelope{
			Payload:   dbTx,
			Signature: testutils.SignatureFromTx(t, adminSigner, dbTx),
		})
	require.NoError(t, err)

	queryUser := &types.GetUserQuery{UserID: "admin", TargetUserID: "admin"}
	queryUserSig, err := cryptoservice.SignQuery(adminSigner, queryUser)
	require.NoError(t, err)

	adminUserRec, err := client.GetUser(&types.GetUserQueryEnvelope{
		Payload:   queryUser,
		Signature: queryUserSig,
	})
	require.NoError(t, err)
	require.NotNil(t, adminUserRec)
	require.NotNil(t, adminUserRec.GetPayload())
	require.NotNil(t, adminUserRec.GetPayload().GetUser())

	if adminUserRec.GetPayload().GetUser().GetPrivilege().DBPermission == nil {
		adminUserRec.GetPayload().GetUser().GetPrivilege().DBPermission = map[string]types.Privilege_Access{}
	}
	adminUserRec.GetPayload().GetUser().GetPrivilege().DBPermission["testDB"] = types.Privilege_ReadWrite

	userTx := &types.UserAdministrationTx{
		TxID:   uuid.New().String(),
		UserID: "admin",
		UserWrites: []*types.UserWrite{
			{
				User: adminUserRec.GetPayload().GetUser(),
				ACL:  adminUserRec.GetPayload().GetMetadata().GetAccessControl(),
			},
		},
		UserReads: []*types.UserRead{
			{
				UserID:  adminUserRec.GetPayload().GetUser().GetID(),
				Version: adminUserRec.GetPayload().GetMetadata().GetVersion(),
			},
		},
	}
	_, err = client.SubmitTransaction(constants.PostUserTx, &types.UserAdministrationTxEnvelope{
		Payload:   userTx,
		Signature: testutils.SignatureFromTx(t, adminSigner, userTx),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		rec, err := client.GetUser(&types.GetUserQueryEnvelope{
			Payload:   queryUser,
			Signature: queryUserSig,
		})
		acl, ok := rec.GetPayload().GetUser().GetPrivilege().GetDBPermission()["testDB"]
		return err == nil && ok && acl == types.Privilege_ReadWrite
	}, time.Minute, 100*time.Millisecond)

	dbStatusQuery := &types.GetDBStatusQuery{UserID: "admin", DBName: "testDB"}
	require.Eventually(t, func() bool {
		db, err := client.GetDBStatus(&types.GetDBStatusQueryEnvelope{
			Payload:   dbStatusQuery,
			Signature: testutils.SignatureFromQuery(t, adminSigner, dbStatusQuery),
		})
		return err == nil && db.GetPayload().GetExist()
	}, time.Minute, 100*time.Millisecond)

	dataTx := &types.DataTx{
		UserID: "admin",
		DBName: "testDB",
		TxID:   uuid.New().String(),
		DataWrites: []*types.DataWrite{
			{
				Key:   "foo",
				Value: []byte("bar"),
			},
		},
	}
	// Post transaction into new database
	_, err = client.SubmitTransaction(constants.PostDataTx,
		&types.DataTxEnvelope{
			Payload:   dataTx,
			Signature: testutils.SignatureFromTx(t, adminSigner, dataTx),
		})
	require.NoError(t, err)

	// Make sure key was created and we can query it
	dataQuery := &types.GetDataQuery{DBName: "testDB", UserID: "admin", Key: "foo"}
	dataQuerySig, err := cryptoservice.SignQuery(adminSigner, dataQuery)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		data, err := client.GetData(&types.GetDataQueryEnvelope{
			Payload:   dataQuery,
			Signature: dataQuerySig,
		})

		return err == nil &&
			data.GetPayload().GetValue() != nil &&
			bytes.Equal(data.GetPayload().GetValue(), []byte("bar"))
	}, time.Minute, 100*time.Millisecond)
}

func TestDataQueries_FailureScenarios(t *testing.T) {
	testCases := []struct {
		testName         string
		envelopeProvider func(signer *crypto.Signer) *types.GetDataQueryEnvelope
		expectedError    string
		skip             bool
	}{
		{
			testName: "do not have db access",
			envelopeProvider: func(signer *crypto.Signer) *types.GetDataQueryEnvelope {
				getKeyQuery := &types.GetDataQuery{
					UserID: "admin",
					DBName: "bdb",
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
			envelopeProvider: func(_ *crypto.Signer) *types.GetDataQueryEnvelope {
				getKeyQuery := &types.GetDataQuery{
					UserID: "admin",
					DBName: "bdb",
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
			envelopeProvider: func(signer *crypto.Signer) *types.GetDataQueryEnvelope {
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
			fmt.Println("Starting Server")
			server, _, tempDir, err := setupTestServer(t)
			require.NoError(t, err)

			fmt.Println("Create Signer")
			adminSigner, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: path.Join(tempDir, "admin.key")})
			require.NoError(t, err)
			require.NotNil(t, adminSigner)

			err = server.Start()
			require.NoError(t, err)
			defer server.Stop()

			port, err := server.Port()
			require.NoError(t, err)
			fmt.Println("Create Client")
			client, err := mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port))
			require.NoError(t, err)

			fmt.Println("Send Request")
			envelope := tt.envelopeProvider(adminSigner)
			_, err = client.GetData(envelope)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.expectedError)
		})
	}
}
