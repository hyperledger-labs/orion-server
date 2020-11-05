package server

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"path"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/server/mock"
)

func setupTestServer(t *testing.T) (*BCDBHTTPServer, tls.Certificate, error) {
	tempDir, err := ioutil.TempDir("/tmp", "serverTest")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	rootCAPemCert, caPrivKey, err := generateRootCA("BCDB RootCA", "127.0.0.1")
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

	pemCert, privKey, err := issueCertificate("BCDB Instance", "127.0.0.1", keyPair)
	require.NoError(t, err)

	pemCertFile, err := os.Create(path.Join(tempDir, "server.pem"))
	require.NoError(t, err)
	pemCertFile.Write(pemCert)
	pemCertFile.Close()

	pemPrivKeyFile, err := os.Create(path.Join(tempDir, "server.key"))
	require.NoError(t, err)
	pemPrivKeyFile.Write(privKey)
	pemPrivKeyFile.Close()

	pemAdminCert, _, err := issueCertificate("BCDB Admin", "127.0.0.1", keyPair)
	pemAdminCertFile, err := os.Create(path.Join(tempDir, "admin.pem"))
	require.NoError(t, err)
	pemAdminCertFile.Write(pemAdminCert)
	pemAdminCertFile.Close()

	server, err := New(&config.Configurations{
		Node: config.NodeConf{
			Identity: config.IdentityConf{
				ID:              "testNode",
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
	return server, keyPair, err
}

func issueCertificate(subjectCN string, host string, rootCAKeyPair tls.Certificate) ([]byte, []byte, error) {
	ca, err := x509.ParseCertificate(rootCAKeyPair.Certificate[0])
	if err != nil {
		return nil, nil, err
	}

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	pubKey := privKey.Public()

	ip := net.ParseIP(host)
	template, err := certTemplate(subjectCN, []net.IP{ip})
	if err != nil {
		return nil, nil, err
	}

	certBytes, err := x509.CreateCertificate(rand.Reader, template, ca, pubKey, rootCAKeyPair.PrivateKey)

	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	keyBytes, err := x509.MarshalECPrivateKey(privKey)
	caPvtPemByte := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	return certPem, caPvtPemByte, nil
}

func generateRootCA(subjectCN string, host string) ([]byte, []byte, error) {
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	pubKey := privKey.Public()

	ip := net.ParseIP(host)
	template, err := certTemplate(subjectCN, []net.IP{ip})
	if err != nil {
		return nil, nil, err
	}
	template.KeyUsage |= x509.KeyUsageCertSign
	template.IsCA = true

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, pubKey, privKey)

	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	keyBytes, err := x509.MarshalECPrivateKey(privKey)
	keyPem := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})

	return certPem, keyPem, nil
}

func certTemplate(subjectCN string, ips []net.IP) (*x509.Certificate, error) {
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	SN, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, err
	}

	return &x509.Certificate{
		Subject:               pkix.Name{CommonName: subjectCN},
		SerialNumber:          SN,
		NotBefore:             time.Now().Add(-5 * time.Minute),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IPAddresses:           ips,
	}, nil
}

func TestDataQueries_CheckKeyForExistenceAndPostNew(t *testing.T) {
	// Scenario: we instantiate a server, trying to query for key,
	// making sure key does not exist and then posting it into DB
	server, _, err := setupTestServer(t)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	port, err := server.Port()
	require.NoError(t, err)
	client, err := mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port))
	require.NoError(t, err)

	adminUserRec, err := client.GetUser(&types.GetUserQueryEnvelope{
		Payload: &types.GetUserQuery{
			UserID:       "admin",
			TargetUserID: "admin",
		},
		Signature: []byte{0},
	})
	require.NoError(t, err)
	require.NotNil(t, adminUserRec)
	require.NotNil(t, adminUserRec.GetPayload())
	require.NotNil(t, adminUserRec.GetPayload().GetUser())

	if adminUserRec.GetPayload().GetUser().GetPrivilege().DBPermission == nil {
		adminUserRec.GetPayload().GetUser().GetPrivilege().DBPermission = map[string]types.Privilege_Access{}
	}
	adminUserRec.GetPayload().GetUser().GetPrivilege().DBPermission["bdb"] = types.Privilege_ReadWrite

	_, err = client.SubmitTransaction(constants.PostUserTx, &types.UserAdministrationTxEnvelope{
		Payload: &types.UserAdministrationTx{
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
		},
		Signature: []byte{0},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		rec, err := client.GetUser(&types.GetUserQueryEnvelope{
			Payload: &types.GetUserQuery{
				UserID:       "admin",
				TargetUserID: "admin",
			},
			Signature: []byte{0},
		})
		acl, ok := rec.GetPayload().GetUser().GetPrivilege().GetDBPermission()["bdb"]
		return err == nil && ok && acl == types.Privilege_ReadWrite
	}, time.Minute, 100*time.Millisecond)

	data, err := client.GetData(&types.GetDataQueryEnvelope{
		Payload: &types.GetDataQuery{
			DBName: "bdb",
			UserID: "admin",
			Key:    "foo",
		},
		Signature: []byte{0}, // still no support for signature verification
	})
	require.NoError(t, err)
	require.NotNil(t, data)
	require.Nil(t, data.Payload.Value)

	// TODO (bartem): need to came up with the better way to handle
	// transaction submission and getting results back
	_, err = client.SubmitTransaction(constants.PostDataTx,
		&types.DataTxEnvelope{
			Payload: &types.DataTx{
				UserID: "admin",
				DBName: "bdb",
				TxID:   uuid.New().String(),
				DataWrites: []*types.DataWrite{
					{
						Key:   "foo",
						Value: []byte("bar"),
					},
				},
			},
			Signature: []byte{0},
		})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		data, err := client.GetData(&types.GetDataQueryEnvelope{
			Payload: &types.GetDataQuery{
				DBName: "bdb",
				UserID: "admin",
				Key:    "foo",
			},
			Signature: []byte{0},
		})

		return err == nil &&
			data.GetPayload().GetValue() != nil &&
			bytes.Equal(data.GetPayload().GetValue(), []byte("bar"))
	}, time.Minute, 100*time.Millisecond)
}

func TestDataQueries_ProvisionNewUser(t *testing.T) {
	server, caKeys, err := setupTestServer(t)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	port, err := server.Port()
	require.NoError(t, err)
	client, err := mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port))
	require.NoError(t, err)

	userCert, _, err := issueCertificate("BCDB User", "127.0.0.1", caKeys)
	require.NoError(t, err)
	certBlock, _ := pem.Decode(userCert)

	_, err = client.SubmitTransaction(constants.PostUserTx,
		&types.UserAdministrationTxEnvelope{
			Payload: &types.UserAdministrationTx{
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
			},
			Signature: []byte{0},
		})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		user, err := client.GetUser(&types.GetUserQueryEnvelope{
			Payload: &types.GetUserQuery{
				UserID:       "admin",
				TargetUserID: "testUser",
			},
			Signature: []byte{0},
		})
		return err == nil &&
			user.GetPayload().GetUser() != nil &&
			user.GetPayload().GetUser().GetID() == "testUser"
	}, time.Minute, 100*time.Millisecond)
}

func TestDataQueries_CreateNewDB(t *testing.T) {
	server, _, err := setupTestServer(t)
	require.NoError(t, err)

	err = server.Start()
	require.NoError(t, err)
	defer server.Stop()

	port, err := server.Port()
	require.NoError(t, err)
	client, err := mock.NewRESTClient(fmt.Sprintf("http://127.0.0.1:%s", port))
	require.NoError(t, err)

	// Create new database
	_, err = client.SubmitTransaction(constants.PostDBTx,
		&types.DBAdministrationTxEnvelope{
			Payload: &types.DBAdministrationTx{
				TxID:      uuid.New().String(),
				UserID:    "admin",
				CreateDBs: []string{"testDB"},
			},
			Signature: []byte{0},
		})
	require.NoError(t, err)

	adminUserRec, err := client.GetUser(&types.GetUserQueryEnvelope{
		Payload: &types.GetUserQuery{
			UserID:       "admin",
			TargetUserID: "admin",
		},
		Signature: []byte{0},
	})
	require.NoError(t, err)
	require.NotNil(t, adminUserRec)
	require.NotNil(t, adminUserRec.GetPayload())
	require.NotNil(t, adminUserRec.GetPayload().GetUser())

	if adminUserRec.GetPayload().GetUser().GetPrivilege().DBPermission == nil {
		adminUserRec.GetPayload().GetUser().GetPrivilege().DBPermission = map[string]types.Privilege_Access{}
	}
	adminUserRec.GetPayload().GetUser().GetPrivilege().DBPermission["testDB"] = types.Privilege_ReadWrite

	_, err = client.SubmitTransaction(constants.PostUserTx, &types.UserAdministrationTxEnvelope{
		Payload: &types.UserAdministrationTx{
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
		},
		Signature: []byte{0},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		rec, err := client.GetUser(&types.GetUserQueryEnvelope{
			Payload: &types.GetUserQuery{
				UserID:       "admin",
				TargetUserID: "admin",
			},
			Signature: []byte{0},
		})
		acl, ok := rec.GetPayload().GetUser().GetPrivilege().GetDBPermission()["testDB"]
		return err == nil && ok && acl == types.Privilege_ReadWrite
	}, time.Minute, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		db, err := client.GetDBStatus(&types.GetDBStatusQueryEnvelope{
			Payload: &types.GetDBStatusQuery{
				UserID: "admin",
				DBName: "testDB",
			},
			Signature: []byte{0},
		})
		return err == nil && db.GetPayload().GetExist()
	}, time.Minute, 100*time.Millisecond)

	// Post transaction into new database
	_, err = client.SubmitTransaction(constants.PostDataTx,
		&types.DataTxEnvelope{
			Payload: &types.DataTx{
				UserID: "admin",
				DBName: "testDB",
				TxID:   uuid.New().String(),
				DataWrites: []*types.DataWrite{
					{
						Key:   "foo",
						Value: []byte("bar"),
					},
				},
			},
			Signature: []byte{0},
		})
	require.NoError(t, err)

	// Make sure key was created and we can query it
	require.Eventually(t, func() bool {
		data, err := client.GetData(&types.GetDataQueryEnvelope{
			Payload: &types.GetDataQuery{
				DBName: "testDB",
				UserID: "admin",
				Key:    "foo",
			},
			Signature: []byte{0},
		})

		return err == nil &&
			data.GetPayload().GetValue() != nil &&
			bytes.Equal(data.GetPayload().GetValue(), []byte("bar"))
	}, time.Minute, 100*time.Millisecond)

}
