// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"crypto/tls"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/mock"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pkg/errors"
)

// Server holds parameters related to the server
type Server struct {
	serverNum            uint64
	serverID             string
	address              string // For testing, the node-host and peer-host address are the same.
	nodePort             uint32
	peerPort             uint32
	configDir            string
	configFilePath       string
	bootstrapFilePath    string
	cryptoMaterialsDir   string
	serverRootCACertPath string
	serverCertPath       string
	serverKeyPath        string
	adminID              string
	adminCertPath        string
	adminKeyPath         string
	adminSigner          crypto.Signer
	cmd                  *exec.Cmd
	outBuffer            *gbytes.Buffer
	errBuffer            *gbytes.Buffer
	clientCheckRedirect  func(req *http.Request, via []*http.Request) error
	logger               *logger.SugarLogger
	mu                   sync.RWMutex
}

// NewServer creates a new blockchain database server
func NewServer(id uint64, clusterBaseDir string, baseNodePort, basePeerPort uint32, checkRedirect func(req *http.Request, via []*http.Request) error, logger *logger.SugarLogger) (*Server, error) {

	sNumber := strconv.FormatInt(int64(id+1), 10)
	s := &Server{
		serverNum:           id + 1,
		serverID:            "node-" + sNumber,
		address:             "127.0.0.1",
		nodePort:            baseNodePort + uint32(id),
		peerPort:            basePeerPort + uint32(id),
		adminID:             "admin",
		configDir:           filepath.Join(clusterBaseDir, "node-"+sNumber),
		configFilePath:      filepath.Join(clusterBaseDir, "node-"+sNumber, "config.yml"),
		bootstrapFilePath:   filepath.Join(clusterBaseDir, "node-"+sNumber, "shared-config-bootstrap.yml"),
		cryptoMaterialsDir:  filepath.Join(clusterBaseDir, "node-"+sNumber, "crypto"),
		clientCheckRedirect: checkRedirect,
		logger:              logger,
	}

	if err := fileops.CreateDir(s.configDir); err != nil {
		return nil, err
	}
	if err := fileops.CreateDir(s.cryptoMaterialsDir); err != nil {
		return nil, err
	}

	logger.Infof("Server %s on %s:%d", s.serverID, s.address, s.nodePort)
	return s, nil
}

func (s *Server) AdminID() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.adminID
}

func (s *Server) AdminSigner() crypto.Signer {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.adminSigner
}

func (s *Server) QueryClusterStatus(t *testing.T) (*types.GetClusterStatusResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	query := &types.GetClusterStatusQuery{
		UserId: s.AdminID(),
	}
	response, err := client.GetClusterStatus(
		&types.GetClusterStatusQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, s.AdminSigner(), query),
		},
	)

	return response, err
}

func (s *Server) QueryConfig(t *testing.T) (*types.GetConfigResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	query := &types.GetConfigQuery{
		UserId: s.AdminID(),
	}
	response, err := client.GetConfig(
		&types.GetConfigQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, s.AdminSigner(), query),
		},
	)

	return response, err
}

func (s *Server) QueryData(t *testing.T, db, key string) (*types.GetDataResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	query := &types.GetDataQuery{
		UserId: s.AdminID(),
		DbName: db,
		Key:    key,
	}
	response, err := client.GetData(
		&types.GetDataQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, s.AdminSigner(), query),
		},
	)

	return response, err
}

func (s *Server) WriteDataTx(t *testing.T, db, key string, value []byte) (string, *types.TxReceipt, error) {
	client, err := s.NewRESTClient(s.clientCheckRedirect)
	if err != nil {
		return "", nil, err
	}

	txID := uuid.New().String()
	dataTx := &types.DataTx{
		MustSignUserIds: []string{"admin"},
		TxId:            txID,
		DbOperations: []*types.DBOperation{
			{
				DbName: db,
				DataWrites: []*types.DataWrite{
					{
						Key:   key,
						Value: value,
					},
				},
			},
		},
	}

	// Post transaction into new database
	response, err := client.SubmitTransaction(constants.PostDataTx,
		&types.DataTxEnvelope{
			Payload: dataTx,
			Signatures: map[string][]byte{
				"admin": testutils.SignatureFromTx(t, s.AdminSigner(), dataTx),
			},
		})

	if err != nil {
		return txID, nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		var errMsg string
		if response.StatusCode == http.StatusAccepted {
			return txID, nil, errors.Errorf("ServerTimeout TxID: %s", txID)
		}
		if response.Body != nil {
			errRes := &types.HttpResponseErr{}
			if err := json.NewDecoder(response.Body).Decode(errRes); err != nil {
				errMsg = "(failed to parse the server's error message)"
			} else {
				errMsg = errRes.Error()
			}
		}

		return txID, nil, errors.Errorf("failed to submit transaction, server returned: status: %s, message: %s", response.Status, errMsg)
	}

	txResponseEnvelope := &types.TxReceiptResponseEnvelope{}
	err = json.NewDecoder(response.Body).Decode(txResponseEnvelope)
	if err != nil {
		t.Errorf("error: %s", err)
		return txID, nil, err
	}

	receipt := txResponseEnvelope.GetResponse().GetReceipt()

	if receipt != nil {
		validationInfo := receipt.GetHeader().GetValidationInfo()
		if validationInfo == nil {
			return txID, receipt, errors.Errorf("server error: validation info is nil")
		} else {
			validFlag := validationInfo[receipt.TxIndex].GetFlag()
			if validFlag != types.Flag_VALID {
				return txID, receipt, errors.Errorf("TxValidation TxID: %s, Flag: %s, Reason: %s", txID, validFlag, validationInfo[receipt.TxIndex].ReasonIfInvalid)
			}
		}
	}

	return txID, receipt, nil
}

func (s *Server) createCryptoMaterials(rootCAPemCert, caPrivKey []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
	if err != nil {
		return err
	}
	s.serverRootCACertPath = path.Join(s.cryptoMaterialsDir, "serverRootCACert.pem")
	serverRootCACertFile, err := os.Create(s.serverRootCACertPath)
	if err != nil {
		return err
	}
	if _, err = serverRootCACertFile.Write(rootCAPemCert); err != nil {
		return err
	}
	if err = serverRootCACertFile.Close(); err != nil {
		return err
	}

	pemCert, privKey, err := testutils.IssueCertificate(s.serverID+" Instance", s.address, keyPair)
	if err != nil {
		return err
	}

	s.serverCertPath = path.Join(s.cryptoMaterialsDir, "server.pem")
	pemCertFile, err := os.Create(s.serverCertPath)
	if err != nil {
		return err
	}
	if _, err = pemCertFile.Write(pemCert); err != nil {
		return err
	}
	if err = pemCertFile.Close(); err != nil {
		return err
	}

	s.serverKeyPath = path.Join(s.cryptoMaterialsDir, "server.key")
	pemPrivKeyFile, err := os.Create(s.serverKeyPath)
	if err != nil {
		return err
	}
	if _, err = pemPrivKeyFile.Write(privKey); err != nil {
		return err
	}
	if err = pemPrivKeyFile.Close(); err != nil {
		return err
	}

	return nil
}

func (s *Server) createConfigFile() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	localCofig := &config.LocalConfiguration{
		Server: config.ServerConf{
			Identity: config.IdentityConf{
				ID:              s.serverID,
				CertificatePath: s.serverCertPath,
				KeyPath:         s.serverKeyPath,
			},
			Network: config.NetworkConf{
				Address: s.address,
				Port:    uint32(s.nodePort),
			},
			Database: config.DatabaseConf{
				Name:            "leveldb",
				LedgerDirectory: filepath.Join(s.configDir, "ledger"),
			},
			QueueLength: config.QueueLengthConf{
				Transaction:               1000,
				ReorderedTransactionBatch: 100,
				Block:                     100,
			},
			LogLevel: "info",
		},
		BlockCreation: config.BlockCreationConf{
			MaxBlockSize:                1024 * 1024,
			MaxTransactionCountPerBlock: 10,
			BlockTimeout:                50 * time.Millisecond,
		},
		Replication: config.ReplicationConf{
			WALDir:  filepath.Join(s.configDir, "etcdraft", "wal"),
			SnapDir: filepath.Join(s.configDir, "etcdraft", "snap"),
			Network: config.NetworkConf{
				Address: s.address,
				Port:    uint32(s.peerPort),
			},
			TLS: config.TLSConf{
				Enabled: false,
			},
		},
		Bootstrap: config.BootstrapConf{
			Method: "genesis",
			File:   s.bootstrapFilePath,
		},
	}

	if err := WriteLocalConfig(localCofig, s.configFilePath); err != nil {
		return err
	}

	return nil
}

func (s *Server) createCmdToStartServers(executablePath string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.outBuffer = gbytes.NewBuffer()
	s.errBuffer = gbytes.NewBuffer()
	commandOut := io.MultiWriter(s.outBuffer, os.Stdout)
	commandErr := io.MultiWriter(s.errBuffer, os.Stderr)

	s.cmd = &exec.Cmd{
		Path:   executablePath,
		Args:   []string{executablePath, "start", "--configpath", s.configDir},
		Stdout: commandOut,
		Stderr: commandErr,
	}
}

func (s *Server) start(timeout time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Debug("Starting server " + s.serverID + " on " + s.address + ":" + strconv.FormatInt(int64(s.nodePort), 10))
	if err := s.cmd.Start(); err != nil {
		return errors.Wrap(err, "error while starting "+s.serverID)
	}

	log.Println("Check whether the server " + s.serverID + " has started")

	g := gomega.NewWithT(&testFailure{})

	if !g.Eventually(s.outBuffer, 10).Should(gbytes.Say("Starting to serve requests on: " + s.address + ":")) {
		return errors.New("failed to start the server: " + s.serverID)
	}

	port, err := retrievePort(string(s.outBuffer.Contents()), s.address)
	if err != nil {
		return err
	}

	s.logger.Debug("Successfully started server " + s.serverID + " on " + s.address + ":" + strconv.FormatInt(int64(port), 10))
	return nil
}

func (s *Server) shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.cmd.Process.Kill(); err != nil {
		return errors.Wrap(err, "error while shutting down "+s.serverID)
	}

	if _, err := s.cmd.Process.Wait(); err != nil {
		return errors.Wrap(err, "error while shutting down "+s.serverID)
	}

	return nil
}

func retrievePort(output string, addr string) (int, error) {
	toFind := "Starting to serve requests on: " + addr + ":"
	index := strings.Index(output, toFind)
	if index < 0 {
		return 0, errors.New("server " + addr + " has not started successfully yet")
	}

	portIndex := index + len(toFind)
	var portStr string
	for ch := output[portIndex]; ch != '\n'; ch = output[portIndex] {
		portStr += string(ch)
		portIndex++
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, err
	}
	return port, nil
}

func (s *Server) URL() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return "http://" + s.address + ":" + strconv.FormatInt(int64(s.nodePort), 10)
}

// NewRESTClient creates a new REST client for the user to submit requests and transactions
// to the server
func (s *Server) NewRESTClient(checkRedirect func(req *http.Request, via []*http.Request) error) (*mock.Client, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return mock.NewRESTClient(
		s.URL(),
		checkRedirect,
	)
}

// testFailure is in lieu of *testing.T for gomega's types.GomegaTestingT
type testFailure struct {
}

func (t *testFailure) Fatalf(format string, args ...interface{}) {
	log.Printf(format, args...)
}
