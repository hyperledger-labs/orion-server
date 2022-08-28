// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
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
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
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
	method               string
	cryptoMaterialsDir   string
	serverRootCACertPath string
	serverCertPath       string
	serverKeyPath        string
	adminID              string
	adminCertPath        string
	adminKeyPath         string
	usersCryptoDir       string
	queryLimit           uint64
	adminSigner          crypto.Signer
	cmd                  *exec.Cmd
	outBuffer            *gbytes.Buffer
	errBuffer            *gbytes.Buffer
	clientCheckRedirect  func(req *http.Request, via []*http.Request) error
	logger               *logger.SugarLogger
	mu                   sync.RWMutex
}

// NewServer creates a new blockchain database server
func NewServer(id uint64, clusterBaseDir string, baseNodePort, basePeerPort uint32, checkRedirect func(req *http.Request, via []*http.Request) error, logger *logger.SugarLogger, method string, queryLimit uint64) (*Server, error) {
	sNumber := strconv.FormatInt(int64(id+1), 10)
	bootstrapFile := filepath.Join(clusterBaseDir, "node-"+sNumber, "shared-config-bootstrap.yml")
	if method == "join" {
		bootstrapFile = filepath.Join(clusterBaseDir, "node-"+sNumber, "join-block.yml")
	}
	s := &Server{
		serverNum:           id + 1,
		serverID:            "node-" + sNumber,
		address:             "127.0.0.1",
		nodePort:            baseNodePort + uint32(id),
		peerPort:            basePeerPort + uint32(id),
		adminID:             "admin",
		configDir:           filepath.Join(clusterBaseDir, "node-"+sNumber),
		configFilePath:      filepath.Join(clusterBaseDir, "node-"+sNumber, "config.yml"),
		bootstrapFilePath:   bootstrapFile,
		method:              method,
		cryptoMaterialsDir:  filepath.Join(clusterBaseDir, "node-"+sNumber, "crypto"),
		usersCryptoDir:      filepath.Join(clusterBaseDir, "users"),
		queryLimit:          queryLimit,
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

func (s *Server) AdminCertPath() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.adminCertPath
}

func (s *Server) AdminKeyPath() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.adminKeyPath
}

func (s *Server) AdminSigner() crypto.Signer {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.adminSigner
}

func (s *Server) ConfigDir() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.configDir
}

func (s *Server) ConfigFilePath() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.configFilePath
}

func (s *Server) BootstrapFilePath() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.bootstrapFilePath
}

func (s *Server) Signer(userID string) (crypto.Signer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	signer, err := crypto.NewSigner(
		&crypto.SignerOptions{
			Identity:    userID,
			KeyFilePath: path.Join(s.usersCryptoDir, userID+".key"),
		},
	)
	if err != nil {
		return nil, err
	}

	return signer, nil
}

func (s *Server) SetAdmin(newAdminID string, newAdminCertPath string, newAdminKeyPath string, newAdminSigner crypto.Signer) {
	s.adminID = newAdminID
	s.adminSigner = newAdminSigner
	s.adminCertPath = newAdminCertPath
	s.adminKeyPath = newAdminKeyPath
}

func (s *Server) SetAdminSigner(newAdminSigner crypto.Signer) {
	s.adminSigner = newAdminSigner
}

func (s *Server) GetTxProof(t *testing.T, userID string, blockNumber, txIndex uint64) (*types.GetTxProofResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetTxProofQuery{
		UserId:      userID,
		BlockNumber: blockNumber,
		TxIndex:     txIndex,
	}

	response, err := client.GetTxProof(
		&types.GetTxProofQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		})

	return response, err
}

func (s *Server) QueryDataRange(t *testing.T, userID, dbName, startKey, endKey string, limit uint64) (*types.GetDataRangeResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	dataQuery := &types.GetDataQuery{
		UserId: userID,
		DbName: dbName,
	}
	dataRangeQuery := &types.GetDataRangeQuery{
		UserId:   userID,
		DbName:   dbName,
		StartKey: startKey,
		EndKey:   endKey,
		Limit:    limit,
	}

	response, err := client.GetDataRange(
		&types.GetDataQueryEnvelope{
			Payload:   dataQuery,
			Signature: testutils.SignatureFromQuery(t, signer, dataRangeQuery),
		}, startKey, endKey, limit)
	return response, err
}

func (s *Server) GetDataProof(t *testing.T, db, key, userID string, blockNumber uint64, isDeleted bool) (*types.GetDataProofResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetDataProofQuery{
		UserId:      userID,
		BlockNumber: blockNumber,
		DbName:      db,
		Key:         key,
		IsDeleted:   isDeleted,
	}

	response, err := client.GetDataProof(
		&types.GetDataProofQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		})

	return response, err
}

func (s *Server) QueryConfigBlockStatus(t *testing.T) (*types.GetConfigBlockResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	query := &types.GetConfigBlockQuery{
		UserId: s.AdminID(),
	}
	response, err := client.GetLastConfigBlockStatus(
		&types.GeConfigBlockQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, s.AdminSigner(), query),
		},
	)

	return response, err
}

func (s *Server) QueryLastBlockStatus(t *testing.T) (*types.GetBlockResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	query := &types.GetLastBlockQuery{
		UserId: s.AdminID(),
	}
	response, err := client.GetLastBlock(
		&types.GetLastBlockQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, s.AdminSigner(), query),
		},
	)

	return response, err
}

func (s *Server) QueryBlockHeader(t *testing.T, number uint64, forceParam bool, user string) (*types.GetBlockResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	userSigner, err := s.Signer(user)
	require.NoError(t, err)

	query := &types.GetBlockQuery{
		UserId:      user,
		BlockNumber: number,
	}
	response, err := client.GetBlockHeader(
		&types.GetBlockQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, userSigner, query),
		},
		forceParam,
	)

	return response, err
}

func (s *Server) QueryLedgerPath(t *testing.T, startNum, endNum uint64, user string) (*types.GetLedgerPathResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	userSigner, err := s.Signer(user)
	require.NoError(t, err)

	query := &types.GetLedgerPathQuery{
		UserId:           user,
		StartBlockNumber: startNum,
		EndBlockNumber:   endNum,
	}

	response, err := client.GetLedgerPath(&types.GetLedgerPathQueryEnvelope{
		Payload:   query,
		Signature: testutils.SignatureFromQuery(t, userSigner, query),
	})

	return response, err
}

func (s *Server) QueryTxReceipt(t *testing.T, txID, user string) (*types.TxReceiptResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	userSigner, err := s.Signer(user)
	require.NoError(t, err)

	query := &types.GetTxReceiptQuery{
		UserId: user,
		TxId:   txID,
	}

	response, err := client.GetTxReceipt(&types.GetTxReceiptQueryEnvelope{
		Payload:   query,
		Signature: testutils.SignatureFromQuery(t, userSigner, query),
	})

	return response, err
}

func (s *Server) QueryAugmentedBlockHeader(t *testing.T, number uint64, user string) (*types.GetAugmentedBlockHeaderResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	userSigner, err := s.Signer(user)
	require.NoError(t, err)

	query := &types.GetBlockQuery{
		UserId:      user,
		BlockNumber: number,
		Augmented:   true,
	}
	response, err := client.GetAugmentedBlockHeader(
		&types.GetBlockQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, userSigner, query),
		},
	)

	return response, err
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

func (s *Server) QueryConfig(t *testing.T, user string) (*types.GetConfigResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	query := &types.GetConfigQuery{
		UserId: user,
	}
	response, err := client.GetConfig(
		&types.GetConfigQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, s.AdminSigner(), query),
		},
	)

	return response, err
}

func (s *Server) QueryData(t *testing.T, db, key string, userID string) (*types.GetDataResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetDataQuery{
		UserId: userID,
		DbName: db,
		Key:    key,
	}
	response, err := client.GetData(
		&types.GetDataQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) GetAllValues(t *testing.T, db, key, userID string) (*types.GetHistoricalDataResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetHistoricalDataQuery{
		UserId: userID,
		DbName: db,
		Key:    key,
	}
	response, err := client.GetHistoricalData(
		constants.URLForGetHistoricalData(db, key),
		&types.GetHistoricalDataQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) GetValueAt(t *testing.T, db, key, userID string, ver *types.Version) (*types.GetHistoricalDataResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetHistoricalDataQuery{
		UserId:  userID,
		DbName:  db,
		Key:     key,
		Version: ver,
	}
	fmt.Println(constants.URLForGetHistoricalDataAt(db, key, ver))
	response, err := client.GetHistoricalData(
		constants.URLForGetHistoricalDataAt(db, key, ver),
		&types.GetHistoricalDataQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) GetPreviousValues(t *testing.T, db, key, userID string, ver *types.Version) (*types.GetHistoricalDataResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetHistoricalDataQuery{
		UserId:    userID,
		DbName:    db,
		Key:       key,
		Version:   ver,
		Direction: "previous",
	}
	response, err := client.GetHistoricalData(
		constants.URLForGetPreviousHistoricalData(db, key, ver),
		&types.GetHistoricalDataQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) GetNextValues(t *testing.T, db, key, userID string, ver *types.Version) (*types.GetHistoricalDataResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetHistoricalDataQuery{
		UserId:    userID,
		DbName:    db,
		Key:       key,
		Version:   ver,
		Direction: "next",
	}
	response, err := client.GetHistoricalData(
		constants.URLForGetNextHistoricalData(db, key, ver),
		&types.GetHistoricalDataQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) GetMostRecentValueAtOrBelow(t *testing.T, db, key, userID string, ver *types.Version) (*types.GetHistoricalDataResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetHistoricalDataQuery{
		UserId:     userID,
		DbName:     db,
		Key:        key,
		Version:    ver,
		MostRecent: true,
	}
	response, err := client.GetHistoricalData(
		constants.URLForGetHistoricalDataAtOrBelow(db, key, ver),
		&types.GetHistoricalDataQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) GetDeletedValues(t *testing.T, db, key, userID string) (*types.GetHistoricalDataResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetHistoricalDataQuery{
		UserId:      userID,
		DbName:      db,
		Key:         key,
		OnlyDeletes: true,
	}
	response, err := client.GetHistoricalData(
		constants.URLForGetHistoricalDeletedData(db, key),
		&types.GetHistoricalDataQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) GetValuesReadByUser(t *testing.T, userID, targetUserId string) (*types.GetDataProvenanceResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetDataReadByQuery{
		UserId:       userID,
		TargetUserId: targetUserId,
	}
	response, err := client.GetDataReadByUser(
		constants.URLForGetDataReadBy(targetUserId),
		&types.GetDataReadByQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) GetValuesWrittenByUser(t *testing.T, userID string, targetUserId string) (*types.GetDataProvenanceResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetDataWrittenByQuery{
		UserId:       userID,
		TargetUserId: targetUserId,
	}
	response, err := client.GetDataWrittenByUser(
		constants.URLForGetDataWrittenBy(targetUserId),
		&types.GetDataWrittenByQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) GetValuesDeletedByUser(t *testing.T, userID string, targetUserId string) (*types.GetDataProvenanceResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetDataDeletedByQuery{
		UserId:       userID,
		TargetUserId: targetUserId,
	}
	response, err := client.GetDataDeletedByUser(
		constants.URLForGetDataDeletedBy(targetUserId),
		&types.GetDataDeletedByQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) GetReaders(t *testing.T, dbName, key, userID string) (*types.GetDataReadersResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetDataReadersQuery{
		UserId: userID,
		DbName: dbName,
		Key:    key,
	}
	response, err := client.GetDataReaders(
		constants.URLForGetDataReaders(dbName, key),
		&types.GetDataReadersQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) GetWriters(t *testing.T, dbName, key, userID string) (*types.GetDataWritersResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetDataWritersQuery{
		UserId: userID,
		DbName: dbName,
		Key:    key,
	}
	response, err := client.GetDataWriters(
		constants.URLForGetDataWriters(dbName, key),
		&types.GetDataWritersQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) GetTxIDsSubmittedBy(t *testing.T, userID, targetUserId string) (*types.GetTxIDsSubmittedByResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetTxIDsSubmittedByQuery{
		UserId:       userID,
		TargetUserId: targetUserId,
	}
	response, err := client.GetTxIDsSubmitedBy(
		constants.URLForGetTxIDsSubmittedBy(targetUserId),
		&types.GetTxIDsSubmittedByQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) ExecuteJSONQuery(t *testing.T, userID, dbName, query string) (*types.DataQueryResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	dataJSONQuery := &types.DataJSONQuery{
		UserId: userID,
		DbName: dbName,
		Query:  query,
	}
	response, err := client.ExecuteJSONQuery(
		constants.URLForJSONQuery(dbName),
		dataJSONQuery,
		testutils.SignatureFromQuery(t, signer, dataJSONQuery),
	)

	return response, err
}

func (s *Server) QueryUser(t *testing.T, userID string) (*types.GetUserResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	query := &types.GetUserQuery{
		UserId:       s.AdminID(),
		TargetUserId: userID,
	}
	response, err := client.GetUser(
		&types.GetUserQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, s.AdminSigner(), query),
		},
	)

	return response, err
}

func (s *Server) GetDBStatus(t *testing.T, dbName string) (*types.GetDBStatusResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	query := &types.GetDBStatusQuery{
		UserId: s.AdminID(),
		DbName: dbName,
	}
	response, err := client.GetDBStatus(
		&types.GetDBStatusQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, s.AdminSigner(), query),
		},
	)

	return response, err
}

func (s *Server) GetDBIndex(t *testing.T, dbName string, userID string) (*types.GetDBIndexResponseEnvelope, error) {
	client, err := s.NewRESTClient(nil)
	if err != nil {
		return nil, err
	}

	signer, err := s.Signer(userID)
	if err != nil {
		return nil, err
	}

	query := &types.GetDBIndexQuery{
		UserId: userID,
		DbName: dbName,
	}
	response, err := client.GetDBIndex(
		&types.GetDBIndexQueryEnvelope{
			Payload:   query,
			Signature: testutils.SignatureFromQuery(t, signer, query),
		},
	)

	return response, err
}

func (s *Server) DeleteDataTx(t *testing.T, db, key string) (string, *types.TxReceipt, *types.DataTxEnvelope, error) {
	txID := uuid.New().String()
	dataTx := &types.DataTx{
		MustSignUserIds: []string{"admin"},
		TxId:            txID,
		DbOperations: []*types.DBOperation{
			{
				DbName: db,
				DataDeletes: []*types.DataDelete{
					{
						Key: key,
					},
				},
			},
		},
	}

	// Post transaction into new database
	txEnv := &types.DataTxEnvelope{
		Payload:    dataTx,
		Signatures: map[string][]byte{"admin": testutils.SignatureFromTx(t, s.AdminSigner(), dataTx)},
	}
	receipt, err := s.SubmitTransaction(t, constants.PostDataTx, txEnv)
	if err != nil {
		return txID, nil, nil, err
	}

	return txID, receipt, txEnv, nil
}

func (s *Server) WriteDataTx(t *testing.T, db, key string, value []byte) (string, *types.TxReceipt, *types.DataTxEnvelope, error) {
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
	txEnv := &types.DataTxEnvelope{
		Payload:    dataTx,
		Signatures: map[string][]byte{"admin": testutils.SignatureFromTx(t, s.AdminSigner(), dataTx)},
	}
	receipt, err := s.SubmitTransaction(t, constants.PostDataTx, txEnv)
	if err != nil {
		return txID, nil, nil, err
	}

	return txID, receipt, txEnv, nil
}

func (s *Server) UserWriteDataTx(t *testing.T, db, key string, value []byte, user string) (string, *types.TxReceipt, *types.DataTxEnvelope, error) {
	txID := uuid.New().String()
	dataTx := &types.DataTx{
		MustSignUserIds: []string{user},
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

	signer, err := s.Signer(user)
	if err != nil {
		return txID, nil, nil, err
	}
	// Post transaction into new database
	txEnv := &types.DataTxEnvelope{
		Payload:    dataTx,
		Signatures: map[string][]byte{user: testutils.SignatureFromTx(t, signer, dataTx)},
	}
	receipt, err := s.SubmitTransaction(t, constants.PostDataTx, txEnv)
	if err != nil {
		return txID, nil, nil, err
	}

	return txID, receipt, txEnv, nil
}

func (s *Server) WriteDataTxAsync(t *testing.T, db, key string, value []byte) (string, *types.DataTxEnvelope, error) {
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
	txEnv := &types.DataTxEnvelope{
		Payload:    dataTx,
		Signatures: map[string][]byte{"admin": testutils.SignatureFromTx(t, s.AdminSigner(), dataTx)},
	}
	err := s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
	if err != nil {
		return txID, nil, err
	}

	return txID, txEnv, nil
}

func (s *Server) CreateUsers(t *testing.T, users []*types.UserWrite) (*types.TxReceipt, error) {
	userTx := &types.UserAdministrationTx{
		UserId:     "admin",
		TxId:       uuid.New().String(),
		UserWrites: users,
	}

	receipt, err := s.SubmitTransaction(t, constants.PostUserTx, &types.UserAdministrationTxEnvelope{
		Payload:   userTx,
		Signature: testutils.SignatureFromTx(t, s.AdminSigner(), userTx),
	})
	if err != nil {
		return nil, err
	}

	return receipt, nil

}

func (s *Server) SubmitTransaction(t *testing.T, urlPath string, tx interface{}) (*types.TxReceipt, error) {
	client, err := s.NewRESTClient(s.clientCheckRedirect)
	if err != nil {
		return nil, err
	}

	// Post transaction into new database
	response, err := client.SubmitTransaction(urlPath, tx, 30*time.Second)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		var errMsg string
		if response.StatusCode == http.StatusAccepted {
			return nil, errors.Errorf("ServerTimeout")
		}
		if response.Body != nil {
			errRes := &types.HttpResponseErr{}
			if err = json.NewDecoder(response.Body).Decode(errRes); err != nil {
				errMsg = "(failed to parse the server's error message)"
			} else {
				errMsg = errRes.Error()
			}
		}

		return nil, errors.Errorf("failed to submit transaction, server returned: status: %s, message: %s", response.Status, errMsg)
	}

	requestBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		t.Errorf("error: %s", err)
		return nil, err
	}

	txResponseEnvelope := &types.TxReceiptResponseEnvelope{}
	if err := protojson.Unmarshal(requestBody, txResponseEnvelope); err != nil {
		t.Errorf("error: %s", err)
		return nil, err
	}

	// TODO validate server signature

	receipt := txResponseEnvelope.GetResponse().GetReceipt()

	if receipt != nil {
		validationInfo := receipt.GetHeader().GetValidationInfo()
		if validationInfo == nil {
			return receipt, errors.Errorf("server error: validation info is nil")
		} else {
			validFlag := validationInfo[receipt.TxIndex].GetFlag()
			if validFlag != types.Flag_VALID {
				return receipt, errors.Errorf("TxValidation: Flag: %s, Reason: %s", validFlag, validationInfo[receipt.TxIndex].ReasonIfInvalid)
			}
		}
	}

	return receipt, nil
}

func (s *Server) SubmitTransactionAsync(t *testing.T, urlPath string, tx interface{}) error {
	client, err := s.NewRESTClient(s.clientCheckRedirect)
	if err != nil {
		return err
	}

	response, err := client.SubmitTransaction(urlPath, tx, 0) // timeout==0 is async
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		var errMsg string
		if response.StatusCode == http.StatusAccepted {
			return errors.Errorf("ServerTimeout")
		}
		if response.Body != nil {
			errRes := &types.HttpResponseErr{}
			if err = json.NewDecoder(response.Body).Decode(errRes); err != nil {
				errMsg = "(failed to parse the server's error message)"
			} else {
				errMsg = errRes.Error()
			}
		}

		return errors.Errorf("failed to submit transaction, server returned: status: %s, message: %s", response.Status, errMsg)
	}

	txResponseEnvelope := &types.TxReceiptResponseEnvelope{}
	responseBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		t.Errorf("error: %s", err)
		return err
	}

	err = protojson.Unmarshal(responseBytes, txResponseEnvelope)
	if err != nil {
		t.Errorf("error: %s", err)
		return err
	}

	// TODO validate server signature

	return nil
}

func (s *Server) SetConfigTx(t *testing.T, newConfig *types.ClusterConfig, version *types.Version, signer crypto.Signer, user string) (string, *types.TxReceipt, error) {
	client, err := s.NewRESTClient(s.clientCheckRedirect)
	if err != nil {
		return "", nil, err
	}

	txID := uuid.New().String()
	configTx := &types.ConfigTx{
		UserId:               user,
		TxId:                 txID,
		ReadOldConfigVersion: version,
		NewConfig:            newConfig,
	}

	// Post transaction into new database
	response, err := client.SubmitTransaction(constants.PostConfigTx, &types.ConfigTxEnvelope{
		Payload:   configTx,
		Signature: testutils.SignatureFromTx(t, signer, configTx),
	}, 30*time.Second)

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

	requestBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		t.Errorf("error: %s", err)
		return "", nil, err
	}

	txResponseEnvelope := &types.TxReceiptResponseEnvelope{}
	if err := protojson.Unmarshal(requestBody, txResponseEnvelope); err != nil {
		t.Errorf("error: %s", err)
		return "", nil, err
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

func (s *Server) CreateNewCryptoMaterials(rootCAPemCert, caPrivKey []byte) (string, string, string, error) {
	keyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
	if err != nil {
		return "", "", "", err
	}
	rootCACertPath := path.Join(s.cryptoMaterialsDir, "serverRootCACert.pem")
	serverRootCACertFile, err := os.Create(rootCACertPath)
	if err != nil {
		return "", "", "", err
	}
	if _, err = serverRootCACertFile.Write(rootCAPemCert); err != nil {
		return "", "", "", err
	}
	if err = serverRootCACertFile.Close(); err != nil {
		return "", "", "", err
	}

	pemCert, privKey, err := testutils.IssueCertificate(s.serverID+" Instance", s.address, keyPair)
	if err != nil {
		return "", "", "", err
	}

	certPath := path.Join(s.cryptoMaterialsDir, "server.pem")
	pemCertFile, err := os.Create(certPath)
	if err != nil {
		return "", "", "", err
	}
	if _, err = pemCertFile.Write(pemCert); err != nil {
		return "", "", "", err
	}
	if err = pemCertFile.Close(); err != nil {
		return "", "", "", err
	}

	keyPath := path.Join(s.cryptoMaterialsDir, "server.key")
	pemPrivKeyFile, err := os.Create(keyPath)
	if err != nil {
		return "", "", "", err
	}
	if _, err = pemPrivKeyFile.Write(privKey); err != nil {
		return "", "", "", err
	}
	if err = pemPrivKeyFile.Close(); err != nil {
		return "", "", "", err
	}

	return keyPath, certPath, rootCACertPath, nil
}

func (s *Server) CreateCryptoMaterials(rootCAPemCert, caPrivKey []byte) error {
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

func (s *Server) CreateConfigFile(conf *config.LocalConfiguration) error {
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
			Provenance: config.ProvenanceConf{
				Disabled: conf.Server.Provenance.Disabled,
			},
			QueueLength: config.QueueLengthConf{
				Transaction:               1000,
				ReorderedTransactionBatch: 100,
				Block:                     100,
			},
			QueryProcessing: config.QueryProcessingConf{
				ResponseSizeLimitInBytes: s.queryLimit,
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
				Enabled:               conf.Replication.TLS.Enabled,
				ClientAuthRequired:    false,
				ServerCertificatePath: s.serverCertPath,
				ServerKeyPath:         s.serverKeyPath,
				ClientCertificatePath: s.serverCertPath,
				ClientKeyPath:         s.serverKeyPath,
				CaConfig: config.CAConfiguration{
					RootCACertsPath: []string{s.serverRootCACertPath},
				},
			},
		},
		Bootstrap: config.BootstrapConf{
			Method: s.method,
			File:   s.bootstrapFilePath,
		},
	}

	emptyBlockCreationConf := config.BlockCreationConf{}
	if conf.BlockCreation != emptyBlockCreationConf {
		localCofig.BlockCreation = conf.BlockCreation
	}
	if conf.Replication.TLS.ServerCertificatePath != "" && conf.Replication.TLS.ServerKeyPath != "" {
		localCofig.Replication.TLS.ServerKeyPath = conf.Replication.TLS.ServerKeyPath
		localCofig.Replication.TLS.ServerCertificatePath = conf.Replication.TLS.ServerCertificatePath
		localCofig.Replication.TLS.ClientKeyPath = conf.Replication.TLS.ServerKeyPath
		localCofig.Replication.TLS.ClientCertificatePath = conf.Replication.TLS.ServerCertificatePath
		localCofig.Replication.TLS.CaConfig = conf.Replication.TLS.CaConfig
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

func (s *Server) ID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.serverID
}

// NewRESTClient creates a new REST client for the user to submit requests and transactions
// to the server
func (s *Server) NewRESTClient(checkRedirect func(req *http.Request, via []*http.Request) error) (*mock.Client, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return mock.NewRESTClient(s.URL(), checkRedirect, nil)
}

// testFailure is in lieu of *testing.T for gomega's types.GomegaTestingT
type testFailure struct {
}

func (t *testFailure) Fatalf(format string, args ...interface{}) {
	log.Printf(format, args...)
}

func (t *testFailure) Helper() {
}

func CreateDatabases(t *testing.T, s *Server, dbNames []string, dbsIndex map[string]*types.DBIndex) {
	dbAdminTx := &types.DBAdministrationTx{
		UserId:    s.AdminID(),
		TxId:      uuid.New().String(),
		CreateDbs: dbNames,
		DbsIndex:  dbsIndex,
	}

	receipt, err := s.SubmitTransaction(t, constants.PostDBTx, &types.DBAdministrationTxEnvelope{
		Payload:   dbAdminTx,
		Signature: testutils.SignatureFromTx(t, s.AdminSigner(), dbAdminTx),
	})
	require.NoError(t, err)
	require.NotNil(t, receipt)

	for _, dbName := range dbNames {
		response, err := s.GetDBStatus(t, dbName)
		require.NoError(t, err)
		require.Equal(t, true, response.GetResponse().Exist)
	}
}

func CreateUsers(t *testing.T, s *Server, users []*types.UserWrite) {
	receipt, err := s.CreateUsers(t, users)
	require.NoError(t, err)
	require.NotNil(t, receipt)

	for _, user := range users {
		respEnv, err := s.QueryUser(t, user.GetUser().GetId())
		require.NoError(t, err)
		require.Equal(t, user.User, respEnv.GetResponse().User)
	}
}
