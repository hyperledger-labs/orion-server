package bcdb

import (
	"crypto/x509"
	"encoding/pem"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"io/ioutil"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/internal/blockstore"
	"github.ibm.com/blockchaindb/server/internal/fileops"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/provenance"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/internal/worldstate/leveldb"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

//go:generate mockery --dir . --name DB --case underscore --output mocks/

// DB encapsulates functionality required to operate with database state
type DB interface {
	// LedgerHeight returns current height of the ledger
	LedgerHeight() (uint64, error)

	// Height returns ledger height
	Height() (uint64, error)

	// DoesUserExist checks whenever user with given userID exists
	DoesUserExist(userID string) (bool, error)

	// GetCertificate returns the certificate associated with useID, if it exists.
	GetCertificate(userID string) (*x509.Certificate, error)

	// GetUser retrieves user' record
	GetUser(querierUserID, targetUserID string) (*types.GetUserResponseEnvelope, error)

	// GetConfig returns database configuration
	GetConfig() (*types.GetConfigResponseEnvelope, error)

	// GetNodeConfig returns single node subsection of database configuration
	GetNodeConfig(nodeID string) (*types.GetNodeConfigResponseEnvelope, error)

	// GetDBStatus returns status for database, checks whenever database was created
	GetDBStatus(dbName string) (*types.GetDBStatusResponseEnvelope, error)

	// GetData retrieves values for given key
	GetData(dbName, querierUserID, key string) (*types.GetDataResponseEnvelope, error)

	// GetBlockHeader returns ledger block header
	GetBlockHeader(userID string, blockNum uint64) (*types.GetBlockResponseEnvelope, error)

	// GetTxProof returns intermediate hashes to recalculate merkle tree root from tx hash
	GetTxProof(userID string, blockNum uint64, txIdx uint64) (*types.GetTxProofResponseEnvelope, error)

	// GetLedgerPath returns list of blocks that forms shortest path in skip list chain in ledger
	GetLedgerPath(userID string, start, end uint64) (*types.GetLedgerPathResponseEnvelope, error)

	// GetValues returns all values associated with a given key
	GetValues(dbName, key string) (*types.GetHistoricalDataResponseEnvelope, error)

	// GetDeletedValues returns all deleted values associated with a given key
	GetDeletedValues(dbname, key string) (*types.GetHistoricalDataResponseEnvelope, error)

	// GetValueAt returns the value of a given key at a particular version
	GetValueAt(dbName, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error)

	// GetPreviousValues returns previous values of a given key and a version. The number of records returned would be limited
	// by the limit parameters.
	GetPreviousValues(dbname, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error)

	// GetNextValues returns next values of a given key and a version. The number of records returned would be limited
	// by the limit parameters.
	GetNextValues(dbname, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error)

	// GetValuesReadByUser returns all values read by a given user
	GetValuesReadByUser(userID string) (*types.GetDataReadByResponseEnvelope, error)

	// GetValuesReadByUser returns all values read by a given user
	GetValuesWrittenByUser(userID string) (*types.GetDataWrittenByResponseEnvelope, error)

	// GetValuesDeletedByUser returns all values deleted by a given user
	GetValuesDeletedByUser(userID string) (*types.GetDataDeletedByResponseEnvelope, error)

	// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
	GetReaders(dbName, key string) (*types.GetDataReadersResponseEnvelope, error)

	// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
	GetWriters(dbName, key string) (*types.GetDataWritersResponseEnvelope, error)

	// GetTxIDsSubmittedByUser returns all ids of all transactions submitted by a given user
	GetTxIDsSubmittedByUser(userID string) (*types.GetTxIDsSubmittedByResponseEnvelope, error)

	// GetTxReceipt returns transaction receipt - block header of ledger block that contains the transaction
	// and transaction index inside the block
	GetTxReceipt(userId string, txID string) (*types.GetTxReceiptResponseEnvelope, error)

	// SubmitTransaction submits transaction to the database with a timeout. If the timeout is
	// set to 0, the submission would be treated as async while a non-zero timeout would be
	// treated as a sync submission. When a timeout occurs with the sync submission, a
	// timeout error will be returned
	SubmitTransaction(tx interface{}, timeout time.Duration) (*types.TxResponseEnvelope, error)

	// BootstrapDB given bootstrap configuration initialize database by
	// creating required system tables to include database meta data
	BootstrapDB(conf *config.Configurations) (*types.TxResponseEnvelope, error)

	// IsDBExists returns true if database with given name is exists otherwise false
	IsDBExists(name string) bool

	// Close frees and closes resources allocated by database instance
	Close() error
}

type db struct {
	worldstateQueryProcessor *worldstateQueryProcessor
	ledgerQueryProcessor     *ledgerQueryProcessor
	provenanceQueryProcessor *provenanceQueryProcessor
	txProcessor              *transactionProcessor
	db                       worldstate.DB
	blockStore               *blockstore.Store
	provenanceStore          *provenance.Store
	signer                   crypto.Signer
	logger                   *logger.SugarLogger
}

// NewDB creates a new database bcdb which handles both the queries and transactions.
func NewDB(conf *config.Configurations, logger *logger.SugarLogger) (DB, error) {
	if conf.Node.Database.Name != "leveldb" {
		return nil, errors.New("only leveldb is supported as the state database")
	}

	ledgerDir := conf.Node.Database.LedgerDirectory
	if err := createLedgerDir(ledgerDir); err != nil {
		return nil, err
	}

	levelDB, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: constructWorldStatePath(ledgerDir),
			Logger:    logger,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating the world state database")
	}

	blockStore, err := blockstore.Open(
		&blockstore.Config{
			StoreDir: constructBlockStorePath(ledgerDir),
			Logger:   logger,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating the block store")
	}

	provenanceStore, err := provenance.Open(
		&provenance.Config{
			StoreDir: constructProvenanceStorePath(ledgerDir),
			Logger:   logger,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating the block store")
	}

	querier := identity.NewQuerier(levelDB)

	signer, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: conf.Node.Identity.KeyPath})
	if err != nil {
		return nil, errors.Wrap(err, "can't load private key")
	}

	worldstateQueryProcessor := newWorldstateQueryProcessor(
		&worldstateQueryProcessorConfig{
			nodeID:          conf.Node.Identity.ID,
			db:              levelDB,
			blockStore:      blockStore,
			identityQuerier: querier,
			logger:          logger,
		},
	)

	ledgerQueryProcessorConfig := &ledgerQueryProcessorConfig{
		nodeID:          conf.Node.Identity.ID,
		db:              levelDB,
		blockStore:      blockStore,
		provenanceStore: provenanceStore,
		identityQuerier: querier,
		logger:          logger,
	}
	ledgerQueryProcessor := newLedgerQueryProcessor(ledgerQueryProcessorConfig)

	provenanceQueryProcessor := newProvenanceQueryProcessor(
		&provenanceQueryProcessorConfig{
			nodeID:          conf.Node.Identity.ID,
			provenanceStore: provenanceStore,
			logger:          logger,
		},
	)

	txProcessor, err := newTransactionProcessor(
		&txProcessorConfig{
			nodeID:             conf.Node.Identity.ID,
			db:                 levelDB,
			blockStore:         blockStore,
			provenanceStore:    provenanceStore,
			txQueueLength:      conf.Node.QueueLength.Transaction,
			txBatchQueueLength: conf.Node.QueueLength.ReorderedTransactionBatch,
			blockQueueLength:   conf.Node.QueueLength.Block,
			maxTxCountPerBatch: conf.Consensus.MaxTransactionCountPerBlock,
			batchTimeout:       conf.Consensus.BlockTimeout,
			logger:             logger,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "can't initiate tx processor")
	}

	return &db{
		worldstateQueryProcessor: worldstateQueryProcessor,
		ledgerQueryProcessor:     ledgerQueryProcessor,
		provenanceQueryProcessor: provenanceQueryProcessor,
		txProcessor:              txProcessor,
		db:                       levelDB,
		blockStore:               blockStore,
		provenanceStore:          provenanceStore,
		logger:                   logger,
		signer:                   signer,
	}, nil
}

// BootstrapDB bootstraps DB with system tables
func (d *db) BootstrapDB(conf *config.Configurations) (*types.TxResponseEnvelope, error) {
	configTx, err := prepareConfigTx(conf)
	if err != nil {
		return nil, errors.Wrap(err, "failed to prepare and commit a configuration transaction")
	}

	resp, err := d.SubmitTransaction(configTx, 30*time.Second)
	if err != nil {
		return nil, errors.Wrap(err, "error while committing configuration transaction")
	}
	return resp, nil
}

// LedgerHeight returns ledger height
func (d *db) LedgerHeight() (uint64, error) {
	return d.worldstateQueryProcessor.blockStore.Height()
}

// Height returns ledger height
func (d *db) Height() (uint64, error) {
	return d.worldstateQueryProcessor.db.Height()
}

// DoesUserExist checks whenever userID exists
func (d *db) DoesUserExist(userID string) (bool, error) {
	return d.worldstateQueryProcessor.identityQuerier.DoesUserExist(userID)
}

func (d *db) GetCertificate(userID string) (*x509.Certificate, error) {
	return d.worldstateQueryProcessor.identityQuerier.GetCertificate(userID)
}

// GetUser returns user's record
func (d *db) GetUser(querierUserID, targetUserID string) (*types.GetUserResponseEnvelope, error) {
	userResponse, err := d.worldstateQueryProcessor.getUser(querierUserID, targetUserID)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, userResponse)
	if err != nil {
		return nil, err
	}
	return &types.GetUserResponseEnvelope{
		Payload:   userResponse,
		Signature: sig,
	}, err
}

// GetNodeConfig returns single node subsection of database configuration
func (d *db) GetNodeConfig(nodeID string) (*types.GetNodeConfigResponseEnvelope, error) {
	nodeConfigResponse, err := d.worldstateQueryProcessor.getNodeConfig(nodeID)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, nodeConfigResponse)
	if err != nil {
		return nil, err
	}
	return &types.GetNodeConfigResponseEnvelope{
		Payload:   nodeConfigResponse,
		Signature: sig,
	}, err
}

// GetConfig returns database configuration
func (d *db) GetConfig() (*types.GetConfigResponseEnvelope, error) {
	configResponse, err := d.worldstateQueryProcessor.getConfig()
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, configResponse)
	if err != nil {
		return nil, err
	}
	return &types.GetConfigResponseEnvelope{
		Payload:   configResponse,
		Signature: sig,
	}, err
}

// GetDBStatus returns database status
func (d *db) GetDBStatus(dbName string) (*types.GetDBStatusResponseEnvelope, error) {
	dbStatusResponse, err := d.worldstateQueryProcessor.getDBStatus(dbName)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, dbStatusResponse)
	if err != nil {
		return nil, err
	}
	return &types.GetDBStatusResponseEnvelope{
		Payload:   dbStatusResponse,
		Signature: sig,
	}, err
}

// SubmitTransaction submits transaction to the database with a timeout. If the timeout is
// set to 0, the submission would be treated as async while a non-zero timeout would be
// treated as a sync submission. When a timeout occurs with the sync submission, a
// timeout error will be returned
func (d *db) SubmitTransaction(tx interface{}, timeout time.Duration) (*types.TxResponseEnvelope, error) {
	return d.txProcessor.submitTransaction(tx, timeout)
}

// GetData returns value for provided key
func (d *db) GetData(dbName, querierUserID, key string) (*types.GetDataResponseEnvelope, error) {
	dataResponse, err := d.worldstateQueryProcessor.getData(dbName, querierUserID, key)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, dataResponse)
	if err != nil {
		return nil, err
	}
	return &types.GetDataResponseEnvelope{
		Payload:   dataResponse,
		Signature: sig,
	}, err
}

func (d *db) IsDBExists(name string) bool {
	return d.worldstateQueryProcessor.isDBExists(name)
}

func (d *db) GetBlockHeader(userID string, blockNum uint64) (*types.GetBlockResponseEnvelope, error) {
	blockHeader, err := d.ledgerQueryProcessor.getBlockHeader(userID, blockNum)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, blockHeader)
	if err != nil {
		return nil, err
	}
	return &types.GetBlockResponseEnvelope{
		Payload:   blockHeader,
		Signature: sig,
	}, err
}

func (d *db) GetTxProof(userID string, blockNum uint64, txIdx uint64) (*types.GetTxProofResponseEnvelope, error) {
	proofResponse, err := d.ledgerQueryProcessor.getProof(userID, blockNum, txIdx)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, proofResponse)
	if err != nil {
		return nil, err
	}
	return &types.GetTxProofResponseEnvelope{
		Payload:   proofResponse,
		Signature: sig,
	}, err
}

func (d *db) GetLedgerPath(userID string, start, end uint64) (*types.GetLedgerPathResponseEnvelope, error) {
	pathResponse, err := d.ledgerQueryProcessor.getPath(userID, start, end)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, pathResponse)
	if err != nil {
		return nil, err
	}
	return &types.GetLedgerPathResponseEnvelope{
		Payload:   pathResponse,
		Signature: sig,
	}, err
}

func (d *db) GetTxReceipt(userId string, txID string) (*types.GetTxReceiptResponseEnvelope, error) {
	receiptResponse, err := d.ledgerQueryProcessor.getTxReceipt(userId, txID)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, receiptResponse)
	if err != nil {
		return nil, err
	}
	return &types.GetTxReceiptResponseEnvelope{
		Payload:   receiptResponse,
		Signature: sig,
	}, err
}

// GetValues returns all values associated with a given key
func (d *db) GetValues(dbName, key string) (*types.GetHistoricalDataResponseEnvelope, error) {
	values, err := d.provenanceQueryProcessor.GetValues(dbName, key)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, values)
	if err != nil {
		return nil, err
	}
	return &types.GetHistoricalDataResponseEnvelope{
		Payload:   values,
		Signature: sig,
	}, err
}

// GetDeletedValues returns all deleted values associated with a given key
func (d *db) GetDeletedValues(dbName, key string) (*types.GetHistoricalDataResponseEnvelope, error) {
	deletedValues, err := d.provenanceQueryProcessor.GetDeletedValues(dbName, key)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, deletedValues)
	if err != nil {
		return nil, err
	}
	return &types.GetHistoricalDataResponseEnvelope{
		Payload:   deletedValues,
		Signature: sig,
	}, err
}

// GetValueAt returns the value of a given key at a particular version
func (d *db) GetValueAt(dbName, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error) {
	valueAt, err := d.provenanceQueryProcessor.GetValueAt(dbName, key, version)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, valueAt)
	if err != nil {
		return nil, err
	}
	return &types.GetHistoricalDataResponseEnvelope{
		Payload:   valueAt,
		Signature: sig,
	}, err
}

// GetPreviousValues returns previous values of a given key and a version. The number of records returned would be limited
// by the limit parameters.
func (d *db) GetPreviousValues(dbName, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error) {
	previousValues, err := d.provenanceQueryProcessor.GetPreviousValues(dbName, key, version)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, previousValues)
	if err != nil {
		return nil, err
	}
	return &types.GetHistoricalDataResponseEnvelope{
		Payload:   previousValues,
		Signature: sig,
	}, err
}

// GetNextValues returns next values of a given key and a version. The number of records returned would be limited
// by the limit parameters.
func (d *db) GetNextValues(dbName, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error) {
	nextValues, err := d.provenanceQueryProcessor.GetNextValues(dbName, key, version)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, nextValues)
	if err != nil {
		return nil, err
	}
	return &types.GetHistoricalDataResponseEnvelope{
		Payload:   nextValues,
		Signature: sig,
	}, err

}

// GetValuesReadByUser returns all values read by a given user
func (d *db) GetValuesReadByUser(userID string) (*types.GetDataReadByResponseEnvelope, error) {
	readByUser, err := d.provenanceQueryProcessor.GetValuesReadByUser(userID)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, readByUser)
	if err != nil {
		return nil, err
	}
	return &types.GetDataReadByResponseEnvelope{
		Payload:   readByUser,
		Signature: sig,
	}, err
}

// GetValuesWrittenByUser returns all values written by a given user
func (d *db) GetValuesWrittenByUser(userID string) (*types.GetDataWrittenByResponseEnvelope, error) {
	writtenByUser, err := d.provenanceQueryProcessor.GetValuesWrittenByUser(userID)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, writtenByUser)
	if err != nil {
		return nil, err
	}
	return &types.GetDataWrittenByResponseEnvelope{
		Payload:   writtenByUser,
		Signature: sig,
	}, err
}

// GetValuesDeletedByUser returns all values deleted by a given user
func (d *db) GetValuesDeletedByUser(userID string) (*types.GetDataDeletedByResponseEnvelope, error) {
	deletedByUser, err := d.provenanceQueryProcessor.GetValuesDeletedByUser(userID)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, deletedByUser)
	if err != nil {
		return nil, err
	}
	return &types.GetDataDeletedByResponseEnvelope{
		Payload:   deletedByUser,
		Signature: sig,
	}, err
}

// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
func (d *db) GetReaders(dbName, key string) (*types.GetDataReadersResponseEnvelope, error) {
	readers, err := d.provenanceQueryProcessor.GetReaders(dbName, key)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, readers)
	if err != nil {
		return nil, err
	}
	return &types.GetDataReadersResponseEnvelope{
		Payload:   readers,
		Signature: sig,
	}, err
}

// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
func (d *db) GetWriters(dbName, key string) (*types.GetDataWritersResponseEnvelope, error) {
	writers, err := d.provenanceQueryProcessor.GetWriters(dbName, key)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, writers)
	if err != nil {
		return nil, err
	}
	return &types.GetDataWritersResponseEnvelope{
		Payload:   writers,
		Signature: sig,
	}, err
}

// GetTxIDsSubmittedByUser returns all ids of all transactions submitted by a given user
func (d *db) GetTxIDsSubmittedByUser(userID string) (*types.GetTxIDsSubmittedByResponseEnvelope, error) {
	submittedByUser, err := d.provenanceQueryProcessor.GetTxIDsSubmittedByUser(userID)
	if err != nil {
		return nil, err
	}

	sig, err := cryptoservice.SignQueryResponse(d.signer, submittedByUser)
	if err != nil {
		return nil, err
	}
	return &types.GetTxIDsSubmittedByResponseEnvelope{
		Payload:   submittedByUser,
		Signature: sig,
	}, err
}

// Close closes and release resources used by db
func (d *db) Close() error {
	if err := d.txProcessor.close(); err != nil {
		return errors.WithMessage(err, "error while closing the transaction processor")
	}

	if err := d.db.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the worldstate database")
	}

	if err := d.provenanceStore.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the provenance store")
	}

	if err := d.blockStore.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the block store")
	}

	return nil
}

func prepareConfigTx(conf *config.Configurations) (*types.ConfigTxEnvelope, error) {
	certs, err := readCerts(conf)
	if err != nil {
		return nil, err
	}

	clusterConfig := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				ID:          conf.Node.Identity.ID,
				Certificate: certs.nodeCert,
				Address:     conf.Node.Network.Address,
				Port:        conf.Node.Network.Port,
			},
		},
		Admins: []*types.Admin{
			{
				ID:          conf.Admin.ID,
				Certificate: certs.adminCert,
			},
		},
		CertAuthConfig: certs.caCerts,
	}

	return &types.ConfigTxEnvelope{
		Payload: &types.ConfigTx{
			TxID:      uuid.New().String(), // TODO: we need to change TxID to string
			NewConfig: clusterConfig,
		},
		// TODO: we can make the node itself sign the transaction
	}, nil
}

type certsInGenesisConfig struct {
	nodeCert   []byte
	adminCert  []byte
	rootCACert []byte
	caCerts    *types.CAConfig
}

func readCerts(conf *config.Configurations) (*certsInGenesisConfig, error) {
	certsInGen := &certsInGenesisConfig{caCerts: &types.CAConfig{}}

	nodeCert, err := ioutil.ReadFile(conf.Node.Identity.CertificatePath)
	if err != nil {
		return nil, errors.Wrapf(err, "error while reading node certificate %s", conf.Node.Identity.CertificatePath)
	}
	nodePemCert, _ := pem.Decode(nodeCert)
	certsInGen.nodeCert = nodePemCert.Bytes

	adminCert, err := ioutil.ReadFile(conf.Admin.CertificatePath)
	if err != nil {
		return nil, errors.Wrapf(err, "error while reading admin certificate %s", conf.Admin.CertificatePath)
	}
	adminPemCert, _ := pem.Decode(adminCert)
	certsInGen.adminCert = adminPemCert.Bytes

	for _, certPath := range conf.CAConfig.RootCACertsPath {
		rootCACert, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, errors.Wrapf(err, "error while reading root CA certificate %s", certPath)
		}
		caPemCert, _ := pem.Decode(rootCACert)
		certsInGen.caCerts.Roots = append(certsInGen.caCerts.Roots, caPemCert.Bytes)
	}

	for _, certPath := range conf.CAConfig.IntermediateCACertsPath {
		caCert, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, errors.Wrapf(err, "error while reading intermediate CA certificate %s", certPath)
		}
		caPemCert, _ := pem.Decode(caCert)
		certsInGen.caCerts.Intermediates = append(certsInGen.caCerts.Intermediates, caPemCert.Bytes)
	}

	return certsInGen, nil
}

func createLedgerDir(dir string) error {
	exist, err := fileops.Exists(dir)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}

	return fileops.CreateDir(dir)
}
