// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"time"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	mptrieStore "github.com/hyperledger-labs/orion-server/internal/mptrie/store"
	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/certificateauthority"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/marshal"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

//go:generate mockery --dir . --name DB --case underscore --output mocks/

// DB encapsulates functionality required to operate with database state
type DB interface {
	// LedgerHeight returns current height of the ledger
	LedgerHeight() (uint64, error)

	// Height returns ledger height
	Height() (uint64, error)

	// IsLeader returns whether this server is the leader
	IsLeader() *ierrors.NotLeaderError

	// DoesUserExist checks whenever user with given userID exists
	DoesUserExist(userID string) (bool, error)

	// GetCertificate returns the certificate associated with useID, if it exists.
	GetCertificate(userID string) (*x509.Certificate, error)

	// GetUser retrieves user' record
	GetUser(querierUserID, targetUserID string) (*types.GetUserResponseEnvelope, error)

	// GetConfig returns database configuration.
	// Limited access to admins only. Regular users can use the `GetNodeConfig` or `GetClusterStatus` APIs to discover
	// and fetch the details of nodes that are needed for external cluster access.
	GetConfig(querierUserID string) (*types.GetConfigResponseEnvelope, error)

	// GetConfigBlock returns a config block.
	// Only admin users can get a config block.
	// If blockNumber==0, the last config block is returned.
	GetConfigBlock(querierUserID string, blockNumber uint64) (*types.GetConfigBlockResponseEnvelope, error)

	// GetClusterStatus returns the cluster status:
	// - the nodes, as defined in the ClusterConfig, without certificates if `noCert`=true;
	// - the ID of the leader, if it exists;
	// - the IDs of all active nodes, including the leader.
	GetClusterStatus(noCerts bool) (*types.GetClusterStatusResponseEnvelope, error)

	// GetNodeConfig returns single node subsection of database configuration
	GetNodeConfig(nodeID string) (*types.GetNodeConfigResponseEnvelope, error)

	// GetDBStatus returns status for database, checks whenever database was created
	GetDBStatus(dbName string) (*types.GetDBStatusResponseEnvelope, error)

	// GetDBIndex returns the defined index for a given database. Index present in the
	// response envelope is a JSON string.
	GetDBIndex(dbName, querierUserID string) (*types.GetDBIndexResponseEnvelope, error)

	// GetData retrieves values for given key
	GetData(dbName, querierUserID, key string) (*types.GetDataResponseEnvelope, error)

	// GetDataRange retrieves a range of values
	GetDataRange(dbName, querierUserID, startKey, endKey string, limit uint64) (*types.GetDataRangeResponseEnvelope, error)

	// DataQuery executes a given JSON query and return key-value pairs which are matching
	// the criteria provided in the query. The query is a json marshled bytes which needs
	// to contain a top level combinational operator followed by a list of attributes and
	// a list of conditions per attributes. For example, the following is one of the query:
	//
	// {
	//   "selector": {
	// 		"$and": {            -- top level combinational operator
	// 			"attr1": {          -- a field in the json document
	// 				"$gte": "a",    -- value criteria for the field
	// 				"$lt": "b"      -- value criteria for the field
	// 			},
	// 			"attr2": {          -- a field in the json document
	// 				"$eq": true     -- value criteria for the field
	// 			},
	// 			"attr3": {          -- a field in the json document
	// 				"$lt": "a2"     -- a field in the json document
	// 			}
	// 		}
	//   }
	// }
	DataQuery(ctx context.Context, dbName, querierUserID string, query []byte) (*types.DataQueryResponseEnvelope, error)

	// GetBlockHeader returns ledger block header
	GetBlockHeader(userID string, blockNum uint64) (*types.GetBlockResponseEnvelope, error)

	// GetAugmentedBlockHeader returns ledger block header
	GetAugmentedBlockHeader(userID string, blockNum uint64) (*types.GetAugmentedBlockHeaderResponseEnvelope, error)

	// GetTxProof returns intermediate hashes to recalculate merkle tree root from tx hash
	GetTxProof(userID string, blockNum uint64, txIdx uint64) (*types.GetTxProofResponseEnvelope, error)

	// GetDataProof returns hashes path from value to root in merkle-patricia trie
	GetDataProof(userID string, blockNum uint64, dbname string, key string, deleted bool) (*types.GetDataProofResponseEnvelope, error)

	// GetLedgerPath returns list of blocks that forms the shortest path in the skip list chain of the ledger.
	// Parameter 'start' is the block number of the earlier block, 'end' is the block number of the last block. That is
	// 'start'<='end'. The returned path is the shortest path from the 'end' block to the 'start' block.
	GetLedgerPath(userID string, start, end uint64) (*types.GetLedgerPathResponseEnvelope, error)

	// GetValues returns all values associated with a given key
	GetValues(userID, dbName, key string) (*types.GetHistoricalDataResponseEnvelope, error)

	// GetDeletedValues returns all deleted values associated with a given key
	GetDeletedValues(userID, dbname, key string) (*types.GetHistoricalDataResponseEnvelope, error)

	// GetValueAt returns the value of a given key at a particular version
	GetValueAt(userID, dbName, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error)

	// GetMostRecentValueAtOrBelow returns the most recent value of a given key at or below the given version
	GetMostRecentValueAtOrBelow(userID, dbName, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error)

	// GetPreviousValues returns previous values of a given key and a version. The number of records returned would be limited
	// by the limit parameters.
	GetPreviousValues(userID, dbname, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error)

	// GetNextValues returns next values of a given key and a version. The number of records returned would be limited
	// by the limit parameters.
	GetNextValues(userID, dbname, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error)

	// GetValuesReadByUser returns all values read by a given targetUserID
	GetValuesReadByUser(querierUserID, targetUserID string) (*types.GetDataProvenanceResponseEnvelope, error)

	// GetValuesWrittenByUser returns all values written by a targetUserID
	GetValuesWrittenByUser(querierUserID, targetUserID string) (*types.GetDataProvenanceResponseEnvelope, error)

	// GetValuesDeletedByUser returns all values deleted by a targetUserID
	GetValuesDeletedByUser(querierUserID, targetUserID string) (*types.GetDataProvenanceResponseEnvelope, error)

	// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
	GetReaders(userID, dbName, key string) (*types.GetDataReadersResponseEnvelope, error)

	// GetWriters returns all userIDs who have updated a given key as well as the access frequency
	GetWriters(userID, dbName, key string) (*types.GetDataWritersResponseEnvelope, error)

	// GetTxIDsSubmittedByUser returns all ids of all transactions submitted by a targetUserID
	GetTxIDsSubmittedByUser(querierUserID, targetUserID string) (*types.GetTxIDsSubmittedByResponseEnvelope, error)

	// GetTxReceipt returns transaction receipt - block header of ledger block that contains the transaction
	// and transaction index inside the block
	GetTxReceipt(userId string, txID string) (*types.TxReceiptResponseEnvelope, error)

	// SubmitTransaction submits transaction to the database with a timeout. If the timeout is
	// set to 0, the submission would be treated as async while a non-zero timeout would be
	// treated as a sync submission. When a timeout occurs with the sync submission, a
	// timeout error will be returned
	SubmitTransaction(tx interface{}, timeout time.Duration) (*types.TxReceiptResponseEnvelope, error)

	// IsDBExists returns true if database with given name is exists otherwise false
	IsDBExists(name string) bool

	// Close frees and closes resources allocated by database instance
	Close() error
}

//go:generate mockery --dir . --name TxProcessor --case underscore --output mocks/
type TxProcessor interface {
	Close() error
	ClusterStatus() (leader string, active []string)
	IsLeader() *ierrors.NotLeaderError
	SubmitTransaction(tx interface{}, timeout time.Duration) (*types.TxReceiptResponse, error)
}

type db struct {
	nodeID                   string
	worldstateQueryProcessor *worldstateQueryProcessor
	ledgerQueryProcessor     *ledgerQueryProcessor
	provenanceQueryProcessor *provenanceQueryProcessor
	txProcessor              TxProcessor
	db                       worldstate.DB
	blockStore               *blockstore.Store
	provenanceStore          *provenance.Store
	stateTrieStore           *mptrieStore.Store
	signer                   crypto.Signer
	logger                   *logger.SugarLogger
}

// NewDB creates a new database bcdb which handles both the queries and transactions.
func NewDB(conf *config.Configurations, logger *logger.SugarLogger) (DB, error) {
	localConf := conf.LocalConfig
	if localConf.Server.Database.Name != "leveldb" {
		return nil, errors.New("only leveldb is supported as the state database")
	}

	ledgerDir := localConf.Server.Database.LedgerDirectory
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
			Disabled: conf.LocalConfig.Server.Provenance.Disabled,
			Logger:   logger,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating the provenance store")
	}

	stateTrieStore, err := mptrieStore.Open(
		&mptrieStore.Config{
			StoreDir: constructStateTrieStorePath(ledgerDir),
			Logger:   logger,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating the state trie store")
	}

	querier := identity.NewQuerier(levelDB)

	signer, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: localConf.Server.Identity.KeyPath})
	if err != nil {
		return nil, errors.Wrap(err, "can't load private key")
	}

	worldstateQueryProcessor := newWorldstateQueryProcessor(
		&worldstateQueryProcessorConfig{
			nodeID:              localConf.Server.Identity.ID,
			db:                  levelDB,
			queryProcessingConf: &localConf.Server.QueryProcessing,
			blockStore:          blockStore,
			identityQuerier:     querier,
			logger:              logger,
		},
	)

	ledgerQueryProcessorConfig := &ledgerQueryProcessorConfig{
		db:              levelDB,
		blockStore:      blockStore,
		trieStore:       stateTrieStore,
		identityQuerier: querier,
		logger:          logger,
	}
	ledgerQueryProcessor := newLedgerQueryProcessor(ledgerQueryProcessorConfig)

	provenanceQueryProcessor := newProvenanceQueryProcessor(
		&provenanceQueryProcessorConfig{
			provenanceStore: provenanceStore,
			identityQuerier: querier,
			logger:          logger,
		},
	)

	txProcessor, err := newTransactionProcessor(
		&txProcessorConfig{
			config:          conf,
			db:              levelDB,
			blockStore:      blockStore,
			provenanceStore: provenanceStore,
			stateTrieStore:  stateTrieStore,
			logger:          logger,
		},
	)
	if err != nil {
		return nil, errors.WithMessage(err, "can't initiate tx processor")
	}

	return &db{
		nodeID:                   localConf.Server.Identity.ID,
		worldstateQueryProcessor: worldstateQueryProcessor,
		ledgerQueryProcessor:     ledgerQueryProcessor,
		provenanceQueryProcessor: provenanceQueryProcessor,
		txProcessor:              txProcessor,
		db:                       levelDB,
		blockStore:               blockStore,
		provenanceStore:          provenanceStore,
		stateTrieStore:           stateTrieStore,
		logger:                   logger,
		signer:                   signer,
	}, nil
}

// LedgerHeight returns ledger height
func (d *db) LedgerHeight() (uint64, error) {
	return d.worldstateQueryProcessor.blockStore.Height()
}

// Height returns ledger height
func (d *db) Height() (uint64, error) {
	return d.worldstateQueryProcessor.db.Height()
}

// IsLeader returns whether the current node is a leader
func (d *db) IsLeader() *ierrors.NotLeaderError {
	return d.txProcessor.IsLeader()
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

	userResponse.Header = d.responseHeader()
	sign, err := d.signature(userResponse)
	if err != nil {
		return nil, err
	}

	return &types.GetUserResponseEnvelope{
		Response:  userResponse,
		Signature: sign,
	}, nil
}

// GetNodeConfig returns single node subsection of database configuration
func (d *db) GetNodeConfig(nodeID string) (*types.GetNodeConfigResponseEnvelope, error) {
	nodeConfigResponse, err := d.worldstateQueryProcessor.getNodeConfig(nodeID)
	if err != nil {
		return nil, err
	}

	nodeConfigResponse.Header = d.responseHeader()
	sign, err := d.signature(nodeConfigResponse)
	if err != nil {
		return nil, err
	}

	return &types.GetNodeConfigResponseEnvelope{
		Response:  nodeConfigResponse,
		Signature: sign,
	}, nil
}

// GetConfig returns database configuration.
// Limited access to admins only. Regular users can use the `GetNodeConfig` or `GetClusterStatus` APIs to discover
// and fetch the details of nodes that are needed for external cluster access.
func (d *db) GetConfig(querierUserID string) (*types.GetConfigResponseEnvelope, error) {
	configResponse, err := d.worldstateQueryProcessor.getConfig(querierUserID)
	if err != nil {
		return nil, err
	}

	configResponse.Header = d.responseHeader()
	sign, err := d.signature(configResponse)
	if err != nil {
		return nil, err
	}

	return &types.GetConfigResponseEnvelope{
		Response:  configResponse,
		Signature: sign,
	}, nil
}

// GetConfigBlock retrieves a numbered config block. If blockNumber==0 the latest valid block is returned. If the
// blockNumber does not identify a config-block, and error is returned.
func (d *db) GetConfigBlock(querierUserID string, blockNumber uint64) (*types.GetConfigBlockResponseEnvelope, error) {
	configBlockResponse, err := d.worldstateQueryProcessor.getConfigBlock(querierUserID, blockNumber)
	if err != nil {
		return nil, err
	}

	configBlockResponse.Header = d.responseHeader()
	sign, err := d.signature(configBlockResponse)
	if err != nil {
		return nil, err
	}

	return &types.GetConfigBlockResponseEnvelope{
		Response:  configBlockResponse,
		Signature: sign,
	}, nil
}

// GetClusterStatus returns the cluster status
func (d *db) GetClusterStatus(noCerts bool) (*types.GetClusterStatusResponseEnvelope, error) {
	nodes, metadata, err := d.worldstateQueryProcessor.getNodeConfigAndMetadata()
	if err != nil {
		return nil, err
	}

	clusterStatusResponse := &types.GetClusterStatusResponse{
		Nodes:   nodes,
		Version: metadata.GetVersion(),
	}

	leader, active := d.txProcessor.ClusterStatus()

	// The configResponse is from the world-state-db, whereas the (leader, active) pair is from the block-replicator.
	// Since they may be out of sync (if the request hits right in the middle of a config change), we make sure the
	// response is self-consistent.
	for _, activeID := range active {
		for _, node := range nodes {
			if activeID == node.Id {
				clusterStatusResponse.Active = append(clusterStatusResponse.Active, activeID)
				break
			}
		}
	}
	for _, node := range nodes {
		if leader == node.Id {
			clusterStatusResponse.Leader = leader
			break
		}
	}

	if noCerts {
		for i := 0; i < len(clusterStatusResponse.Nodes); i++ {
			clusterStatusResponse.Nodes[i].Certificate = nil
		}
	}

	clusterStatusResponse.Header = d.responseHeader()
	sign, err := d.signature(clusterStatusResponse)
	if err != nil {
		return nil, err
	}

	return &types.GetClusterStatusResponseEnvelope{
		Response:  clusterStatusResponse,
		Signature: sign,
	}, nil
}

// GetDBStatus returns database status
func (d *db) GetDBStatus(dbName string) (*types.GetDBStatusResponseEnvelope, error) {
	dbStatusResponse, err := d.worldstateQueryProcessor.getDBStatus(dbName)
	if err != nil {
		return nil, err
	}

	dbStatusResponse.Header = d.responseHeader()
	sign, err := d.signature(dbStatusResponse)
	if err != nil {
		return nil, err
	}

	return &types.GetDBStatusResponseEnvelope{
		Response:  dbStatusResponse,
		Signature: sign,
	}, nil
}

// GetDBIndex returns the defined index of a given database
func (d *db) GetDBIndex(dbName, querierUserID string) (*types.GetDBIndexResponseEnvelope, error) {
	dbIndexResponse, err := d.worldstateQueryProcessor.getDBIndex(dbName, querierUserID)
	if err != nil {
		return nil, err
	}

	dbIndexResponse.Header = d.responseHeader()
	sign, err := d.signature(dbIndexResponse)
	if err != nil {
		return nil, err
	}

	return &types.GetDBIndexResponseEnvelope{
		Response:  dbIndexResponse,
		Signature: sign,
	}, nil
}

// SubmitTransaction submits transaction to the database with a timeout. If the timeout is
// set to 0, the submission would be treated as async while a non-zero timeout would be
// treated as a sync submission. When a timeout occurs with the sync submission, a
// timeout error will be returned
func (d *db) SubmitTransaction(tx interface{}, timeout time.Duration) (*types.TxReceiptResponseEnvelope, error) {
	receipt, err := d.txProcessor.SubmitTransaction(tx, timeout)
	if err != nil {
		return nil, err
	}

	receipt.Header = d.responseHeader()
	sign, err := d.signature(receipt)
	if err != nil {
		return nil, err
	}

	return &types.TxReceiptResponseEnvelope{
		Response:  receipt,
		Signature: sign,
	}, nil
}

// GetData returns value for provided key
func (d *db) GetData(dbName, querierUserID, key string) (*types.GetDataResponseEnvelope, error) {
	dataResponse, err := d.worldstateQueryProcessor.getData(dbName, querierUserID, key)
	if err != nil {
		return nil, err
	}

	dataResponse.Header = d.responseHeader()
	sign, err := d.signature(dataResponse)
	if err != nil {
		return nil, err
	}

	return &types.GetDataResponseEnvelope{
		Response:  dataResponse,
		Signature: sign,
	}, nil
}

// GetDataRange returns a range of values starting from the start key and till before the end key
func (d *db) GetDataRange(dbName, querierUserID, startKey, endKey string, limit uint64) (*types.GetDataRangeResponseEnvelope, error) {
	dataResponse, err := d.worldstateQueryProcessor.getDataRange(dbName, querierUserID, startKey, endKey, limit)
	if err != nil {
		return nil, err
	}

	dataResponse.Header = d.responseHeader()
	sign, err := d.signature(dataResponse)
	if err != nil {
		return nil, err
	}

	return &types.GetDataRangeResponseEnvelope{
		Response:  dataResponse,
		Signature: sign,
	}, nil
}

// DataQuery executes a given JSON query and return key-value pairs which are matching
// the criteria provided in the query
func (d *db) DataQuery(ctx context.Context, dbName, querierUserID string, query []byte) (*types.DataQueryResponseEnvelope, error) {
	queryResponse, err := d.worldstateQueryProcessor.executeJSONQuery(ctx, dbName, querierUserID, query)

	select {
	case <-ctx.Done():
		return nil, nil
	default:
		if err != nil {
			return nil, err
		}
		queryResponse.Header = d.responseHeader()
		sign, err := d.signature(queryResponse)
		if err != nil {
			return nil, err
		}

		return &types.DataQueryResponseEnvelope{
			Response:  queryResponse,
			Signature: sign,
		}, nil
	}

}

func (d *db) IsDBExists(name string) bool {
	return d.worldstateQueryProcessor.isDBExists(name)
}

func (d *db) GetBlockHeader(userID string, blockNum uint64) (*types.GetBlockResponseEnvelope, error) {
	blockHeader, err := d.ledgerQueryProcessor.getBlockHeader(userID, blockNum)
	if err != nil {
		return nil, err
	}

	blockHeader.Header = d.responseHeader()
	sign, err := d.signature(blockHeader)
	if err != nil {
		return nil, err
	}

	return &types.GetBlockResponseEnvelope{
		Response:  blockHeader,
		Signature: sign,
	}, nil
}

func (d *db) GetAugmentedBlockHeader(userID string, blockNum uint64) (*types.GetAugmentedBlockHeaderResponseEnvelope, error) {
	blockHeader, err := d.ledgerQueryProcessor.getAugmentedBlockHeader(userID, blockNum)
	if err != nil {
		return nil, err
	}

	blockHeader.Header = d.responseHeader()
	sign, err := d.signature(blockHeader)
	if err != nil {
		return nil, err
	}

	return &types.GetAugmentedBlockHeaderResponseEnvelope{
		Response:  blockHeader,
		Signature: sign,
	}, nil
}

func (d *db) GetTxProof(userID string, blockNum uint64, txIdx uint64) (*types.GetTxProofResponseEnvelope, error) {
	proofResponse, err := d.ledgerQueryProcessor.getTxProof(userID, blockNum, txIdx)
	if err != nil {
		return nil, err
	}

	proofResponse.Header = d.responseHeader()
	sign, err := d.signature(proofResponse)
	if err != nil {
		return nil, err
	}

	return &types.GetTxProofResponseEnvelope{
		Response:  proofResponse,
		Signature: sign,
	}, nil
}

func (d *db) GetDataProof(userID string, blockNum uint64, dbname string, key string, deleted bool) (*types.GetDataProofResponseEnvelope, error) {
	proofResponse, err := d.ledgerQueryProcessor.getDataProof(userID, blockNum, dbname, key, deleted)
	if err != nil {
		return nil, err
	}

	proofResponse.Header = d.responseHeader()
	sign, err := d.signature(proofResponse)
	if err != nil {
		return nil, err
	}

	return &types.GetDataProofResponseEnvelope{
		Response:  proofResponse,
		Signature: sign,
	}, nil
}

func (d *db) GetLedgerPath(userID string, start, end uint64) (*types.GetLedgerPathResponseEnvelope, error) {
	pathResponse, err := d.ledgerQueryProcessor.getPath(userID, start, end)
	if err != nil {
		return nil, err
	}

	pathResponse.Header = d.responseHeader()
	sign, err := d.signature(pathResponse)
	if err != nil {
		return nil, err
	}

	return &types.GetLedgerPathResponseEnvelope{
		Response:  pathResponse,
		Signature: sign,
	}, nil
}

func (d *db) GetTxReceipt(userId string, txID string) (*types.TxReceiptResponseEnvelope, error) {
	receiptResponse, err := d.ledgerQueryProcessor.getTxReceipt(userId, txID)
	if err != nil {
		return nil, err
	}

	receiptResponse.Header = d.responseHeader()
	sign, err := d.signature(receiptResponse)
	if err != nil {
		return nil, err
	}

	return &types.TxReceiptResponseEnvelope{
		Response:  receiptResponse,
		Signature: sign,
	}, nil
}

// GetValues returns all values associated with a given key
func (d *db) GetValues(userID, dbName, key string) (*types.GetHistoricalDataResponseEnvelope, error) {
	values, err := d.provenanceQueryProcessor.GetValues(userID, dbName, key)
	if err != nil {
		return nil, err
	}

	values.Header = d.responseHeader()
	sign, err := d.signature(values)
	if err != nil {
		return nil, err
	}

	return &types.GetHistoricalDataResponseEnvelope{
		Response:  values,
		Signature: sign,
	}, nil
}

// GetDeletedValues returns all deleted values associated with a given key
func (d *db) GetDeletedValues(userID, dbName, key string) (*types.GetHistoricalDataResponseEnvelope, error) {
	deletedValues, err := d.provenanceQueryProcessor.GetDeletedValues(userID, dbName, key)
	if err != nil {
		return nil, err
	}

	deletedValues.Header = d.responseHeader()
	sign, err := d.signature(deletedValues)
	if err != nil {
		return nil, err
	}

	return &types.GetHistoricalDataResponseEnvelope{
		Response:  deletedValues,
		Signature: sign,
	}, nil
}

// GetValueAt returns the value of a given key at a particular version
func (d *db) GetValueAt(userID, dbName, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error) {
	valueAt, err := d.provenanceQueryProcessor.GetValueAt(userID, dbName, key, version)
	if err != nil {
		return nil, err
	}

	valueAt.Header = d.responseHeader()
	sign, err := d.signature(valueAt)
	if err != nil {
		return nil, err
	}

	return &types.GetHistoricalDataResponseEnvelope{
		Response:  valueAt,
		Signature: sign,
	}, nil
}

// GetMostRecentValueAtOrBelow returns the most recent value of a given key at or below the given version
func (d *db) GetMostRecentValueAtOrBelow(userID, dbName, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error) {
	valueAt, err := d.provenanceQueryProcessor.GetMostRecentValueAtOrBelow(userID, dbName, key, version)
	if err != nil {
		return nil, err
	}

	valueAt.Header = d.responseHeader()
	sign, err := d.signature(valueAt)
	if err != nil {
		return nil, err
	}

	return &types.GetHistoricalDataResponseEnvelope{
		Response:  valueAt,
		Signature: sign,
	}, nil
}

// GetPreviousValues returns previous values of a given key and a version. The number of records returned would be limited
// by the limit parameters.
func (d *db) GetPreviousValues(userID, dbName, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error) {
	previousValues, err := d.provenanceQueryProcessor.GetPreviousValues(userID, dbName, key, version)
	if err != nil {
		return nil, err
	}

	previousValues.Header = d.responseHeader()
	sign, err := d.signature(previousValues)
	if err != nil {
		return nil, err
	}

	return &types.GetHistoricalDataResponseEnvelope{
		Response:  previousValues,
		Signature: sign,
	}, nil
}

// GetNextValues returns next values of a given key and a version. The number of records returned would be limited
// by the limit parameters.
func (d *db) GetNextValues(userID, dbName, key string, version *types.Version) (*types.GetHistoricalDataResponseEnvelope, error) {
	nextValues, err := d.provenanceQueryProcessor.GetNextValues(userID, dbName, key, version)
	if err != nil {
		return nil, err
	}

	nextValues.Header = d.responseHeader()
	sign, err := d.signature(nextValues)
	if err != nil {
		return nil, err
	}

	return &types.GetHistoricalDataResponseEnvelope{
		Response:  nextValues,
		Signature: sign,
	}, nil
}

// GetValuesReadByUser returns all values read by a given targetUserID
func (d *db) GetValuesReadByUser(querierUserID, targetUserID string) (*types.GetDataProvenanceResponseEnvelope, error) {
	readByUser, err := d.provenanceQueryProcessor.GetValuesReadByUser(querierUserID, targetUserID)
	if err != nil {
		return nil, err
	}

	readByUser.Header = d.responseHeader()
	sign, err := d.signature(readByUser)
	if err != nil {
		return nil, err
	}

	return &types.GetDataProvenanceResponseEnvelope{
		Response:  readByUser,
		Signature: sign,
	}, nil
}

// GetValuesWrittenByUser returns all values written by a given targetUserID
func (d *db) GetValuesWrittenByUser(querierUserID, targetUserID string) (*types.GetDataProvenanceResponseEnvelope, error) {
	writtenByUser, err := d.provenanceQueryProcessor.GetValuesWrittenByUser(querierUserID, targetUserID)
	if err != nil {
		return nil, err
	}

	writtenByUser.Header = d.responseHeader()
	sign, err := d.signature(writtenByUser)
	if err != nil {
		return nil, err
	}

	return &types.GetDataProvenanceResponseEnvelope{
		Response:  writtenByUser,
		Signature: sign,
	}, nil
}

// GetValuesDeletedByUser returns all values deleted by a given targetUserID
func (d *db) GetValuesDeletedByUser(querierUserID, targetUserID string) (*types.GetDataProvenanceResponseEnvelope, error) {
	deletedByUser, err := d.provenanceQueryProcessor.GetValuesDeletedByUser(querierUserID, targetUserID)
	if err != nil {
		return nil, err
	}

	deletedByUser.Header = d.responseHeader()
	sign, err := d.signature(deletedByUser)
	if err != nil {
		return nil, err
	}

	return &types.GetDataProvenanceResponseEnvelope{
		Response:  deletedByUser,
		Signature: sign,
	}, nil
}

// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
func (d *db) GetReaders(userID, dbName, key string) (*types.GetDataReadersResponseEnvelope, error) {
	readers, err := d.provenanceQueryProcessor.GetReaders(userID, dbName, key)
	if err != nil {
		return nil, err
	}

	readers.Header = d.responseHeader()
	sign, err := d.signature(readers)
	if err != nil {
		return nil, err
	}

	return &types.GetDataReadersResponseEnvelope{
		Response:  readers,
		Signature: sign,
	}, nil
}

// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
func (d *db) GetWriters(userID, dbName, key string) (*types.GetDataWritersResponseEnvelope, error) {
	writers, err := d.provenanceQueryProcessor.GetWriters(userID, dbName, key)
	if err != nil {
		return nil, err
	}

	writers.Header = d.responseHeader()
	sign, err := d.signature(writers)
	if err != nil {
		return nil, err
	}

	return &types.GetDataWritersResponseEnvelope{
		Response:  writers,
		Signature: sign,
	}, nil
}

// GetTxIDsSubmittedByUser returns all ids of all transactions submitted by a given targetUserID
func (d *db) GetTxIDsSubmittedByUser(querierUserID, targetUserID string) (*types.GetTxIDsSubmittedByResponseEnvelope, error) {
	submittedByUser, err := d.provenanceQueryProcessor.GetTxIDsSubmittedByUser(querierUserID, targetUserID)
	if err != nil {
		return nil, err
	}

	submittedByUser.Header = d.responseHeader()
	sign, err := d.signature(submittedByUser)
	if err != nil {
		return nil, err
	}

	return &types.GetTxIDsSubmittedByResponseEnvelope{
		Response:  submittedByUser,
		Signature: sign,
	}, nil
}

// Close closes and release resources used by db
func (d *db) Close() error {
	if err := d.txProcessor.Close(); err != nil {
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

	if err := d.stateTrieStore.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the block store")
	}

	d.logger.Info("Closed internal DB")
	return nil
}

func (d *db) responseHeader() *types.ResponseHeader {
	return &types.ResponseHeader{
		NodeId: d.nodeID,
	}
}

func (d *db) signature(response interface{}) ([]byte, error) {
	responseBytes, err := marshal.DefaultMarshaler().Marshal(response.(proto.Message))
	if err != nil {
		return nil, err
	}

	return d.signer.Sign(responseBytes)
}

type certsInGenesisConfig struct {
	nodeCertificates map[string][]byte
	adminCert        []byte
	caCerts          *types.CAConfig
}

func readCerts(conf *config.Configurations) (*certsInGenesisConfig, error) {
	certsInGen := &certsInGenesisConfig{
		nodeCertificates: make(map[string][]byte),
	}

	for _, node := range conf.SharedConfig.Nodes {
		nodeCert, err := ioutil.ReadFile(node.CertificatePath)
		if err != nil {
			return nil, errors.Wrapf(err, "error while reading node certificate: %s", node.CertificatePath)
		}
		nodePemCert, _ := pem.Decode(nodeCert)
		certsInGen.nodeCertificates[node.NodeID] = nodePemCert.Bytes
	}

	adminCert, err := ioutil.ReadFile(conf.SharedConfig.Admin.CertificatePath)
	if err != nil {
		return nil, errors.Wrapf(err, "error while reading admin certificate %s", conf.SharedConfig.Admin.CertificatePath)
	}
	adminPemCert, _ := pem.Decode(adminCert)
	certsInGen.adminCert = adminPemCert.Bytes

	certsInGen.caCerts, err = certificateauthority.LoadCAConfig(&conf.SharedConfig.CAConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "error while loading CA certificates from: %+v", conf.SharedConfig.CAConfig)
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
