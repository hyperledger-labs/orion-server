// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package constants

import (
	"fmt"
	"path"
	"strconv"

	"github.ibm.com/blockchaindb/server/pkg/types"
)

const (
	UserHeader      = "UserID"
	SignatureHeader = "Signature"
	TimeoutHeader   = "TxTimeout"

	UserEndpoint = "/user/"
	GetUser      = "/user/{userid}"
	PostUserTx   = "/user/tx"

	DataEndpoint = "/data/"
	GetData      = "/data/{dbname:" + `[0-9a-zA-Z_\-\.]+` + "}/{key}"
	PostDataTx   = "/data/tx"

	DBEndpoint  = "/db/"
	GetDBStatus = "/db/{dbname:" + `[0-9a-zA-Z_\-\.]+` + "}"
	PostDBTx    = "/db/tx"

	ConfigEndpoint = "/config/"
	PostConfigTx   = "/config/tx"
	GetConfig      = "/config/tx"
	GetNodesConfig = "/config/node"
	GetNodeConfig  = "/config/node/{nodeId}"

	LedgerEndpoint = "/ledger/"
	GetBlockHeader = "/ledger/block/{blockId:[0-9]+}"
	GetPath        = "/ledger/path"
	GetTxProof     = "/ledger/proof/{blockId:[0-9]+}"
	GetTxReceipt   = "/ledger/tx/receipt/{txId}"

	ProvenanceEndpoint      = "/provenance/"
	GetHistoricalData       = "/provenance/data/history/{dbname}/{key}"
	GetDataReaders          = "/provenance/data/readers/{dbname}/{key}"
	GetDataWriters          = "/provenance/data/writers/{dbname}/{key}"
	GetDataReadBy           = "/provenance/data/read/{userId}"
	GetDataWrittenBy        = "/provenance/data/written/{userId}"
	GetDataDeletedBy        = "/provenance/data/deleted/{userId}"
	GetTxIDsSubmittedBy     = "/provenance/data/tx/{userId}"
	GetMostRecentUserOrNode = "/provenance/{type:user|node}/{id}"
)

// URLForGetData returns url for GET request to retrieve
// value of the key present in the dbName
func URLForGetData(dbName, key string) string {
	return DataEndpoint + path.Join(dbName, key)
}

// URLForGetUser returns url for GET request to retrieve
// a user information
func URLForGetUser(userID string) string {
	return UserEndpoint + userID
}

// URLForGetDBStatus returns url for GET request to find
// status of a given database
func URLForGetDBStatus(dbName string) string {
	return DBEndpoint + dbName
}

// URLForGetConfig returns url for GET request to retrieve
// the cluster configuration
func URLForGetConfig() string {
	return GetConfig
}

func URLForLedgerBlock(blockNum uint64) string {
	return LedgerEndpoint + path.Join("block", strconv.FormatUint(blockNum, 10))
}

func URLForLedgerPath(start, end uint64) string {
	return LedgerEndpoint + fmt.Sprintf("path?start=%d&end=%d", start, end)
}

func URLTxProof(blockNum uint64, txIdx int) string {
	return LedgerEndpoint + fmt.Sprintf("proof/%d?idx=%d", blockNum, txIdx)
}

func URLForNodeConfigPath(nodeID string) string {
	return path.Join(GetNodesConfig, nodeID)
}

// URLForGetHistoricalData returns url for GET request to
// retrieve all values associated with a given key on a database
func URLForGetHistoricalData(dbName, key string) string {
	return ProvenanceEndpoint + path.Join("data", "history", dbName, key)
}

// URLForGetHistoricalDeletedData returns url for GET request to
// retrieve all deleted values associated with a given key on a database
func URLForGetHistoricalDeletedData(dbName, key string) string {
	return ProvenanceEndpoint + path.Join("data", "history", dbName, key) + "?onlydeletes=true"
}

// URLForGetHistoricalDataAt returns url for GET request to
// retrieve a value at a particular version for a given key on a database
func URLForGetHistoricalDataAt(dbName, key string, version *types.Version) string {
	return ProvenanceEndpoint + path.Join("data", "history", dbName, key) +
		fmt.Sprintf("?blocknumber=%d&transactionnumber=%d", version.BlockNum, version.TxNum)
}

// URLForGetHistoricalDataAtOrBelow returns url for GET request to
// retrieve a most recent value at a particular version for a given key on a database
func URLForGetHistoricalDataAtOrBelow(dbName, key string, version *types.Version) string {
	return ProvenanceEndpoint + path.Join("data", "history", dbName, key) +
		fmt.Sprintf("?blocknumber=%d&transactionnumber=%d", version.BlockNum, version.TxNum) +
		fmt.Sprintf("&mostrecent=true")
}

// URLForGetPreviousHistoricalData returns url for GET request to
// retrieve previous values for a given key on a database from a particular version
func URLForGetPreviousHistoricalData(dbName, key string, version *types.Version) string {
	return ProvenanceEndpoint + path.Join("data", "history", dbName, key) +
		fmt.Sprintf("?blocknumber=%d&transactionnumber=%d", version.BlockNum, version.TxNum) +
		"&direction=previous"
}

// URLForGetNextHistoricalData returns url for GET request to
// retrieve next values for a given key on a database from a particular version
func URLForGetNextHistoricalData(dbName, key string, version *types.Version) string {
	return ProvenanceEndpoint + path.Join("data", "history", dbName, key) +
		fmt.Sprintf("?blocknumber=%d&transactionnumber=%d", version.BlockNum, version.TxNum) +
		"&direction=next"
}

// URLForGetDataReaders returns url for GET request to
// retrive all users who have read a given key from a database
func URLForGetDataReaders(dbName, key string) string {
	return ProvenanceEndpoint + path.Join("data", "readers", dbName, key)
}

// URLForGetDataWriters returns url for GET request to
// retrive all users who have written a given key from a database
func URLForGetDataWriters(dbName, key string) string {
	return ProvenanceEndpoint + path.Join("data", "writers", dbName, key)
}

// URLForGetDataReadBy returns url for GET request to
// retrieve all data read by a given user
func URLForGetDataReadBy(userID string) string {
	return ProvenanceEndpoint + path.Join("data", "read", userID)
}

// URLForGetDataWrittenBy returns url for GET request to
// retrieve all data written by a given user
func URLForGetDataWrittenBy(userID string) string {
	return ProvenanceEndpoint + path.Join("data", "written", userID)
}

// URLForGetDataDeletedBy returns url for GET request to
// retrieve all data written by a given user
func URLForGetDataDeletedBy(userID string) string {
	return ProvenanceEndpoint + path.Join("data", "deleted", userID)
}

// URLForGetTxIDsSubmittedBy returns url for GET request to
// retrieve all txIDs submitted by a given user
func URLForGetTxIDsSubmittedBy(userID string) string {
	return ProvenanceEndpoint + path.Join("data", "tx", userID)
}

func URLForGetTransactionReceipt(txId string) string {
	return LedgerEndpoint + path.Join("tx", "receipt", txId)
}

func URLForGetMostRecentUserInfo(userID string, version *types.Version) string {
	return ProvenanceEndpoint + path.Join("user", userID) +
		fmt.Sprintf("?blocknumber=%d&transactionnumber=%d", version.BlockNum, version.TxNum)
}

func URLForGetMostRecentNodeConfig(nodeID string, version *types.Version) string {
	return ProvenanceEndpoint + path.Join("node", nodeID) +
		fmt.Sprintf("?blocknumber=%d&transactionnumber=%d", version.BlockNum, version.TxNum)
}
