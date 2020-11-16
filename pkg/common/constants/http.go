package constants

import (
	"fmt"
	"path"
	"strconv"
)

const (
	UserHeader      = "UserID"
	SignatureHeader = "Signature"

	UserEndpoint = "/user/"
	GetUser      = "/user/{userid}"
	PostUserTx   = "/user/tx"

	DataEndpoint = "/data/"
	GetData      = "/data/{dbname}/{key}"
	PostDataTx   = "/data/tx"

	DBEndpoint  = "/db/"
	GetDBStatus = "/db/{dbname}"
	PostDBTx    = "/db/tx"

	ConfigEndpoint = "/config/"
	PostConfigTx   = "/config/tx"
	GetConfig      = "/config/tx"

	LedgerEndpoint = "/ledger/"
	GetBlockHeader = "/ledger/block/{blockId}"
	GetPath        = "/ledger/path"
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
