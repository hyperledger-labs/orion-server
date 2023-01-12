// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package constants

import (
	"encoding/base64"
	"fmt"
	"path"
	"regexp"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

var validURLSegmentNZ *regexp.Regexp

func init() {
	// from https://www.ietf.org/rfc/rfc3986.txt
	// segment-nz  = pchar*1
	// pchar       = unreserved / pct-encoded / sub-delims / ":" / "@"
	// unreserved  = ALPHA / DIGIT / "-" / "." / "_" / "~"
	// pct-encoded = % HEXDIGIT HEXDIGIT
	// sub-delims  = "!" / "$" / "&" / "'" / "(" / ")" / "*" / "+" / "," / ";" / "="
	unReserved := `[-\._~[:alnum:]]`
	pctEncoded := "%[[:xdigit:]]{2}"
	subDelim := `[!\$&'\(\)\*\+,;=]`
	plus := `[:@]`
	validURLSegmentNZ = regexp.MustCompile(`^(` + unReserved + `|` + pctEncoded + `|` + subDelim + `|` + plus + `)+$`)
}

const (
	UserHeader      = "UserID"
	SignatureHeader = "Signature"
	TimeoutHeader   = "TxTimeout"

	UserEndpoint = "/user/"
	GetUser      = "/user/{userid}"
	PostUserTx   = "/user/tx"

	DataEndpoint = "/data/"
	// GetData Keys in URLs are expected to be encoded in base64 URL encoding without padding.
	GetData       = "/data/{dbname:" + `[0-9a-zA-Z_\-\.]+` + "}/{key}"
	GetDataRange  = "/data/{dbname:" + `[0-9a-zA-Z_\-\.]+` + "}"
	PostDataTx    = "/data/tx"
	PostDataQuery = "/data/{dbname:" + `[0-9a-zA-Z_\-\.]+` + "}/jsonquery"

	DBEndpoint  = "/db/"
	GetDBStatus = "/db/{dbname:" + `[0-9a-zA-Z_\-\.]+` + "}"
	GetDBIndex  = "/db/index/{dbname:" + `[0-9a-zA-Z_\-\.]+` + "}"
	PostDBTx    = "/db/tx"

	ConfigEndpoint     = "/config/"
	PostConfigTx       = "/config/tx"
	GetConfig          = "/config/tx"
	GetNodeConfigPath  = "/config/node"
	GetNodeConfig      = "/config/node/{nodeId}"
	GetLastConfigBlock = "/config/block/last"
	GetClusterStatus   = "/config/cluster"

	LedgerEndpoint     = "/ledger/"
	GetBlockHeader     = "/ledger/block/{blockId:[0-9]+}"
	GetLastBlockHeader = "/ledger/block/last"
	GetPath            = "/ledger/path"
	GetTxProofPrefix   = "/ledger/proof/tx"
	GetTxProof         = "/ledger/proof/tx/{blockId:[0-9]+}"
	GetDataProofPrefix = "/ledger/proof/data"
	// GetDataProof Keys in URLs are expected to be encoded in base64 URL encoding without padding.
	GetDataProof       = "/ledger/proof/data/{dbname:" + `[0-9a-zA-Z_\-\.]+` + "}/{key}"
	GetTxReceipt       = "/ledger/tx/receipt/{txId}"
	GetTxContentPrefix = "/ledger/tx/content"
	GetTxContent       = "/ledger/tx/content/{blockId:[0-9]+}"

	ProvenanceEndpoint = "/provenance/"
	// GetHistoricalData Keys in URLs are expected to be encoded in base64 URL encoding without padding.
	GetHistoricalData = "/provenance/data/history/{dbname}/{key}"
	// GetDataReaders Keys in URLs are expected to be encoded in base64 URL encoding without padding.
	GetDataReaders = "/provenance/data/readers/{dbname}/{key}"
	// GetDataWriters Keys in URLs are expected to be encoded in base64 URL encoding without padding.
	GetDataWriters          = "/provenance/data/writers/{dbname}/{key}"
	GetDataReadBy           = "/provenance/data/read/{userId}"
	GetDataWrittenBy        = "/provenance/data/written/{userId}"
	GetDataDeletedBy        = "/provenance/data/deleted/{userId}"
	GetTxIDsSubmittedBy     = "/provenance/data/tx/{userId}"
	GetMostRecentUserOrNode = "/provenance/{type:user|node}/{id}"
)

// URLForGetData returns url for GET request to retrieve value of the key present in the dbName.
// Keys in URLs are expected to be encoded in base64 URL encoding without padding.
func URLForGetData(dbName, key string) string {
	base64urlKey := base64.RawURLEncoding.EncodeToString([]byte(key))
	return DataEndpoint + path.Join(dbName, base64urlKey)
}

// URLForGetDataRange returns url for GET request to retrieve a range of values.
// Keys in URLs are encoded in base64 URL encoding without padding.
func URLForGetDataRange(dbName, startKey, endKey string, limit uint64) string {
	base64urlStartKey := base64.RawURLEncoding.EncodeToString([]byte(startKey))
	base64urlEndKey := base64.RawURLEncoding.EncodeToString([]byte(endKey))
	return path.Join(DataEndpoint, dbName) +
		fmt.Sprintf("?startkey=%s&endkey=%s&limit=%d", base64urlStartKey, base64urlEndKey, limit)
}

// URLForJSONQuery returns url for GET request to retrieve
// key-value pairs present in the dbName which are matching the
// given JSON query criteria
func URLForJSONQuery(dbName string) string {
	return DataEndpoint + path.Join(dbName, "jsonquery")
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

// URLForGetDBIndex returns url for GET request to retrieve
// the index definition of a given database
func URLForGetDBIndex(dbName string) string {
	return DBEndpoint + "index/" + dbName
}

// URLForGetConfig returns url for GET request to retrieve
// the cluster configuration
func URLForGetConfig() string {
	return GetConfig
}

func URLForLedgerBlock(blockNum uint64, augmented bool) string {
	if augmented {
		return LedgerEndpoint + fmt.Sprintf("block/%d?augmented=%t", blockNum, augmented)
	}
	return LedgerEndpoint + fmt.Sprintf("block/%d", blockNum)
}

func URLForLastLedgerBlock() string {
	return GetLastBlockHeader
}

func URLForLedgerPath(start, end uint64) string {
	return LedgerEndpoint + fmt.Sprintf("path?start=%d&end=%d", start, end)
}

func URLTxProof(blockNum uint64, txIdx uint64) string {
	return LedgerEndpoint + fmt.Sprintf("proof/tx/%d?idx=%d", blockNum, txIdx)
}

func URLTxContent(blockNum uint64, txIdx uint64) string {
	return LedgerEndpoint + fmt.Sprintf("tx/content/%d?idx=%d", blockNum, txIdx)
}

// URLDataProof returns URL for GET request to retrieve a data existence proof.
// Keys in URLs are encoded in base64 URL encoding without padding.
func URLDataProof(blockNum uint64, dbname, key string, deleted bool) string {
	base64urlKey := base64.RawURLEncoding.EncodeToString([]byte(key))
	if deleted {
		return LedgerEndpoint + fmt.Sprintf("proof/data/%s/%s?block=%d&deleted=%t", dbname, base64urlKey, blockNum, deleted)
	}
	return LedgerEndpoint + fmt.Sprintf("proof/data/%s/%s?block=%d", dbname, base64urlKey, blockNum)
}

func URLForNodeConfigPath(nodeID string) string {
	return path.Join(GetNodeConfigPath, nodeID)
}

// URLForGetHistoricalData returns url for GET request to retrieve all values associated with a given key on a database.
// Keys in URLs are encoded in base64 URL encoding without padding.
func URLForGetHistoricalData(dbName, key string) string {
	base64urlKey := base64.RawURLEncoding.EncodeToString([]byte(key))
	return ProvenanceEndpoint + path.Join("data", "history", dbName, base64urlKey)
}

// URLForGetHistoricalDeletedData returns url for GET request to retrieve all deleted values associated with a given
// key on a database.
// Keys in URLs are encoded in base64 URL encoding without padding.
func URLForGetHistoricalDeletedData(dbName, key string) string {
	return URLForGetHistoricalData(dbName, key) + "?onlydeletes=true"
}

// URLForGetHistoricalDataAt returns url for GET request to retrieve a value at a particular version for a given key on
// a database.
// Keys in URLs are encoded in base64 URL encoding without padding.
func URLForGetHistoricalDataAt(dbName, key string, version *types.Version) string {
	return URLForGetHistoricalData(dbName, key) +
		fmt.Sprintf("?blocknumber=%d&transactionnumber=%d", version.BlockNum, version.TxNum)
}

// URLForGetHistoricalDataAtOrBelow returns url for GET request to
// retrieve a most recent value at a particular version for a given key on a database
// Keys in URLs are encoded in base64 URL encoding without padding.
func URLForGetHistoricalDataAtOrBelow(dbName, key string, version *types.Version) string {
	return URLForGetHistoricalData(dbName, key) +
		fmt.Sprintf("?blocknumber=%d&transactionnumber=%d", version.BlockNum, version.TxNum) +
		fmt.Sprintf("&mostrecent=true")
}

// URLForGetPreviousHistoricalData returns url for GET request to
// retrieve previous values for a given key on a database from a particular version
// Keys in URLs are encoded in base64 URL encoding without padding.
func URLForGetPreviousHistoricalData(dbName, key string, version *types.Version) string {
	return URLForGetHistoricalData(dbName, key) +
		fmt.Sprintf("?blocknumber=%d&transactionnumber=%d", version.BlockNum, version.TxNum) +
		"&direction=previous"
}

// URLForGetNextHistoricalData returns url for GET request to
// retrieve next values for a given key on a database from a particular version
// Keys in URLs are encoded in base64 URL encoding without padding.
func URLForGetNextHistoricalData(dbName, key string, version *types.Version) string {
	return URLForGetHistoricalData(dbName, key) +
		fmt.Sprintf("?blocknumber=%d&transactionnumber=%d", version.BlockNum, version.TxNum) +
		"&direction=next"
}

// URLForGetDataReaders returns url for GET request to retrieve all users who have read a given key from a database.
// Keys in URLs are encoded in base64 URL encoding without padding.
func URLForGetDataReaders(dbName, key string) string {
	base64urlKey := base64.RawURLEncoding.EncodeToString([]byte(key))
	return ProvenanceEndpoint + path.Join("data", "readers", dbName, base64urlKey)
}

// URLForGetDataWriters returns url for GET request to retrieve all users who have written a given key from a database.
// Keys in URLs are encoded in base64 URL encoding without padding.
func URLForGetDataWriters(dbName, key string) string {
	base64urlKey := base64.RawURLEncoding.EncodeToString([]byte(key))
	return ProvenanceEndpoint + path.Join("data", "writers", dbName, base64urlKey)
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

// SafeURLSegmentNZ checks that the string `s` is safe to use as a URL segment-nz.
// For example: `http://example.com:8080/tx/my-id`, for s="my-id".
// See: `https://www.ietf.org/rfc/rfc3986.txt`.
func SafeURLSegmentNZ(s string) error {
	if !validURLSegmentNZ.MatchString(s) {
		return errors.Errorf("un-safe for a URL segment: %q", s)
	}
	return nil
}
