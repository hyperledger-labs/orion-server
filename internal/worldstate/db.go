// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package worldstate

import (
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

const (
	// UsersDBName holds all users information
	UsersDBName = "_users"
	// DatabasesDBName holds the name of all user databases
	DatabasesDBName = "_dbs"
	// ConfigDBName holds the name of the database that holds
	// the configuration details
	ConfigDBName = "_config"
	// MetadataDBName holds the name of the database that holds
	// the metadata about the worldstate database
	MetadataDBName = "_metadata"
	// DefaultDBName is the default database created during
	// node bootstrap
	DefaultDBName = "bdb"
	// ConfigKey holds the name of the key in the ConfigDB that
	// stores the cluster configuration
	ConfigKey = "config"
	// AllowedCharsInDBName holds the regexp for allowed characters
	// in a database name
	AllowedCharsInDBName = `^[0-9a-zA-Z_-.]+$`
)

// DB provides method to create and access states stored in
// a database.
type DB interface {
	// Exist returns true if the database exist
	Exist(dbName string) bool
	// ListDBs lists all user databases (excludes system
	// databases)
	ListDBs() []string
	// Get returns the value of the key present in the
	// database
	Get(dbName, key string) ([]byte, *types.Metadata, error)
	// GetVersion returns the version of the key present
	// in the database
	GetVersion(dbName, key string) (*types.Version, error)
	// GetACL returns the access control rule for the given
	// key
	GetACL(dbName, key string) (*types.AccessControl, error)
	// Has returns true if the key exist in the database
	Has(dbName, key string) (bool, error)
	// GetConfig returns the cluster configuration
	GetConfig() (*types.ClusterConfig, *types.Metadata, error)
	// GetIndexDefinition returns the index definition of a given database
	GetIndexDefinition(dbName string) ([]byte, *types.Metadata, error)
	// GetIterator returns an iterator to fetch values associated with a range of keys
	// startKey is inclusive while the endKey is exclusive. An empty startKey (i.e., "") denotes that
	// the caller wants from the first key in the database (lexicographic order). An empty
	// endKey (i.e., "") denotes that the caller wants till the last key in the database (lexicographic order).
	GetIterator(dbName string, startKey, endKey string) (Iterator, error)
	// GetDBsSnapshot returns a latest snapshot of the given DB along with all system databases.
	// A snapshot is a frozen snapshot of a DB state at a particular point in time.
	// The content of snapshot are guaranteed to be consistent.
	// The snapshot must be released after use, by calling Release method on the DBSnapshot.
	GetDBsSnapshot(dbNames []string) (DBsSnapshot, error)
	// Commit commits the updates to each database
	Commit(dbsUpdates map[string]*DBUpdates, blockNumber uint64) error
	// Height returns the state database block height. In other
	// words, it returns the last committed block number
	Height() (uint64, error)
	// ValidDBName returns true if the given dbName is valid
	ValidDBName(dbName string) bool
	// Close closes the DB instance
	Close() error
}

// DBsSnapshot provides methods to read from a database snapshot
type DBsSnapshot interface {
	// Get returns the value of the key present in the
	// database
	Get(dbName, key string) ([]byte, *types.Metadata, error)
	// GetIndexDefinition returns the index definition of a given database
	GetIndexDefinition(dbName string) ([]byte, *types.Metadata, error)
	// GetIterator returns an iterator to fetch values associated with a range of keys
	// startKey is inclusive while the endKey is exclusive. An empty startKey (i.e., "") denotes that
	// the caller wants from the first key in the database (lexicographic order). An empty
	// endKey (i.e., "") denotes that the caller wants till the last key in the database (lexicographic order).
	GetIterator(dbName string, startKey, endKey string) (Iterator, error)
	// Release releases the snapshot. This will not release any returned
	// iterators, the iterators would still be valid until released or the
	// underlying DB is closed.
	// Other methods should not be called after the snapshot has been released.
	Release()
}

// KVWithMetadata holds a key and value pair
type KVWithMetadata struct {
	Key      string
	Value    []byte
	Metadata *types.Metadata
}

// DBUpdates holds writes of KV pairs and deletes of
// keys for each database
type DBUpdates struct {
	Writes  []*KVWithMetadata
	Deletes []string
}

// Iterator provides methods to fetch a range of key-value pairs
type Iterator interface {
	// Key returns the key of the current key/value pair, or nil if done.
	// The caller should not modify the contents of the returned slice, and
	// its contents may change on the next call to 'Next()'.
	Key() []byte
	// Value returns the value and metadata of the current key/value
	// pair in bytes, nil if done.
	// The caller should not modify the contents of the returned slice, and
	// its contents may change on the next call to 'Next()'. To get the
	// exact `Value` and `Metadata` from the returned bytes, caller should
	// unmarshal the bytes to types.ValueWithMetadata
	Value() []byte
	// Next moves the iterator to the next key/value pair.
	// It returns false if the iterator is exhausted.
	Next() bool
	// Seek moves the iterator to the first key/value pair whose key is greater
	// than or equal to the given key.
	// It returns whether such pair exist
	Seek(key []byte) bool
	// Error returns any accumulated error during 'Next()'. An error could occur
	// when the 'Next()' is called on the closed iterator or closed database.
	Error() error
	// Release releases associated resources. Release should always succeed
	// and can be called multiple times without causing error.
	Release()
}

// IsSystemDB returns true if the given db is a system database
func IsSystemDB(dbName string) bool {
	return dbName == UsersDBName ||
		dbName == DatabasesDBName ||
		dbName == ConfigDBName ||
		dbName == MetadataDBName
}

// IsDefaultWorldStateDB returns true if the given db is the default
// data DB
func IsDefaultWorldStateDB(dbName string) bool {
	return dbName == DefaultDBName
}

// SystemDBs returns the name of all system databases
func SystemDBs() []string {
	return []string{
		UsersDBName,
		DatabasesDBName,
		ConfigDBName,
		MetadataDBName,
	}
}
