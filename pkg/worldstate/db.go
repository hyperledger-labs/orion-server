package worldstate

import "github.ibm.com/blockchaindb/protos/types"

const (
	// UsersDBName holds all users information
	UsersDBName = "_users"
	// DatabasesDBName holds the name of all user databases
	DatabasesDBName = "_dbs"
	// ConfigDBName holds the name of the database that holds
	// the configuration details
	ConfigDBName = "_config"
	// DefaultDBName is the default database created during
	// node bootstrap
	DefaultDBName = "bdb"
)

// DB provides method to create and access states stored in
// a database.
type DB interface {
	// Create creates a new database
	Create(dbName string) error
	// Open opens an existing database
	Open(dbName string) error
	// Get returns the value of the key present in the
	// database
	Get(dbName, key string) (*types.Value, error)
	// GetVersion returns the version of the key present
	// in the database
	GetVersion(dbName, key string) (*types.Version, error)
	// Commit commits the updates to each database
	Commit(dbsUpdates []*DBUpdates) error
	// Close closes the DB instance
	Close() error
}

// KV holds a key and value pair
type KV struct {
	Key   string
	Value *types.Value
}

// DBUpdates holds writes of KV pairs and deletes of
// keys for each database
type DBUpdates struct {
	DBName  string
	Writes  []*KV
	Deletes []string
}
