package worldstate

import "github.ibm.com/blockchaindb/server/api"

// DB provides method to create and access states stored in
// a database.
type DB interface {
	// Create creates a new database
	Create(dbName string) error
	// Open opens an existing database
	Open(dbName string) error
	// Get returns the value of the key present in the
	// database
	Get(dbName, key string) (*api.Value, error)
	// GetVersion returns the version of the key present
	// in the database
	GetVersion(dbName, key string) (*api.Version, error)
	// Commit commits the updates to each database
	Commit(dbsUpdates []*DBUpdates) error
}

// KV holds a key and value pair
type KV struct {
	Key   string
	Value *api.Value
}

// DBUpdates holds writes of KV pairs and deletes of
// keys for each database
type DBUpdates struct {
	DBName  string
	Writes  []*KV
	Deletes []string
}
