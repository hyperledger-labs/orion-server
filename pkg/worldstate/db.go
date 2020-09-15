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
	// ConfigKey holds the name of the key in the ConfigDB that
	// stores the cluster configuration
	ConfigKey = "config"
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
	// Commit commits the updates to each database
	Commit(dbsUpdates []*DBUpdates) error
	// Close closes the DB instance
	Close() error
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
	DBName  string
	Writes  []*KVWithMetadata
	Deletes []string
}

// IsSystemDB returns true of the given db is a system database
func IsSystemDB(dbName string) bool {
	return dbName == UsersDBName ||
		dbName == DatabasesDBName ||
		dbName == ConfigDBName ||
		dbName == DefaultDBName
}
