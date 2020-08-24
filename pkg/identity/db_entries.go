package identity

import (
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

var (
	// userNamespace holds the user information.
	userNamespace = []byte{0}
	// we need multiple namespaces so that we can add
	// groups and secondary indices later for a faster
	// lookups
)

// ConstructDBEntriesForUsers constructs database entries for the transaction that manipulates
// user information
func ConstructDBEntriesForUsers(tx *types.Transaction, version *types.Version) *worldstate.DBUpdates {
	var kvWrites []*worldstate.KVWithMetadata
	var kvDeletes []string

	for _, write := range tx.Writes {
		if write.IsDelete {
			kvDeletes = append(
				kvDeletes,
				string(userNamespace)+write.Key,
			)
			continue
		}

		kv := &worldstate.KVWithMetadata{
			Key:   string(userNamespace) + write.Key,
			Value: write.Value,
			Metadata: &types.Metadata{
				Version: version,
			},
		}
		kvWrites = append(kvWrites, kv)
	}

	return &worldstate.DBUpdates{
		DBName:  worldstate.UsersDBName,
		Writes:  kvWrites,
		Deletes: kvDeletes,
	}
}
