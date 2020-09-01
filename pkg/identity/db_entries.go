package identity

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

var (
	// UserNamespace holds the user information.
	UserNamespace = []byte{0}
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
				string(UserNamespace)+write.Key,
			)
			continue
		}

		kv := &worldstate.KVWithMetadata{
			Key:   string(UserNamespace) + write.Key,
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

// ConstructDBEntriesForClusterAdmins constructs database entries for the cluster admins
func ConstructDBEntriesForClusterAdmins(oldAdmins, newAdmins []*types.Admin, version *types.Version) (*worldstate.DBUpdates, error) {
	var kvWrites []*worldstate.KVWithMetadata
	var deletes []string

	newAdms := make(map[string]*types.Admin)
	for _, newAdm := range newAdmins {
		newAdms[newAdm.ID] = newAdm
	}

	for _, oldAdm := range oldAdmins {
		if newAdm, ok := newAdms[oldAdm.ID]; ok {
			if proto.Equal(oldAdm, newAdm) {
				delete(newAdms, oldAdm.ID)
			}
			continue
		}
		deletes = append(deletes, string(UserNamespace)+oldAdm.ID)
	}

	for _, admin := range newAdms {
		u := &types.User{
			ID:          admin.ID,
			Certificate: admin.Certificate,
			Privilege: &types.Privilege{
				DBAdministration:      true,
				ClusterAdministration: true,
				UserAdministration:    true,
			},
		}

		value, err := proto.Marshal(u)
		if err != nil {
			return nil, errors.New("error marshaling admin user")
		}

		kvWrites = append(
			kvWrites,
			&worldstate.KVWithMetadata{
				Key:   string(UserNamespace) + admin.ID,
				Value: value,
				Metadata: &types.Metadata{
					Version: version,
				},
			},
		)
	}

	if len(kvWrites) == 0 && len(deletes) == 0 {
		return nil, nil
	}

	return &worldstate.DBUpdates{
		DBName:  worldstate.UsersDBName,
		Writes:  kvWrites,
		Deletes: deletes,
	}, nil
}
