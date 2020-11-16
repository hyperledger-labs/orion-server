package identity

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/pkg/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

var (
	// UserNamespace holds the user information.
	UserNamespace = []byte{0}
)

// ConstructDBEntriesForUserAdminTx constructs database entries for the transaction that manipulates
// user information
func ConstructDBEntriesForUserAdminTx(tx *types.UserAdministrationTx, version *types.Version) (*worldstate.DBUpdates, error) {
	var userWrites []*worldstate.KVWithMetadata
	var userDeletes []string

	for _, w := range tx.UserWrites {
		userSerialized, err := proto.Marshal(w.User)
		if err != nil {
			return nil, errors.Wrap(err, "error while marshaling user")
		}

		kv := &worldstate.KVWithMetadata{
			Key:   string(UserNamespace) + w.User.ID,
			Value: userSerialized,
			Metadata: &types.Metadata{
				Version:       version,
				AccessControl: w.ACL,
			},
		}
		userWrites = append(userWrites, kv)
	}

	for _, d := range tx.UserDeletes {
		userDeletes = append(userDeletes, string(UserNamespace)+d.UserID)
	}

	return &worldstate.DBUpdates{
		DBName:  worldstate.UsersDBName,
		Writes:  userWrites,
		Deletes: userDeletes,
	}, nil
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
		if _, ok := newAdms[oldAdm.ID]; ok {
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
