package identity

import (
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/internal/provenance"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/types"
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

// ConstructProvenanceEntriesForUserAdminTx constructs provenance entries for the transaction that manipulates
func ConstructProvenanceEntriesForUserAdminTx(
	tx *types.UserAdministrationTx,
	version *types.Version,
	db worldstate.DB,
) (*provenance.TxDataForProvenance, error) {
	identityQuerier := NewQuerier(db)
	txData := &provenance.TxDataForProvenance{
		IsValid:            true,
		DBName:             worldstate.UsersDBName,
		UserID:             tx.UserID,
		TxID:               tx.TxID,
		Deletes:            make(map[string]*types.Version),
		OldVersionOfWrites: make(map[string]*types.Version),
	}

	for _, read := range tx.UserReads {
		k := &provenance.KeyWithVersion{
			Key:     read.UserID,
			Version: read.Version,
		}
		txData.Reads = append(txData.Reads, k)
	}

	for _, write := range tx.UserWrites {
		userSerialized, err := proto.Marshal(write.User)
		if err != nil {
			return nil, errors.Wrap(err, "error while marshaling user")
		}

		kv := &types.KVWithMetadata{
			Key:   write.User.ID,
			Value: userSerialized,
			Metadata: &types.Metadata{
				Version:       version,
				AccessControl: write.ACL,
			},
		}
		txData.Writes = append(txData.Writes, kv)

		v, err := identityQuerier.GetVersion(write.User.ID)
		if err != nil {
			if _, ok := err.(*UserNotFoundErr); ok {
				continue
			}

			return nil, err
		}

		txData.OldVersionOfWrites[write.User.ID] = v
	}

	for _, d := range tx.UserDeletes {
		v, err := identityQuerier.GetVersion(d.UserID)
		if err != nil {
			return nil, err
		}

		// for a delete to be valid, the value must exist and hence, the version will
		// never be nil
		txData.Deletes[d.UserID] = v
	}

	return txData, nil
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
			if proto.Equal(oldAdm, newAdms[oldAdm.ID]) {
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
				Admin: true,
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

// ConstructProvenanceEntriesForClusterAdmins constructs provenance entries for the transaction that manipulates
// cluster admins
func ConstructProvenanceEntriesForClusterAdmins(
	userID, txID string,
	adminUpdates *worldstate.DBUpdates,
	db worldstate.DB,
) (*provenance.TxDataForProvenance, error) {
	identityQuerier := NewQuerier(db)
	txData := &provenance.TxDataForProvenance{
		IsValid:            true,
		DBName:             worldstate.UsersDBName,
		UserID:             userID,
		TxID:               txID,
		Deletes:            make(map[string]*types.Version),
		OldVersionOfWrites: make(map[string]*types.Version),
	}

	if adminUpdates == nil {
		return txData, nil
	}

	for _, w := range adminUpdates.Writes {
		adminID := getUserIDFromCompositeUserKey(w.Key)
		txData.Writes = append(
			txData.Writes,
			&types.KVWithMetadata{
				Key:      adminID,
				Value:    w.Value,
				Metadata: w.Metadata,
			},
		)

		version, err := identityQuerier.GetVersion(adminID)
		if err != nil {
			if _, ok := err.(*UserNotFoundErr); ok {
				continue
			}

			return nil, err
		}
		txData.OldVersionOfWrites[adminID] = version
	}

	for _, d := range adminUpdates.Deletes {
		adminID := getUserIDFromCompositeUserKey(d)
		version, err := identityQuerier.GetVersion(adminID)
		if err != nil {
			// admin to be deleted must exist
			return nil, err
		}
		txData.Deletes[adminID] = version
	}

	return txData, nil
}

func getUserIDFromCompositeUserKey(ckey string) string {
	strs := strings.Split(ckey, string(UserNamespace))
	return strs[1]
}
