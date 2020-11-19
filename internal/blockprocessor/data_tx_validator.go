package blockprocessor

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type dataTxValidator struct {
	db              worldstate.DB
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

func (v *dataTxValidator) validate(tx *types.DataTx, pendingUpdates map[string]bool) (*types.ValidationInfo, error) {
	switch {
	case !v.db.Exist(tx.DBName):
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
			ReasonIfInvalid: "the database [" + tx.DBName + "] does not exist in the cluster",
		}, nil

	case worldstate.IsSystemDB(tx.DBName):
		return &types.ValidationInfo{
			Flag: types.Flag_INVALID_NO_PERMISSION,
			ReasonIfInvalid: "the database [" + tx.DBName + "] is a system database and no user can write to a " +
				"system database via data transaction. Use appropriate transaction type to modify the system database",
		}, nil

	default:
		hasPerm, err := v.identityQuerier.HasReadWriteAccess(tx.UserID, tx.DBName)
		if err != nil {
			return nil, err
		}
		if !hasPerm {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [" + tx.UserID + "] has no read-write permission on the database [" + tx.DBName + "]",
			}, nil
		}
	}

	r, err := v.validateFieldsInDataWrites(tx.DataWrites)
	if err != nil {
		return nil, err
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err = v.validateFieldsInDataDeletes(tx.DBName, tx.DataDeletes)
	if err != nil {
		return nil, err
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r = validateUniquenessInDataWritesAndDeletes(tx.DataWrites, tx.DataDeletes)
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err = v.validateACLOnDataReads(tx.UserID, tx.DBName, tx.DataReads)
	if err != nil {
		return nil, err
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err = v.validateACLOnDataWrites(tx.UserID, tx.DBName, tx.DataWrites)
	if err != nil {
		return nil, err
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err = v.validateACLOnDataDeletes(tx.UserID, tx.DBName, tx.DataDeletes)
	if err != nil {
		return nil, err
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	return v.mvccValidation(tx.DBName, tx.DataReads, pendingUpdates)
}

func (v *dataTxValidator) validateFieldsInDataWrites(DataWrites []*types.DataWrite) (*types.ValidationInfo, error) {
	existingUser := make(map[string]bool)

	for _, w := range DataWrites {
		if w == nil {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the write list",
			}, nil
		}

		if w.ACL == nil {
			continue
		}

		userToCheck := make(map[string]struct{})

		for user := range w.ACL.ReadUsers {
			if existingUser[user] {
				continue
			}
			userToCheck[user] = struct{}{}
		}

		for user := range w.ACL.ReadWriteUsers {
			if existingUser[user] {
				continue
			}
			userToCheck[user] = struct{}{}
		}

		for user := range userToCheck {
			exist, err := v.identityQuerier.DoesUserExist(user)
			if err != nil {
				return nil, errors.WithMessagef(err, "error while validating access control definition")
			}

			if !exist {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the user [" + user + "] defined in the access control for the key [" + w.Key + "] does not exist",
				}, nil
			}

			existingUser[user] = true
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *dataTxValidator) validateFieldsInDataDeletes(dbName string, dataDeletes []*types.DataDelete) (*types.ValidationInfo, error) {
	for _, d := range dataDeletes {
		if d == nil {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the delete list",
			}, nil
		}

		val, metadata, err := v.db.Get(dbName, d.Key)
		if err != nil {
			return nil, errors.WithMessage(err, "error while validating delete entries")
		}
		if val == nil && metadata == nil {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [" + d.Key + "] does not exist in the database and hence, it cannot be deleted",
			}, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func validateUniquenessInDataWritesAndDeletes(dataWrites []*types.DataWrite, dataDeletes []*types.DataDelete) *types.ValidationInfo {
	writeKeys := make(map[string]bool)
	deleteKeys := make(map[string]bool)

	for _, w := range dataWrites {
		if writeKeys[w.Key] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [" + w.Key + "] is duplicated in the write list. The keys in the write list must be unique",
			}
		}
		writeKeys[w.Key] = true
	}

	for _, d := range dataDeletes {
		switch {
		case deleteKeys[d.Key]:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [" + d.Key + "] is duplicated in the delete list. The keys in the delete list must be unique",
			}

		case writeKeys[d.Key]:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [" + d.Key + "] is being updated as well as deleted. Only one operation per key is allowed within a transaction",
			}
		}

		deleteKeys[d.Key] = true
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func (v *dataTxValidator) validateACLOnDataReads(userID, dbName string, reads []*types.DataRead) (*types.ValidationInfo, error) {
	for _, r := range reads {
		acl, err := v.db.GetACL(dbName, r.Key)
		if err != nil {
			return nil, errors.WithMessagef(err, "error while validating ACL on the key [%s] in the reads", r.Key)
		}
		if acl == nil {
			continue
		}

		if !acl.ReadUsers[userID] && !acl.ReadWriteUsers[userID] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [" + userID + "] has no read permission on key [" + r.Key + "] present in the database [" + dbName + "]",
			}, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *dataTxValidator) validateACLOnDataWrites(userID, dbName string, writes []*types.DataWrite) (*types.ValidationInfo, error) {
	for _, w := range writes {
		acl, err := v.db.GetACL(dbName, w.Key)
		if err != nil {
			return nil, err
		}
		if acl == nil {
			continue
		}

		if !acl.ReadWriteUsers[userID] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [" + userID + "] has no write permission on key [" + w.Key + "] present in the database [" + dbName + "]",
			}, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *dataTxValidator) validateACLOnDataDeletes(userID, dbName string, deletes []*types.DataDelete) (*types.ValidationInfo, error) {
	for _, d := range deletes {
		acl, err := v.db.GetACL(dbName, d.Key)
		if err != nil {
			return nil, err
		}
		if acl == nil {
			continue
		}

		if !acl.ReadWriteUsers[userID] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [" + userID + "] has no write permission on key [" + d.Key + "] present in the database [" + dbName + "]. Hence, the user cannot delete the key",
			}, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *dataTxValidator) mvccValidation(dbName string, reads []*types.DataRead, pendingUpdates map[string]bool) (*types.ValidationInfo, error) {
	for _, r := range reads {
		if pendingUpdates[r.Key] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [" + r.Key + "] in database [" + dbName + "]",
			}, nil
		}

		committedVersion, err := v.db.GetVersion(dbName, r.Key)
		if err != nil {
			return nil, err
		}
		if proto.Equal(r.Version, committedVersion) {
			continue
		}

		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
			ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the key [" + r.Key + "] in database [" + dbName + "] changed",
		}, nil
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}
