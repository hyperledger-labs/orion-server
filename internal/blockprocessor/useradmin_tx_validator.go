// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/certificateauthority"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type userAdminTxValidator struct {
	db              worldstate.DB
	identityQuerier *identity.Querier
	sigValidator    *txSigValidator
	logger          *logger.SugarLogger
}

func (v *userAdminTxValidator) validate(txEnv *types.UserAdministrationTxEnvelope) (*types.ValidationInfo, error) {
	valInfo, err := v.sigValidator.validate(txEnv.Payload.UserID, txEnv.Signature, txEnv.Payload)
	if err != nil || valInfo.Flag != types.Flag_VALID {
		return valInfo, err
	}

	tx := txEnv.Payload
	hasPerm, err := v.identityQuerier.HasAdministrationPrivilege(tx.UserID)
	if err != nil {
		return nil, errors.WithMessagef(err, "error while checking user administrative privilege for user [%s]", tx.UserID)
	}
	if !hasPerm {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_NO_PERMISSION,
			ReasonIfInvalid: "the user [" + tx.UserID + "] has no privilege to perform user administrative operations",
		}, nil
	}

	r, err := v.validateFieldsInUserWrites(tx.UserWrites)
	if err != nil {
		return nil, errors.WithMessagef(err, "error while validating fields in user writes")
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	if r := validateFieldsInUserDeletes(tx.UserDeletes); r.Flag != types.Flag_VALID {
		return r, nil
	}

	if r := validateUniquenessInUserWritesAndDeletes(tx.UserWrites, tx.UserDeletes); r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err = v.validateACLOnUserReads(tx.UserID, tx.UserReads)
	if err != nil {
		return nil, errors.WithMessage(err, "error while validating ACL on reads")
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err = v.validateACLOnUserWrites(tx.UserID, tx.UserWrites)
	if err != nil {
		return nil, errors.WithMessage(err, "error while validating ACL on writes")
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err = v.validateACLOnUserDeletes(tx.UserID, tx.UserDeletes)
	if err != nil {
		return nil, errors.WithMessage(err, "error while validating ACL on deletes")
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	return v.mvccValidation(tx.UserReads)
}

func (v *userAdminTxValidator) validateFieldsInUserWrites(userWrites []*types.UserWrite) (*types.ValidationInfo, error) {
	config, _, err := v.db.GetConfig()
	if err != nil {
		return nil, errors.Wrap(err, "cannot get config")
	}
	if config == nil {
		return nil, errors.New("config is nil")
	}
	caCertCollection, err := certificateauthority.NewCACertCollection(config.CertAuthConfig.Roots, config.CertAuthConfig.Intermediates)
	if err != nil {
		return nil, errors.Wrap(err, "cannot build CA certificate collection")
	}

	for _, w := range userWrites {
		switch {
		case w == nil:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the write list",
			}, nil

		case w.User == nil:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty user entry in the write list",
			}, nil

		case w.User.ID == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user in the write list with an empty ID. A valid userID must be an non-empty string",
			}, nil

		default:
			if w.User.Privilege != nil {
				if w.User.Privilege.Admin {
					return &types.ValidationInfo{
						Flag:            types.Flag_INVALID_NO_PERMISSION,
						ReasonIfInvalid: "the user [" + w.User.ID + "] is marked as admin user. Only via a cluster configuration transaction, the [" + w.User.ID + "] can be added as admin",
					}, nil
				}

				dbPerm := w.User.Privilege.DBPermission
				for dbName := range dbPerm {
					if v.db.Exist(dbName) {
						continue
					}
					return &types.ValidationInfo{
						Flag:            types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
						ReasonIfInvalid: "the database [" + dbName + "] present in the db permission list does not exist in the cluster",
					}, nil
				}
			}

			err = caCertCollection.VerifyLeafCert(w.User.Certificate)
			if err != nil {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the user [" + w.User.ID + "] in the write list has an invalid certificate: Error = " + err.Error(),
				}, nil
			}
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func validateFieldsInUserDeletes(userDeletes []*types.UserDelete) *types.ValidationInfo {
	for _, d := range userDeletes {
		switch {
		case d == nil:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the delete list",
			}

		case d.UserID == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user in the delete list with an empty ID. A valid userID must be an non-empty string",
			}
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func validateUniquenessInUserWritesAndDeletes(userWrites []*types.UserWrite, userDeletes []*types.UserDelete) *types.ValidationInfo {
	writeUserIDs := make(map[string]bool)
	deleteUserIDs := make(map[string]bool)

	for _, w := range userWrites {
		if writeUserIDs[w.User.ID] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two users with the same userID [" + w.User.ID + "] in the write list. The userIDs in the write list must be unique",
			}
		}

		writeUserIDs[w.User.ID] = true
	}

	for _, d := range userDeletes {
		switch {
		case deleteUserIDs[d.UserID]:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two users with the same userID [" + d.UserID + "] in the delete list. The userIDs in the delete list must be unique",
			}

		case writeUserIDs[d.UserID]:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [" + d.UserID + "] is present in both write and delete list. Only one operation per key is allowed within a transaction",
			}
		}

		deleteUserIDs[d.UserID] = true
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func (v *userAdminTxValidator) validateACLOnUserReads(operatingUser string, reads []*types.UserRead) (*types.ValidationInfo, error) {
	for _, r := range reads {
		targetUser := r.UserID

		hasPerm, err := v.identityQuerier.HasReadAccessOnTargetUser(operatingUser, targetUser)
		if err != nil {
			if _, ok := err.(*identity.NotFoundErr); !ok {
				return nil, err
			}

			continue
		}

		if !hasPerm {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [" + operatingUser + "] has no read permission on the user [" + targetUser + "]",
			}, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *userAdminTxValidator) validateACLOnUserWrites(operatingUser string, writes []*types.UserWrite) (*types.ValidationInfo, error) {
	for _, w := range writes {
		targetUser := w.User.ID

		admin, err := v.identityQuerier.HasAdministrationPrivilege(targetUser)
		if err != nil {
			if _, ok := err.(*identity.NotFoundErr); !ok {
				return nil, err
			}

			continue
		}
		if admin {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [" + targetUser + "] is an admin user. Only via a cluster configuration transaction, the [" + targetUser + "] can be modified",
			}, nil
		}

		hasPerm, err := v.identityQuerier.HasReadWriteAccessOnTargetUser(operatingUser, targetUser)
		if err != nil {
			return nil, err
		}

		if !hasPerm {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [" + operatingUser + "] has no write permission on the user [" + targetUser + "]",
			}, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *userAdminTxValidator) validateACLOnUserDeletes(operatingUser string, deletes []*types.UserDelete) (*types.ValidationInfo, error) {
	for _, d := range deletes {
		targetUser := d.UserID

		admin, err := v.identityQuerier.HasAdministrationPrivilege(targetUser)
		if err != nil {
			if _, ok := err.(*identity.NotFoundErr); !ok {
				return nil, err
			}

			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [" + targetUser + "] present in the delete list does not exist",
			}, nil
		}

		if admin {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [" + targetUser + "] is an admin user. Only via a cluster configuration transaction, the [" + targetUser + "] can be deleted",
			}, nil
		}

		hasPerm, err := v.identityQuerier.HasReadWriteAccessOnTargetUser(operatingUser, targetUser)
		if err != nil {
			return nil, err
		}

		if !hasPerm {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [" + operatingUser + "] has no write permission on the user [" + targetUser + "]. Hence, the delete operation cannot be performed",
			}, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *userAdminTxValidator) mvccValidation(userReads []*types.UserRead) (*types.ValidationInfo, error) {
	for _, r := range userReads {
		committedVersion, err := v.identityQuerier.GetVersion(r.UserID)
		if err != nil {
			if _, ok := err.(*identity.NotFoundErr); !ok {
				return nil, err
			}
		}

		if proto.Equal(r.Version, committedVersion) {
			continue
		}

		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
			ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the user [" + r.UserID + "] has changed",
		}, nil
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}
