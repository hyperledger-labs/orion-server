// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package txvalidation

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/certificateauthority"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

type userAdminTxValidator struct {
	db              worldstate.DB
	identityQuerier *identity.Querier
	sigValidator    *txSigValidator
	logger          *logger.SugarLogger
}

func (v *userAdminTxValidator) validate(txEnv *types.UserAdministrationTxEnvelope) (*types.ValidationInfo, error) {
	valInfo, err := v.sigValidator.validate(txEnv.Payload.UserId, txEnv.Signature, txEnv.Payload)
	if err != nil || valInfo.Flag != types.Flag_VALID {
		return valInfo, err
	}

	tx := txEnv.Payload
	hasPerm, err := v.identityQuerier.HasAdministrationPrivilege(tx.UserId)
	if err != nil {
		return nil, errors.WithMessagef(err, "error while checking user administrative privilege for user [%s]", tx.UserId)
	}
	if !hasPerm {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_NO_PERMISSION,
			ReasonIfInvalid: "the user [" + tx.UserId + "] has no privilege to perform user administrative operations",
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

	r, err = v.validateACLOnUserWrites(tx.UserId, tx.UserWrites)
	if err != nil {
		return nil, errors.WithMessage(err, "error while validating ACL on writes")
	}
	if r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err = v.validateACLOnUserDeletes(tx.UserId, tx.UserDeletes)
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

		case w.User.Id == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user in the write list with an empty ID. A valid userID must be an non-empty string",
			}, nil

		default:
			if w.User.Privilege != nil {
				if w.User.Privilege.Admin {
					return &types.ValidationInfo{
						Flag:            types.Flag_INVALID_NO_PERMISSION,
						ReasonIfInvalid: "the user [" + w.User.Id + "] is marked as admin user. Only via a cluster configuration transaction, the [" + w.User.Id + "] can be added as admin",
					}, nil
				}

				dbPerm := w.User.Privilege.DbPermission
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

			if w.Acl != nil && w.Acl.ReadWriteUsers != nil {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "adding users to Acl.ReadWriteUsers is not supported",
				}, nil
			}

			err = caCertCollection.VerifyLeafCert(w.User.Certificate)
			if err != nil {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the user [" + w.User.Id + "] in the write list has an invalid certificate: Error = " + err.Error(),
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

		case d.UserId == "":
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
		if writeUserIDs[w.User.Id] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two users with the same userID [" + w.User.Id + "] in the write list. The userIDs in the write list must be unique",
			}
		}

		writeUserIDs[w.User.Id] = true
	}

	for _, d := range userDeletes {
		switch {
		case deleteUserIDs[d.UserId]:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two users with the same userID [" + d.UserId + "] in the delete list. The userIDs in the delete list must be unique",
			}

		case writeUserIDs[d.UserId]:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [" + d.UserId + "] is present in both write and delete list. Only one operation per key is allowed within a transaction",
			}
		}

		deleteUserIDs[d.UserId] = true
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func (v *userAdminTxValidator) validateACLOnUserWrites(operatingUser string, writes []*types.UserWrite) (*types.ValidationInfo, error) {
	for _, w := range writes {
		targetUser := w.User.Id

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
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *userAdminTxValidator) validateACLOnUserDeletes(operatingUser string, deletes []*types.UserDelete) (*types.ValidationInfo, error) {
	for _, d := range deletes {
		targetUser := d.UserId

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
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *userAdminTxValidator) mvccValidation(userReads []*types.UserRead) (*types.ValidationInfo, error) {
	for _, r := range userReads {
		committedVersion, err := v.identityQuerier.GetUserVersion(r.UserId)
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
			ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the user [" + r.UserId + "] has changed",
		}, nil
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}
