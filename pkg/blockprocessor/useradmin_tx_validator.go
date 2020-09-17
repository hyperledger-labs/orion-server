package blockprocessor

import (
	"crypto/x509"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/library/pkg/logger"
)

type userAdminTxValidator struct {
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

func (v *userAdminTxValidator) validate(tx *types.UserAdministrationTx) (*types.ValidationInfo, error) {
	hasPerm, err := v.identityQuerier.HasUserAdministrationPrivilege(tx.UserID)
	if err != nil {
		return nil, errors.WithMessagef(err, "error while checking user administrative privilege for user [%s]", tx.UserID)
	}
	if !hasPerm {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_NO_PERMISSION,
			ReasonIfInvalid: "the user [" + tx.UserID + "] has no privilege to perform user administrative operations",
		}, nil
	}

	if r := validateFieldsInUserWrites(tx.UserWrites); r.Flag != types.Flag_VALID {
		return r, nil
	}

	if r := validateFieldsInUserDeletes(tx.UserDeletes); r.Flag != types.Flag_VALID {
		return r, nil
	}

	if r := validateUniquenessInUserWritesAndDeletes(tx.UserWrites, tx.UserDeletes); r.Flag != types.Flag_VALID {
		return r, nil
	}

	r, err := v.validateACLOnUserReads(tx.UserID, tx.UserReads)
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

func validateFieldsInUserWrites(userWrites []*types.UserWrite) *types.ValidationInfo {
	for _, w := range userWrites {
		switch {
		case w == nil:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the write list",
			}

		case w.User == nil:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty user entry in the write list",
			}

		case w.User.ID == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user in the write list with an empty ID. A valid userID must be an non-empty string",
			}

		default:
			if _, err := x509.ParseCertificate(w.User.Certificate); err != nil {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the user [" + w.User.ID + "] in the write list has an invalid certificate: Error = " + err.Error(),
				}
			}
		}
		// TODO: check who issued the certificate
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
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
			if _, ok := err.(*identity.UserNotFoundErr); !ok {
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

		hasPerm, err := v.identityQuerier.HasReadWriteAccessOnTargetUser(operatingUser, targetUser)
		if err != nil {
			if _, ok := err.(*identity.UserNotFoundErr); !ok {
				return nil, err
			}

			continue
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

		hasPerm, err := v.identityQuerier.HasReadWriteAccessOnTargetUser(operatingUser, targetUser)
		if err != nil {
			if _, ok := err.(*identity.UserNotFoundErr); !ok {
				return nil, err
			}

			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [" + targetUser + "] present in the delete list does not exist",
			}, nil
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
			if _, ok := err.(*identity.UserNotFoundErr); !ok {
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
