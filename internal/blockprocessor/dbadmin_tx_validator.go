package blockprocessor

import (
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type dbAdminTxValidator struct {
	db              worldstate.DB
	identityQuerier *identity.Querier
	sigValidator    *txSigValidator
	logger          *logger.SugarLogger
}

func (v *dbAdminTxValidator) validate(txEnv *types.DBAdministrationTxEnvelope) (*types.ValidationInfo, error) {
	valInfo, err := v.sigValidator.validate(txEnv.Payload.UserID, txEnv.Signature, txEnv.Payload)
	if err != nil || valInfo.Flag != types.Flag_VALID {
		return valInfo, err
	}

	tx := txEnv.Payload
	hasPerm, err := v.identityQuerier.HasDBAdministrationPrivilege(tx.UserID)
	if err != nil {
		return nil, errors.WithMessagef(err, "error while checking database administrative privilege for user [%s]", tx.UserID)
	}
	if !hasPerm {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_NO_PERMISSION,
			ReasonIfInvalid: "the user [" + tx.UserID + "] has no privilege to perform database administrative operations",
		}, nil
	}

	if r := v.validateCreateDBEntries(tx.CreateDBs); r.Flag != types.Flag_VALID {
		return r, nil
	}

	return v.validateDeleteDBEntries(tx.DeleteDBs), nil
}

func (v *dbAdminTxValidator) validateCreateDBEntries(toCreateDBs []string) *types.ValidationInfo {
	toCreateDBsLookup := make(map[string]bool)

	for _, dbName := range toCreateDBs {
		switch {
		case dbName == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the name of the database to be created cannot be empty",
			}

		case !v.db.ValidDBName(dbName):
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database name [" + dbName + "] is not valid",
			}

		case worldstate.IsSystemDB(dbName):
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [" + dbName + "] is a system database which cannot be created as it exist by default",
			}

		case worldstate.IsDefaultWorldStateDB(dbName):
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [" + dbName + "] is the system created default database for storing states and it cannot be created as it exist by default",
			}

		default:
			if v.db.Exist(dbName) {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the database [" + dbName + "] already exists in the cluster and hence, it cannot be created",
				}
			}

			if toCreateDBsLookup[dbName] {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the database [" + dbName + "] is duplicated in the create list",
				}
			}

			toCreateDBsLookup[dbName] = true
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func (v *dbAdminTxValidator) validateDeleteDBEntries(toDeleteDBs []string) *types.ValidationInfo {
	toDeleteDBsLookup := make(map[string]bool)

	for _, dbName := range toDeleteDBs {
		switch {
		case dbName == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the name of the database to be deleted cannot be empty",
			}

		case !v.db.ValidDBName(dbName):
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database name [" + dbName + "] is not valid",
			}

		case worldstate.IsSystemDB(dbName):
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [" + dbName + "] is a system database which cannot be deleted",
			}

		case worldstate.IsDefaultWorldStateDB(dbName):
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [" + dbName + "] is the system created default database to store states and it cannot be deleted",
			}

		default:
			if !v.db.Exist(dbName) {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the database [" + dbName + "] does not exist in the cluster and hence, it cannot be deleted",
				}
			}

			if toDeleteDBsLookup[dbName] {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the database [" + dbName + "] is duplicated in the delete list",
				}
			}

			toDeleteDBsLookup[dbName] = true
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}
