package blockprocessor

import (
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

// validator validates the each transaction read set present in a
// block against the committed version to ensure the requested
// isolation level
type validator struct {
	db              worldstate.DB
	identityQuerier *identity.Querier
}

// newValidator creates a new validator
func newValidator(conf *Config) *validator {
	return &validator{
		db:              conf.DB,
		identityQuerier: identity.NewQuerier(conf.DB),
	}
}

// validateBlock validates each transaction present in the block to ensure
// the request isolation level
func (v *validator) validateBlock(block *types.Block) ([]*types.ValidationInfo, error) {
	if block.Header.Number == 1 {
		// for the genesis block, which is created by the node itself, we cannot
		// do a regular validation
		return []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}, nil
	}

	valInfo := make([]*types.ValidationInfo, len(block.TransactionEnvelopes))
	pendingWrites := make(map[string]bool)

	// TODO
	// Currently, we have a code snippet to verify user signature at the http server.
	// However, the user certificate might have been updated between the submission of
	// transaction and this validation. Hence, we either need to check whether the version
	// read by the http server has changed or need to repeat the signature validation. As
	// the later is costlier, former is preferred but need to figure out a way to send
	// additional local information with each transaction so that we can ensure
	// correctness here - issue 108

	// assumption: whenever there is a config, db, user manipulation transaction, the
	// block would contain a single transaction only

	for txIndex, tx := range block.TransactionEnvelopes {
		if !v.db.Exist(tx.Payload.DBName) {
			valInfo[txIndex] = &types.ValidationInfo{
				Flag:            types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
				ReasonIfInvalid: fmt.Sprintf("the database [%s] does not exist", tx.Payload.DBName),
			}
			continue
		}

		valRes, err := v.validateWithACL(tx.Payload)
		if err != nil {
			return nil, err
		}
		if valRes.Flag != types.Flag_VALID {
			valInfo[txIndex] = valRes
			continue
		}

		switch tx.Payload.Type {
		case types.Transaction_USER:
			valRes = v.validateUserEntries(tx.Payload)
		case types.Transaction_DB:
			valRes = v.validateDBEntries(tx.Payload)
		case types.Transaction_CONFIG:
			valRes = v.validateConfigEntries(tx.Payload)
		default:
			valRes, err = v.validateDataEntries(tx.Payload)
			if err != nil {
				return nil, err
			}
		}

		if valRes.Flag != types.Flag_VALID {
			valInfo[txIndex] = valRes
			continue
		}

		// except MVCC validation, all other validation can be executed in parallel for all
		// transactions
		if valInfo[txIndex], err = v.mvccValidation(tx.Payload, pendingWrites); err != nil {
			return nil, err
		}
		if valInfo[txIndex].Flag == types.Flag_VALID {
			for _, write := range tx.Payload.Writes {
				pendingWrites[write.Key] = true
			}
		}
	}

	return valInfo, nil
}

func (v *validator) validateWithACL(tx *types.Transaction) (*types.ValidationInfo, error) {
	userID := string(tx.UserID)
	dbName := tx.DBName

	switch tx.Type {
	case types.Transaction_USER:
		hasPerm, err := v.identityQuerier.HasUserAdministrationPrivilege(userID)
		if err != nil {
			return nil, errors.WithMessagef(err, "error while checking user administrative privilege for user [%s]", userID)
		}
		if !hasPerm {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: fmt.Sprintf("the user [%s] has no privilege to perform user administrative operations", userID),
			}, nil
		}

	case types.Transaction_DB:
		hasPerm, err := v.identityQuerier.HasDBAdministrationPrivilege(userID)
		if err != nil {
			return nil, errors.WithMessagef(err, "error while checking database administrative privilege for user [%s]", userID)
		}
		if !hasPerm {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: fmt.Sprintf("the user [%s] has no privilege to perform database administrative operations", userID),
			}, nil
		}

	case types.Transaction_CONFIG:
		hasPerm, err := v.identityQuerier.HasClusterAdministrationPrivilege(userID)
		if err != nil {
			return nil, errors.WithMessagef(err, "error while checking cluster administrative privilege for user [%s]", userID)
		}
		if !hasPerm {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: fmt.Sprintf("the user [%s] has no privilege to perform cluster administrative operations", userID),
			}, nil
		}

	default:
		hasPerm, err := v.identityQuerier.HasReadWriteAccess(userID, dbName)
		if err != nil {
			return nil, errors.WithMessagef(err, "error while checking database [%s] read-write privilege for user [%s]", dbName, userID)
		}
		if !hasPerm {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: fmt.Sprintf("the user [%s] has no write permission on database [%s]", userID, dbName),
			}, nil
		}
	}

	for _, read := range tx.Reads {
		acl, err := v.db.GetACL(tx.DBName, read.Key)
		if err != nil {
			return nil, err
		}
		if acl == nil {
			continue
		}

		if !acl.ReadUsers[string(tx.UserID)] && !acl.ReadWriteUsers[string(tx.UserID)] {
			return &types.ValidationInfo{
				Flag: types.Flag_INVALID_NO_PERMISSION,
			}, nil
		}
	}

	for _, w := range tx.Writes {
		acl, err := v.db.GetACL(tx.DBName, w.Key)
		if err != nil {
			return nil, err
		}
		if acl == nil {
			continue
		}

		if !acl.ReadWriteUsers[userID] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: fmt.Sprintf("the user [%s] has no write permission on key [%s] present in the database [%s]", userID, w.Key, dbName),
			}, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *validator) validateUserEntries(tx *types.Transaction) *types.ValidationInfo {
	for _, w := range tx.Writes {
		u := &types.User{}
		if err := json.Unmarshal(w.Value, u); err != nil {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("unmarshal error while retrieving user [%s] entry from the transaction: %s", w.Key, err.Error()),
			}
		}

		if u.ID == "" {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user with empty ID. A valid userID must be non empty string",
			}
		}

		if _, err := x509.ParseCertificate(u.Certificate); err != nil {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("the user [%s] has an invalid certificate: %s", u.ID, err.Error()),
			}
		}
		// TODO: check who issued the certificate
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func (v *validator) validateDBEntries(tx *types.Transaction) *types.ValidationInfo {
	toCreateDBs := make(map[string]bool)
	toDeleteDBs := make(map[string]bool)

	for _, w := range tx.Writes {
		dbName := w.Key

		switch {
		case dbName == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database name cannot be empty",
			}

		case worldstate.IsSystemDB(dbName):
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("the database name [%s] is a system database which cannot be administered", dbName),
			}
		}

		switch {
		case w.IsDelete:
			if !v.db.Exist(w.Key) {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: fmt.Sprintf("the database [%s] does not exist in the cluster and hence, it cannot be deleted", dbName),
				}
			}

			if toDeleteDBs[w.Key] {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: fmt.Sprintf("the database [%s] is duplicated in the delete list", dbName),
				}
			}

			toDeleteDBs[w.Key] = true

		default:
			if v.db.Exist(w.Key) {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: fmt.Sprintf("the database [%s] already exists in the cluster and hence, it cannot be created", dbName),
				}
			}

			if toCreateDBs[w.Key] {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: fmt.Sprintf("the database [%s] is duplicated in the create list", dbName),
				}
			}

			toCreateDBs[w.Key] = true
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func (v *validator) validateConfigEntries(tx *types.Transaction) *types.ValidationInfo {
	newConfig := &types.ClusterConfig{}
	if err := json.Unmarshal(tx.Writes[configTxIndex].Value, newConfig); err != nil {
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "unmarshal error while retrieving new configuration from the transaction",
		}
	}

	switch {
	case len(newConfig.Nodes) == 0:
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "node entries are empty. There must be at least single node in the cluster",
		}
	case len(newConfig.Admins) == 0:
		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
			ReasonIfInvalid: "admin entries are empty. There must be at least single admin in the cluster",
		}
	}

	for _, n := range newConfig.Nodes {
		switch {
		case n.ID == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the nodeID cannot be empty",
			}
		case n.Address == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("the node [%s] has an empty ip address", n.ID),
			}
		case net.ParseIP(n.Address) == nil:
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("the node [%s] has an invalid ip address [%s]", n.ID, n.Address),
			}
		default:
			if _, err := x509.ParseCertificate(n.Certificate); err != nil {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: fmt.Sprintf("the node [%s] has an invalid certificate: %s", n.ID, err.Error()),
				}
			}
		}
	}

	for _, a := range newConfig.Admins {
		switch {
		case a.ID == "":
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the adminID cannot be empty",
			}
		default:
			if _, err := x509.ParseCertificate(a.Certificate); err != nil {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: fmt.Sprintf("the admin [%s] has an invalid certificate: %s", a.ID, err.Error()),
				}
			}
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
}

func (v *validator) validateDataEntries(tx *types.Transaction) (*types.ValidationInfo, error) {
	for _, w := range tx.Writes {
		if w.ACL == nil {
			continue
		}

		var users []string

		for user := range w.ACL.ReadUsers {
			users = append(users, user)
		}

		for user := range w.ACL.ReadWriteUsers {
			users = append(users, user)
		}

		for _, user := range users {
			exist, err := v.identityQuerier.DoesUserExist(user)
			if err != nil {
				return nil, errors.WithMessagef(err, "error while validating access control definition")
			}

			if !exist {
				return &types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: fmt.Sprintf("the user [%s] defined in the access control for the key [%s] does not exist", user, w.Key),
				}, nil
			}
		}
	}
	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *validator) mvccValidation(tx *types.Transaction, pendingWrites map[string]bool) (*types.ValidationInfo, error) {
	for _, r := range tx.Reads {
		if pendingWrites[r.Key] {
			return &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: fmt.Sprintf("mvcc conflict has occurred within the block for the key [%s] in database [%s]", r.Key, tx.DBName),
			}, nil
		}

		committedVersion, err := v.db.GetVersion(tx.DBName, r.Key)
		if err != nil {
			return nil, err
		}
		if proto.Equal(r.Version, committedVersion) {
			continue
		}

		return &types.ValidationInfo{
			Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
			ReasonIfInvalid: fmt.Sprintf("mvcc conflict has occurred as the committed state for the key [%s] in database [%s] changed", r.Key, tx.DBName),
		}, nil
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}
