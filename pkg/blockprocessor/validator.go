package blockprocessor

import (
	"github.com/golang/protobuf/proto"
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
				Flag: types.Flag_INVALID_DB_NOT_EXIST,
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
	var hasPerm bool
	var err error

	switch tx.Type {
	case types.Transaction_USER:
		hasPerm, err = v.identityQuerier.HasUserAdministrationPrivilege(string(tx.UserID))
	case types.Transaction_DB:
		hasPerm, err = v.identityQuerier.HasDBAdministrationPrivilege(string(tx.UserID))
	case types.Transaction_CONFIG:
		hasPerm, err = v.identityQuerier.HasClusterAdministrationPrivilege(string(tx.UserID))
	default:
		hasPerm, err = v.identityQuerier.HasReadWriteAccess(string(tx.UserID), tx.DBName)
	}

	if err != nil {
		return nil, err
	}
	if !hasPerm {
		return &types.ValidationInfo{
			Flag: types.Flag_INVALID_NO_PERMISSION,
		}, nil
	}

	for _, read := range tx.Reads {
		_, metadata, err := v.db.Get(tx.DBName, read.Key)
		if err != nil {
			return nil, err
		}

		acl := metadata.GetAccessControl()
		if acl == nil {
			// we reach whem there is no existing entry or acl is not specified
			// for the read key. Hence, anyone who has read-write access
			// to the database can read the key
			continue
		}

		if !acl.ReadUsers[string(tx.UserID)] && !acl.ReadWriteUsers[string(tx.UserID)] {
			return &types.ValidationInfo{
				Flag: types.Flag_INVALID_NO_PERMISSION,
			}, nil
		}
	}

	for _, write := range tx.Writes {
		// TODO: move GetAccessControl API to the worldstate.DB interface
		_, metadata, err := v.db.Get(tx.DBName, write.Key)
		if err != nil {
			return nil, err
		}

		acl := metadata.GetAccessControl()
		if acl == nil {
			// we reach whem there is no existing entry or acl is not specified
			// for the existing entry. Hence, anyone who has read-write access
			// to the database can write the key
			continue
		}

		if !acl.ReadWriteUsers[string(tx.UserID)] {
			return &types.ValidationInfo{
				Flag: types.Flag_INVALID_NO_PERMISSION,
			}, nil
		}
	}

	return &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}, nil
}

func (v *validator) mvccValidation(tx *types.Transaction, pendingWrites map[string]bool) (*types.ValidationInfo, error) {
	valInfo := &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}

	for _, read := range tx.Reads {
		if pendingWrites[read.Key] {
			valInfo.Flag = types.Flag_INVALID_MVCC_CONFLICT
			return valInfo, nil
		}

		committedVersion, err := v.db.GetVersion(tx.DBName, read.Key)
		if err != nil {
			return nil, err
		}
		if proto.Equal(read.Version, committedVersion) {
			continue
		}

		valInfo.Flag = types.Flag_INVALID_MVCC_CONFLICT
		return valInfo, nil
	}

	return valInfo, nil
}
