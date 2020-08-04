package blockprocessor

import (
	"github.com/golang/protobuf/proto"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

// validator validates the each transaction read set present in a
// block against the committed version to ensure the requested
// isolation level
type validator struct {
	db worldstate.DB
}

// newValidator creates a new validator
func newValidator(db worldstate.DB) *validator {
	return &validator{
		db: db,
	}
}

// validateBlock validates each transaction present in the block to ensure
// the request isolation level
func (v *validator) validateBlock(block *types.Block) ([]*types.ValidationInfo, error) {
	var err error
	valInfo := make([]*types.ValidationInfo, len(block.TransactionEnvelopes))
	pendingWrites := make(map[string]bool)

	for txIndex, tx := range block.TransactionEnvelopes {
		if err = v.db.Open(tx.Payload.DBName); err != nil {
			valInfo[txIndex] = &types.ValidationInfo{
				Flag: types.Flag_INVALID_DB_NOT_EXIST,
			}
			continue
		}

		// TODO
		// We need to ensure that a config transaction is submitted only by the admin
		// and it should have the correct database, i.e., _config and correct key.

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
