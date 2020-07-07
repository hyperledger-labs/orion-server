package txisolation

import (
	"github.com/golang/protobuf/proto"
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type Validator struct {
	db worldstate.DB
}

func NewValidator(db worldstate.DB) *Validator {
	return &Validator{
		db: db,
	}

}

func (v *Validator) ValidateBlock(block *api.Block) ([]*api.ValidationInfo, error) {
	var err error
	valInfo := make([]*api.ValidationInfo, len(block.TransactionEnvelopes))
	pendingWrites := make(map[string]bool)

	for txIndex, tx := range block.TransactionEnvelopes {
		if err = v.db.Open(tx.Payload.DBName); err != nil {
			valInfo[txIndex] = &api.ValidationInfo{
				Flag: api.Flag_INVALID_DB_NOT_EXIST,
			}
			continue
		}

		if valInfo[txIndex], err = v.mvccValidation(tx.Payload, pendingWrites); err != nil {
			return nil, err
		}
		if valInfo[txIndex].Flag == api.Flag_VALID {
			for _, write := range tx.Payload.Writes {
				pendingWrites[write.Key] = true
			}
		}
	}

	return valInfo, nil
}

func (v *Validator) mvccValidation(tx *api.Transaction, pendingWrites map[string]bool) (*api.ValidationInfo, error) {
	valInfo := &api.ValidationInfo{
		Flag: api.Flag_VALID,
	}

	for _, read := range tx.Reads {
		if pendingWrites[read.Key] {
			valInfo.Flag = api.Flag_INVALID_MVCC_CONFLICT
			return valInfo, nil
		}

		committedVersion, err := v.db.GetVersion(tx.DBName, read.Key)
		if err != nil {
			return nil, err
		}
		if proto.Equal(read.Version, committedVersion) {
			continue
		}

		valInfo.Flag = api.Flag_INVALID_MVCC_CONFLICT
		return valInfo, nil
	}

	return valInfo, nil
}
