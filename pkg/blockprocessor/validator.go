package blockprocessor

import (
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/identity"
)

// validator validates the each transaction read set present in a
// block against the committed version to ensure the requested
// isolation level
type validator struct {
	configTxValidator    *configTxValidator
	dbAdminTxValidator   *dbAdminTxValidator
	userAdminTxValidator *userAdminTxValidator
	dataTxValidator      *dataTxValidator
}

// newValidator creates a new validator
func newValidator(conf *Config) *validator {
	return &validator{
		configTxValidator: &configTxValidator{
			db:              conf.DB,
			identityQuerier: identity.NewQuerier(conf.DB),
		},

		dbAdminTxValidator: &dbAdminTxValidator{
			db:              conf.DB,
			identityQuerier: identity.NewQuerier(conf.DB),
		},

		userAdminTxValidator: &userAdminTxValidator{
			identityQuerier: identity.NewQuerier(conf.DB),
		},

		dataTxValidator: &dataTxValidator{
			db:              conf.DB,
			identityQuerier: identity.NewQuerier(conf.DB),
		},
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

	switch block.Payload.(type) {
	case *types.Block_DataTxEnvelopes:
		dataTx := block.GetDataTxEnvelopes().Envelopes
		valInfo := make([]*types.ValidationInfo, len(dataTx))
		pendingUpdates := make(map[string]bool)

		for txNum, tx := range dataTx {
			valRes, err := v.dataTxValidator.validate(tx.Payload, pendingUpdates)
			if err != nil {
				return nil, errors.WithMessage(err, "error while validating data transaction")
			}

			valInfo[txNum] = valRes
			if valRes.Flag == types.Flag_VALID {
				for _, w := range tx.Payload.DataWrites {
					pendingUpdates[w.Key] = true
				}

				for _, d := range tx.Payload.DataDeletes {
					pendingUpdates[d.Key] = true
				}
			}
		}

		return valInfo, nil

	case *types.Block_UserAdministrationTxEnvelope:
		valRes, err := v.userAdminTxValidator.validate(block.GetUserAdministrationTxEnvelope().Payload)
		if err != nil {
			return nil, errors.WithMessage(err, "error while validating user administrative transaction")
		}

		return []*types.ValidationInfo{
			valRes,
		}, nil

	case *types.Block_DBAdministrationTxEnvelope:
		valRes, err := v.dbAdminTxValidator.validate(block.GetDBAdministrationTxEnvelope().Payload)
		if err != nil {
			return nil, errors.WithMessage(err, "error while validating db administrative transaction")
		}

		return []*types.ValidationInfo{
			valRes,
		}, nil

	case *types.Block_ConfigTxEnvelope:
		valRes, err := v.configTxValidator.validate(block.GetConfigTxEnvelope().Payload)
		if err != nil {
			return nil, errors.WithMessage(err, "error while validating config transaction")
		}

		return []*types.ValidationInfo{
			valRes,
		}, nil

	default:
		return nil, errors.Errorf("unexpected transaction envelope in the block")
	}
}
