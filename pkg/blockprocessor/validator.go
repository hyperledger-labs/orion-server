package blockprocessor

import (
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/logger"
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
	logger               *logger.SugarLogger
}

// newValidator creates a new validator
func newValidator(conf *Config) *validator {
	return &validator{
		configTxValidator: &configTxValidator{
			db:              conf.DB,
			identityQuerier: identity.NewQuerier(conf.DB),
			logger:          conf.Logger,
		},

		dbAdminTxValidator: &dbAdminTxValidator{
			db:              conf.DB,
			identityQuerier: identity.NewQuerier(conf.DB),
			logger:          conf.Logger,
		},

		userAdminTxValidator: &userAdminTxValidator{
			db:              conf.DB,
			identityQuerier: identity.NewQuerier(conf.DB),
			logger:          conf.Logger,
		},

		dataTxValidator: &dataTxValidator{
			db:              conf.DB,
			identityQuerier: identity.NewQuerier(conf.DB),
			logger:          conf.Logger,
		},

		logger: conf.Logger,
	}
}

// validateBlock validates each transaction present in the block to ensure
// the request isolation level
func (v *validator) validateBlock(block *types.Block) ([]*types.ValidationInfo, error) {
	if block.Header.Number == 1 {
		// for the genesis block, which is created by the node itself, we cannot
		// do a regular validation but we still needs to validate the entries
		configTx := block.GetConfigTxEnvelope().Payload

		if r := validateNodeConfig(configTx.NewConfig.Nodes); r.Flag != types.Flag_VALID {
			return nil, errors.Errorf("genesis block cannot be invalid: reason for invalidation [%s]", r.ReasonIfInvalid)
		}

		if r := validateAdminConfig(configTx.NewConfig.Admins); r.Flag != types.Flag_VALID {
			return nil, errors.Errorf("genesis block cannot be invalid: reason for invalidation [%s]", r.ReasonIfInvalid)
		}

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
			if valRes.Flag != types.Flag_VALID {
				v.logger.Debugf("data transaction [%v] is invalid due to [%s]", tx.Payload, valRes.ReasonIfInvalid)
				continue
			}

			for _, w := range tx.Payload.DataWrites {
				pendingUpdates[w.Key] = true
			}

			for _, d := range tx.Payload.DataDeletes {
				pendingUpdates[d.Key] = true
			}
		}

		return valInfo, nil

	case *types.Block_UserAdministrationTxEnvelope:
		userTx := block.GetUserAdministrationTxEnvelope().Payload
		valRes, err := v.userAdminTxValidator.validate(userTx)
		if err != nil {
			return nil, errors.WithMessage(err, "error while validating user administrative transaction")
		}

		if valRes.Flag != types.Flag_VALID {
			v.logger.Debugf("user administration transaction [%v] is invalid due to [%s]", userTx, valRes.ReasonIfInvalid)
		}

		return []*types.ValidationInfo{
			valRes,
		}, nil

	case *types.Block_DBAdministrationTxEnvelope:
		dbTx := block.GetDBAdministrationTxEnvelope().Payload
		valRes, err := v.dbAdminTxValidator.validate(dbTx)
		if err != nil {
			return nil, errors.WithMessage(err, "error while validating db administrative transaction")
		}

		if valRes.Flag != types.Flag_VALID {
			v.logger.Debugf("database administration transaction [%v] is invalid due to [%s]", dbTx, valRes.ReasonIfInvalid)
		}

		return []*types.ValidationInfo{
			valRes,
		}, nil

	case *types.Block_ConfigTxEnvelope:
		configTx := block.GetConfigTxEnvelope().Payload
		valRes, err := v.configTxValidator.validate(configTx)
		if err != nil {
			return nil, errors.WithMessage(err, "error while validating config transaction")
		}

		if valRes.Flag != types.Flag_VALID {
			v.logger.Debugf("cluster config transaction [%v] is invalid due to [%s]", configTx, valRes.ReasonIfInvalid)
		}

		return []*types.ValidationInfo{
			valRes,
		}, nil

	default:
		return nil, errors.Errorf("unexpected transaction envelope in the block")
	}
}
