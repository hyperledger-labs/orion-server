package blockprocessor

import (
	"github.com/IBM-Blockchain/bcdb-server/internal/identity"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/pkg/errors"
)

func ConstructDBUpdatesForBlock(block *types.Block, db worldstate.DB) ([]*worldstate.DBUpdates, error) {
	var dbsUpdates []*worldstate.DBUpdates
	blockValidationInfo := block.GetHeader().GetValidationInfo()

	switch block.Payload.(type) {
	case *types.Block_DataTxEnvelopes:
		txsEnvelopes := block.GetDataTxEnvelopes().Envelopes

		for txNum, txValidationInfo := range blockValidationInfo {
			if txValidationInfo.Flag != types.Flag_VALID {
				continue
			}

			version := &types.Version{
				BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
				TxNum:    uint64(txNum),
			}

			tx := txsEnvelopes[txNum].Payload

			dbsUpdates = append(
				dbsUpdates,
				constructDBEntriesForDataTx(tx, version)...,
			)
		}

	case *types.Block_UserAdministrationTxEnvelope:
		if blockValidationInfo[userAdminTxIndex].Flag != types.Flag_VALID {
			return nil, nil
		}

		version := &types.Version{
			BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
			TxNum:    userAdminTxIndex,
		}

		tx := block.GetUserAdministrationTxEnvelope().GetPayload()
		entries, err := identity.ConstructDBEntriesForUserAdminTx(tx, version)
		if err != nil {
			return nil, errors.WithMessage(err, "error while creating entries for the user admin transaction")
		}
		dbsUpdates = []*worldstate.DBUpdates{entries}
	case *types.Block_DBAdministrationTxEnvelope:
		if blockValidationInfo[dbAdminTxIndex].Flag != types.Flag_VALID {
			return nil, nil
		}

		version := &types.Version{
			BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
			TxNum:    dbAdminTxIndex,
		}

		tx := block.GetDBAdministrationTxEnvelope().GetPayload()
		dbsUpdates = []*worldstate.DBUpdates{
			constructDBEntriesForDBAdminTx(tx, version),
		}
	case *types.Block_ConfigTxEnvelope:
		if blockValidationInfo[configTxIndex].Flag != types.Flag_VALID {
			return nil, nil
		}

		version := &types.Version{
			BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
			TxNum:    configTxIndex,
		}

		committedConfig, _, err := db.GetConfig()
		if err != nil {
			return nil, errors.WithMessage(err, "error while fetching committed configuration")
		}

		tx := block.GetConfigTxEnvelope().GetPayload()
		entries, err := constructDBEntriesForConfigTx(tx, committedConfig, version)
		if err != nil {
			return nil, errors.WithMessage(err, "error while constructing entries for the config transaction")
		}
		dbsUpdates = append(dbsUpdates, entries.configUpdates)
		if entries.adminUpdates != nil {
			dbsUpdates = append(dbsUpdates, entries.adminUpdates)
		}
		if entries.nodeUpdates != nil {
			dbsUpdates = append(dbsUpdates, entries.nodeUpdates)
		}
	}

	return dbsUpdates, nil
}
