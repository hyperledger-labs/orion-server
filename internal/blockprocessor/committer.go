package blockprocessor

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/internal/blockstore"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

const (
	userAdminTxIndex = 0
	dbAdminTxIndex   = 0
	configTxIndex    = 0
)

type committer struct {
	db         worldstate.DB
	blockStore *blockstore.Store
	logger     *logger.SugarLogger
	// TODO
	// 1. Provenance Store
	// 2. Proof Store
}

func newCommitter(conf *Config) *committer {
	return &committer{
		db:         conf.DB,
		blockStore: conf.BlockStore,
		logger:     conf.Logger,
	}
}

func (c *committer) commitBlock(block *types.Block) error {
	if err := c.commitToBlockStore(block); err != nil {
		return errors.WithMessagef(err, "error while committing block %d to the block store", block.Header.BaseHeader.Number)
	}

	return c.commitToStateDB(block)
	//TODO: add code to commit to provenance store
}

func (c *committer) commitToBlockStore(block *types.Block) error {
	return c.blockStore.Commit(block)
}

func (c *committer) commitToStateDB(block *types.Block) error {
	var dbsUpdates []*worldstate.DBUpdates
	blockValidationInfo := block.Header.ValidationInfo

	c.logger.Debugf("committing to the state changes from the block number %d", block.GetHeader().GetBaseHeader().GetNumber())
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
				constructDBEntriesForDataTx(tx, version),
			)
		}
		c.logger.Debugf("constructed %d, updates for data transactions, block number %d",
			len(blockValidationInfo),
			block.GetHeader().GetBaseHeader().GetNumber())

	case *types.Block_UserAdministrationTxEnvelope:
		if blockValidationInfo[userAdminTxIndex].Flag != types.Flag_VALID {
			return nil
		}

		version := &types.Version{
			BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
			TxNum:    userAdminTxIndex,
		}

		tx := block.GetUserAdministrationTxEnvelope().GetPayload()
		entries, err := identity.ConstructDBEntriesForUserAdminTx(tx, version)
		if err != nil {
			return errors.WithMessage(err, "error while creating entries for the user admin transaction")
		}

		dbsUpdates = []*worldstate.DBUpdates{entries}
		c.logger.Debugf("constructed user admin update, block number %d",
			block.GetHeader().GetBaseHeader().GetNumber())

	case *types.Block_DBAdministrationTxEnvelope:
		if blockValidationInfo[dbAdminTxIndex].Flag != types.Flag_VALID {
			return nil
		}

		version := &types.Version{
			BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
			TxNum:    dbAdminTxIndex,
		}

		tx := block.GetDBAdministrationTxEnvelope().GetPayload()
		dbsUpdates = []*worldstate.DBUpdates{
			constructDBEntriesForDBAdminTx(tx, version),
		}
		c.logger.Debugf("constructed db admin update, block number %d",
			block.GetHeader().GetBaseHeader().GetNumber())

	case *types.Block_ConfigTxEnvelope:
		if blockValidationInfo[configTxIndex].Flag != types.Flag_VALID {
			return nil
		}

		version := &types.Version{
			BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
			TxNum:    configTxIndex,
		}

		committedConfig, _, err := c.db.GetConfig()
		if err != nil {
			return errors.WithMessage(err, "error while fetching committed configuration")
		}

		tx := block.GetConfigTxEnvelope().GetPayload()
		entries, err := constructDBEntriesForConfigTx(tx, committedConfig, version)
		if err != nil {
			return errors.WithMessage(err, "error while constructing entries for the config transaction")
		}

		dbsUpdates = entries
		c.logger.Debugf("constructed configuration update, block number %d",
			block.GetHeader().GetBaseHeader().GetNumber())
	}

	blockNum := block.Header.BaseHeader.Number
	if err := c.db.Commit(dbsUpdates, blockNum); err != nil {
		return errors.WithMessagef(err, "failed to commit block %d to state database", blockNum)
	}
	return nil
}

func constructDBEntriesForDataTx(tx *types.DataTx, version *types.Version) *worldstate.DBUpdates {
	var kvWrites []*worldstate.KVWithMetadata
	var kvDeletes []string

	for _, write := range tx.DataWrites {
		kv := &worldstate.KVWithMetadata{
			Key:   write.Key,
			Value: write.Value,
			Metadata: &types.Metadata{
				Version:       version,
				AccessControl: write.ACL,
			},
		}
		kvWrites = append(kvWrites, kv)
	}

	for _, d := range tx.DataDeletes {
		kvDeletes = append(kvDeletes, d.Key)
	}

	return &worldstate.DBUpdates{
		DBName:  tx.DBName,
		Writes:  kvWrites,
		Deletes: kvDeletes,
	}
}

func constructDBEntriesForDBAdminTx(tx *types.DBAdministrationTx, version *types.Version) *worldstate.DBUpdates {
	var toCreateDBs []*worldstate.KVWithMetadata

	for _, dbName := range tx.CreateDBs {
		db := &worldstate.KVWithMetadata{
			Key: dbName,
			Metadata: &types.Metadata{
				Version: version,
			},
		}
		toCreateDBs = append(toCreateDBs, db)
	}

	return &worldstate.DBUpdates{
		DBName:  worldstate.DatabasesDBName,
		Writes:  toCreateDBs,
		Deletes: tx.DeleteDBs,
	}
}

func constructDBEntriesForConfigTx(tx *types.ConfigTx, oldConfig *types.ClusterConfig, version *types.Version) ([]*worldstate.DBUpdates, error) {
	adminUpdates, err := identity.ConstructDBEntriesForClusterAdmins(oldConfig.Admins, tx.NewConfig.Admins, version)
	if err != nil {
		return nil, err
	}

	newConfigSerialized, err := proto.Marshal(tx.NewConfig)
	if err != nil {
		return nil, errors.Wrap(err, "error while marshaling new configuration")
	}

	configUpdates := &worldstate.DBUpdates{
		DBName: worldstate.ConfigDBName,
		Writes: []*worldstate.KVWithMetadata{
			{
				Key:   worldstate.ConfigKey,
				Value: newConfigSerialized,
				Metadata: &types.Metadata{
					Version: version,
				},
			},
		},
	}

	return []*worldstate.DBUpdates{
		adminUpdates,
		configUpdates,
	}, nil
}
