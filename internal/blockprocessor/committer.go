// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/IBM-Blockchain/bcdb-server/internal/blockstore"
	"github.com/IBM-Blockchain/bcdb-server/internal/identity"
	"github.com/IBM-Blockchain/bcdb-server/internal/provenance"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

const (
	userAdminTxIndex = 0
	dbAdminTxIndex   = 0
	configTxIndex    = 0
)

type committer struct {
	db              worldstate.DB
	blockStore      *blockstore.Store
	provenanceStore *provenance.Store
	logger          *logger.SugarLogger
	// TODO
	// 2. Proof Store
}

func newCommitter(conf *Config) *committer {
	return &committer{
		db:              conf.DB,
		blockStore:      conf.BlockStore,
		provenanceStore: conf.ProvenanceStore,
		logger:          conf.Logger,
	}
}

func (c *committer) commitBlock(block *types.Block) error {
	if err := c.commitToBlockStore(block); err != nil {
		return errors.WithMessagef(
			err,
			"error while committing block %d to the block store",
			block.GetHeader().GetBaseHeader().GetNumber(),
		)
	}

	return c.commitToDBs(block)
}

func (c *committer) commitToBlockStore(block *types.Block) error {
	if err := c.blockStore.Commit(block); err != nil {
		return errors.WithMessagef(err, "failed to commit block %d to block store", block.Header.BaseHeader.Number)
	}

	return nil
}

func (c *committer) commitToDBs(block *types.Block) error {
	blockNum := block.GetHeader().GetBaseHeader().GetNumber()

	dbsUpdates, provenanceData, err := c.constructDBAndProvenanceEntries(block)
	if err != nil {
		return errors.WithMessagef(err, "error while constructing database and provenance entries for block %d", blockNum)
	}

	if err := c.commitToProvenanceStore(blockNum, provenanceData); err != nil {
		return errors.WithMessagef(err, "error while committing block %d to the block store", blockNum)
	}

	return c.commitToStateDB(blockNum, dbsUpdates)
}

func (c *committer) commitToProvenanceStore(blockNum uint64, provenanceData []*provenance.TxDataForProvenance) error {
	if err := c.provenanceStore.Commit(blockNum, provenanceData); err != nil {
		return errors.WithMessagef(err, "failed to commit block %d to provenance store", blockNum)
	}

	return nil
}

func (c *committer) commitToStateDB(blockNum uint64, dbsUpdates []*worldstate.DBUpdates) error {
	if err := c.db.Commit(dbsUpdates, blockNum); err != nil {
		return errors.WithMessagef(err, "failed to commit block %d to state database", blockNum)
	}

	return nil
}

func (c *committer) constructDBAndProvenanceEntries(block *types.Block) ([]*worldstate.DBUpdates, []*provenance.TxDataForProvenance, error) {
	var dbsUpdates []*worldstate.DBUpdates
	var provenanceData []*provenance.TxDataForProvenance
	dirtyWriteKeyVersion := make(map[string]*types.Version)
	blockValidationInfo := block.Header.ValidationInfo

	c.logger.Debugf("committing to the state changes from the block number %d", block.GetHeader().GetBaseHeader().GetNumber())
	switch block.Payload.(type) {
	case *types.Block_DataTxEnvelopes:
		txsEnvelopes := block.GetDataTxEnvelopes().Envelopes

		for txNum, txValidationInfo := range blockValidationInfo {
			if txValidationInfo.Flag != types.Flag_VALID {
				provenanceData = append(
					provenanceData,
					&provenance.TxDataForProvenance{
						IsValid: false,
						TxID:    txsEnvelopes[txNum].Payload.TxID,
					},
				)
				continue
			}

			version := &types.Version{
				BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
				TxNum:    uint64(txNum),
			}

			tx := txsEnvelopes[txNum].Payload

			// we pass the dirty write-set to the provenance entries constructor so that the
			// old version can be recorded to add previous value link in the provenance store
			pData, err := constructProvenanceEntriesForDataTx(c.db, tx, version, dirtyWriteKeyVersion)
			if err != nil {
				return nil, nil, err
			}
			provenanceData = append(provenanceData, pData...)

			toProcessUpdatesFromIndex := len(dbsUpdates)
			dbsUpdates = append(
				dbsUpdates,
				constructDBEntriesForDataTx(tx, version)...,
			)

			// after constructing entries for each transaction, we update the
			// dirty write set which holds the to be committed version of the key.
			// when more than one transaction in a block performs write to the
			// same key (through blind writes), the existing value in the dirty
			// set would get updated to reflect the new version
			for i := toProcessUpdatesFromIndex; i <= len(dbsUpdates)-1; i++ {
				dbUpdates := dbsUpdates[i]
				for _, w := range dbsUpdates[len(dbsUpdates)-1].Writes {
					dirtyWriteKeyVersion[constructCompositeKey(dbUpdates.DBName, w.Key)] = w.Metadata.Version
				}
			}
		}
		c.logger.Debugf("constructed %d, updates for data transactions, block number %d",
			len(blockValidationInfo),
			block.GetHeader().GetBaseHeader().GetNumber())

	case *types.Block_UserAdministrationTxEnvelope:
		if blockValidationInfo[userAdminTxIndex].Flag != types.Flag_VALID {
			return nil, []*provenance.TxDataForProvenance{
				{
					IsValid: false,
					TxID:    block.GetUserAdministrationTxEnvelope().GetPayload().GetTxID(),
				},
			}, nil
		}

		version := &types.Version{
			BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
			TxNum:    userAdminTxIndex,
		}

		tx := block.GetUserAdministrationTxEnvelope().GetPayload()
		entries, err := identity.ConstructDBEntriesForUserAdminTx(tx, version)
		if err != nil {
			return nil, nil, errors.WithMessage(err, "error while creating entries for the user admin transaction")
		}
		dbsUpdates = []*worldstate.DBUpdates{entries}

		pData, err := identity.ConstructProvenanceEntriesForUserAdminTx(tx, version, c.db)
		if err != nil {
			return nil, nil, errors.WithMessage(err, "error while creating provenance entries for the user admin transaction")
		}
		provenanceData = append(provenanceData, pData)

		c.logger.Debugf("constructed user admin update, block number %d",
			block.GetHeader().GetBaseHeader().GetNumber())

	case *types.Block_DBAdministrationTxEnvelope:
		if blockValidationInfo[dbAdminTxIndex].Flag != types.Flag_VALID {
			return nil, nil, nil
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
			return nil, []*provenance.TxDataForProvenance{
				{
					IsValid: false,
					TxID:    block.GetConfigTxEnvelope().GetPayload().GetTxID(),
				},
			}, nil
		}

		version := &types.Version{
			BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
			TxNum:    configTxIndex,
		}

		committedConfig, _, err := c.db.GetConfig()
		if err != nil {
			return nil, nil, errors.WithMessage(err, "error while fetching committed configuration")
		}

		tx := block.GetConfigTxEnvelope().GetPayload()
		entries, err := constructDBEntriesForConfigTx(tx, committedConfig, version)
		if err != nil {
			return nil, nil, errors.WithMessage(err, "error while constructing entries for the config transaction")
		}
		dbsUpdates = append(dbsUpdates, entries.configUpdates)
		if entries.adminUpdates != nil {
			dbsUpdates = append(dbsUpdates, entries.adminUpdates)
		}
		if entries.nodeUpdates != nil {
			dbsUpdates = append(dbsUpdates, entries.nodeUpdates)
		}

		pData, err := constructProvenanceEntriesForConfigTx(tx, version, entries, c.db)
		if err != nil {
			return nil, nil, errors.WithMessage(err, "error while creating provenance entries for the config transaction")
		}
		provenanceData = append(provenanceData, pData...)

		c.logger.Debugf("constructed configuration update, block number %d",
			block.GetHeader().GetBaseHeader().GetNumber())
	}

	return dbsUpdates, provenanceData, nil
}

func constructDBEntriesForDataTx(tx *types.DataTx, version *types.Version) []*worldstate.DBUpdates {
	dbsUpdates := make([]*worldstate.DBUpdates, len(tx.DBOperations))

	for i, ops := range tx.DBOperations {
		var kvWrites []*worldstate.KVWithMetadata
		var kvDeletes []string

		for _, write := range ops.DataWrites {
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

		for _, d := range ops.DataDeletes {
			kvDeletes = append(kvDeletes, d.Key)
		}

		dbsUpdates[i] = &worldstate.DBUpdates{
			DBName:  ops.DBName,
			Writes:  kvWrites,
			Deletes: kvDeletes,
		}
	}

	return dbsUpdates
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

type dbEntriesForConfigTx struct {
	adminUpdates  *worldstate.DBUpdates
	nodeUpdates   *worldstate.DBUpdates
	configUpdates *worldstate.DBUpdates
}

func constructDBEntriesForConfigTx(tx *types.ConfigTx, oldConfig *types.ClusterConfig, version *types.Version) (*dbEntriesForConfigTx, error) {
	adminUpdates, err := identity.ConstructDBEntriesForClusterAdmins(oldConfig.Admins, tx.NewConfig.Admins, version)
	if err != nil {
		return nil, err
	}

	nodeUpdates, err := identity.ConstructDBEntriesForNodes(oldConfig.Nodes, tx.NewConfig.Nodes, version)
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

	return &dbEntriesForConfigTx{
		adminUpdates:  adminUpdates,
		nodeUpdates:   nodeUpdates,
		configUpdates: configUpdates,
	}, nil
}

func constructProvenanceEntriesForDataTx(
	db worldstate.DB,
	tx *types.DataTx,
	version *types.Version,
	dirtyWriteKeyVersion map[string]*types.Version,
) ([]*provenance.TxDataForProvenance, error) {
	txpData := make([]*provenance.TxDataForProvenance, len(tx.DBOperations))

	for i, ops := range tx.DBOperations {
		pData := &provenance.TxDataForProvenance{
			IsValid:            true,
			DBName:             ops.DBName,
			UserID:             tx.MustSignUserIDs[0],
			TxID:               tx.TxID,
			Deletes:            make(map[string]*types.Version),
			OldVersionOfWrites: make(map[string]*types.Version),
		}

		for _, read := range ops.DataReads {
			k := &provenance.KeyWithVersion{
				Key:     read.Key,
				Version: read.Version,
			}
			pData.Reads = append(pData.Reads, k)
		}

		for _, write := range ops.DataWrites {
			kv := &types.KVWithMetadata{
				Key:   write.Key,
				Value: write.Value,
				Metadata: &types.Metadata{
					Version:       version,
					AccessControl: write.ACL,
				},
			}
			pData.Writes = append(pData.Writes, kv)

			// Given that two or more transaction within a
			// block can do blind write to a key, we need
			// to ensure that the old version is the one
			// written by the last transaction and not the
			// one present in the worldstate
			v, err := getVersion(ops.DBName, write.Key, dirtyWriteKeyVersion, db)
			if err != nil {
				return nil, err
			}
			if v == nil {
				continue
			}

			pData.OldVersionOfWrites[write.Key] = v
		}

		// we assume a block to delete a key only once. If more than
		// one transaction in a block deletes the same key, only the
		// first valid transaction gets committed while others get
		// invalidated.
		for _, d := range ops.DataDeletes {
			// as there can be a blind delete with previous transaction
			// in the same block writing the value, we need to first
			// consider the dirty set before fetching the version from
			// the worldstate
			v, err := getVersion(ops.DBName, d.Key, dirtyWriteKeyVersion, db)
			if err != nil {
				return nil, err
			}

			// for a delete to be valid, the value must exist and hence, the version will
			// never be nil
			pData.Deletes[d.Key] = v
		}

		txpData[i] = pData
	}

	return txpData, nil
}

func constructProvenanceEntriesForConfigTx(
	tx *types.ConfigTx,
	version *types.Version,
	updates *dbEntriesForConfigTx,
	db worldstate.DB,
) ([]*provenance.TxDataForProvenance, error) {
	configSerialized, err := proto.Marshal(tx.NewConfig)
	if err != nil {
		return nil, errors.Wrap(err, "error while marshaling new cluster configuration")
	}

	configTxPData := &provenance.TxDataForProvenance{
		IsValid: true,
		DBName:  worldstate.ConfigDBName,
		UserID:  tx.UserID,
		TxID:    tx.TxID,
		Writes: []*types.KVWithMetadata{
			{
				Key:   worldstate.ConfigKey,
				Value: configSerialized,
				Metadata: &types.Metadata{
					Version: version,
				},
			},
		},
		OldVersionOfWrites: make(map[string]*types.Version),
	}

	if tx.ReadOldConfigVersion != nil {
		configTxPData.OldVersionOfWrites[worldstate.ConfigKey] = tx.ReadOldConfigVersion
	}

	adminsPData, err := identity.ConstructProvenanceEntriesForClusterAdmins(tx.UserID, tx.TxID, updates.adminUpdates, db)
	if err != nil {
		return nil, errors.WithMessage(err, "error while constructing provenance entries for cluster admins")
	}

	nodesPData, err := identity.ConstructProvenanceEntriesForNodes(tx.UserID, tx.TxID, updates.nodeUpdates, db)
	if err != nil {
		return nil, errors.WithMessage(err, "error while constructing provenance entries for nodes")
	}

	return []*provenance.TxDataForProvenance{
		configTxPData,
		adminsPData,
		nodesPData,
	}, nil
}

func getVersion(
	dbName, key string,
	dirtyWriteKeyVersion map[string]*types.Version,
	db worldstate.DB,
) (*types.Version, error) {
	version, ok := dirtyWriteKeyVersion[constructCompositeKey(dbName, key)]
	if ok {
		return version, nil
	}

	return db.GetVersion(dbName, key)
}
