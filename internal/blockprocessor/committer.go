// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"encoding/json"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/mptrie"
	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/internal/stateindex"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/state"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
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
	stateTrieStore  mptrie.Store
	stateTrie       *mptrie.MPTrie
	stats           *blockProcessorStats
	logger          *logger.SugarLogger
}

func newCommitter(conf *Config) *committer {
	return &committer{
		db:              conf.DB,
		blockStore:      conf.BlockStore,
		provenanceStore: conf.ProvenanceStore,
		stateTrieStore:  conf.StateTrieStore,
		stats:           stats,
		logger:          conf.Logger,
	}
}

func (c *committer) commitBlock(block *types.Block) error {
	// Calculate expected changes to world state db and provenance db
	start := time.Now()
	dbsUpdates, provenanceData, err := c.constructDBAndProvenanceEntries(block)
	if err != nil {
		return errors.WithMessagef(err, "error while constructing database and provenance entries for block %d", block.GetHeader().GetBaseHeader().GetNumber())
	}
	c.stats.updateCommitEntriesConstructionTime(time.Since(start))

	start = time.Now()
	// Update state trie with expected world state db changes
	if err := c.applyBlockOnStateTrie(dbsUpdates); err != nil {
		panic(err)
	}
	stateTrieRootHash, err := c.stateTrie.Hash()
	if err != nil {
		panic(err)
	}
	// Update block with state trie root
	block.Header.StateMerkelTreeRootHash = stateTrieRootHash
	c.stats.updateStateTrieUpdateTime(time.Since(start))

	start = time.Now()
	// Commit block to block store
	if err := c.commitToBlockStore(block); err != nil {
		return errors.WithMessagef(
			err,
			"error while committing block %d to the block store",
			block.GetHeader().GetBaseHeader().GetNumber(),
		)
	}
	c.stats.updateBlockStoreCommitTime(time.Since(start))

	// Commit block to world state db and provenance db
	if err = c.commitToDBs(dbsUpdates, provenanceData, block); err != nil {
		return err
	}

	start = time.Now()
	// Commit state trie changes to trie store
	if err = c.commitTrie(block.GetHeader().GetBaseHeader().GetNumber()); err != nil {
		return err
	}
	c.stats.updateStateTrieCommitTime(time.Since(start))

	return nil
}

func (c *committer) commitToBlockStore(block *types.Block) error {
	if err := c.blockStore.Commit(block); err != nil {
		return errors.WithMessagef(err, "failed to commit block %d to block store", block.Header.BaseHeader.Number)
	}

	return nil
}

func (c *committer) commitToDBs(dbsUpdates map[string]*worldstate.DBUpdates, provenanceData []*provenance.TxDataForProvenance, block *types.Block) error {
	blockNum := block.GetHeader().GetBaseHeader().GetNumber()

	start := time.Now()
	if err := c.commitToProvenanceStore(blockNum, provenanceData); err != nil {
		return errors.WithMessagef(err, "error while committing block %d to the block store", blockNum)
	}
	c.stats.updateProvenanceStoreCommitTime(time.Since(start))

	start = time.Now()
	if err := c.commitToStateDB(blockNum, dbsUpdates); err != nil {
		return err
	}
	c.stats.updateWorldStateCommitTime(time.Since(start))

	return nil
}

func (c *committer) commitToProvenanceStore(blockNum uint64, provenanceData []*provenance.TxDataForProvenance) error {
	if c.provenanceStore == nil {
		return nil
	}

	if err := c.provenanceStore.Commit(blockNum, provenanceData); err != nil {
		return errors.WithMessagef(err, "failed to commit block %d to provenance store", blockNum)
	}

	return nil
}

func (c *committer) commitToStateDB(blockNum uint64, dbsUpdates map[string]*worldstate.DBUpdates) error {
	indexUpdates, err := stateindex.ConstructIndexEntries(dbsUpdates, c.db)
	if err != nil {
		return errors.WithMessage(err, "failed to create index updates")
	}

	for indexDB, updates := range indexUpdates {
		// note that dbsUpdates will not contain any existing indexDB entries
		dbsUpdates[indexDB] = updates
	}

	if err := c.db.Commit(dbsUpdates, blockNum); err != nil {
		return errors.WithMessagef(err, "failed to commit block %d to state database", blockNum)
	}

	return nil
}

func (c *committer) constructDBAndProvenanceEntries(block *types.Block) (map[string]*worldstate.DBUpdates, []*provenance.TxDataForProvenance, error) {
	dbsUpdates := make(map[string]*worldstate.DBUpdates)
	var provenanceData []*provenance.TxDataForProvenance
	blockValidationInfo := block.Header.ValidationInfo

	c.logger.Debugf("committing to the state changes from the block number %d", block.GetHeader().GetBaseHeader().GetNumber())
	switch block.Payload.(type) {
	case *types.Block_DataTxEnvelopes:
		txsEnvelopes := block.GetDataTxEnvelopes().Envelopes

		for txNum, txValidationInfo := range blockValidationInfo {
			c.stats.incrementTransactionCount(txValidationInfo.Flag, "data_tx")
			if txValidationInfo.Flag != types.Flag_VALID {
				provenanceData = append(
					provenanceData,
					&provenance.TxDataForProvenance{
						IsValid: false,
						TxID:    txsEnvelopes[txNum].Payload.TxId,
					},
				)
				continue
			}

			version := &types.Version{
				BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
				TxNum:    uint64(txNum),
			}

			tx := txsEnvelopes[txNum].Payload

			pData, err := constructProvenanceEntriesForDataTx(c.db, tx, version)
			if err != nil {
				return nil, nil, err
			}
			provenanceData = append(provenanceData, pData...)

			AddDBEntriesForDataTx(tx, version, dbsUpdates)
		}
		c.logger.Debugf("constructed %d, updates for data transactions, block number %d",
			len(blockValidationInfo),
			block.GetHeader().GetBaseHeader().GetNumber())

	case *types.Block_UserAdministrationTxEnvelope:
		flag := blockValidationInfo[userAdminTxIndex].Flag
		c.stats.incrementTransactionCount(flag, "user_tx")
		if flag != types.Flag_VALID {
			return nil, []*provenance.TxDataForProvenance{
				{
					IsValid: false,
					TxID:    block.GetUserAdministrationTxEnvelope().GetPayload().GetTxId(),
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
		dbsUpdates[worldstate.UsersDBName] = entries

		pData, err := identity.ConstructProvenanceEntriesForUserAdminTx(tx, version, c.db)
		if err != nil {
			return nil, nil, errors.WithMessage(err, "error while creating provenance entries for the user admin transaction")
		}
		provenanceData = append(provenanceData, pData)

		c.logger.Debugf("constructed user admin update, block number %d",
			block.GetHeader().GetBaseHeader().GetNumber())

	case *types.Block_DbAdministrationTxEnvelope:
		flag := blockValidationInfo[dbAdminTxIndex].Flag
		c.stats.incrementTransactionCount(flag, "db_tx")
		if flag != types.Flag_VALID {
			return nil, nil, nil
		}

		version := &types.Version{
			BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
			TxNum:    dbAdminTxIndex,
		}

		tx := block.GetDbAdministrationTxEnvelope().GetPayload()
		var err error
		dbsUpdates[worldstate.DatabasesDBName], err = constructDBEntriesForDBAdminTx(tx, version, c.db)
		if err != nil {
			return nil, nil, errors.WithMessage(err, "error while creating entries for db admin transaction")
		}
		c.logger.Debugf("constructed db admin update, block number %d",
			block.GetHeader().GetBaseHeader().GetNumber())

	case *types.Block_ConfigTxEnvelope:
		flag := blockValidationInfo[configTxIndex].Flag
		c.stats.incrementTransactionCount(flag, "config_tx")
		if flag != types.Flag_VALID {
			return nil, []*provenance.TxDataForProvenance{
				{
					IsValid: false,
					TxID:    block.GetConfigTxEnvelope().GetPayload().GetTxId(),
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
		dbsUpdates[worldstate.ConfigDBName] = entries.configUpdates
		if entries.adminUpdates != nil {
			dbsUpdates[worldstate.UsersDBName] = entries.adminUpdates
		}
		if entries.nodeUpdates != nil {
			dbsUpdates[worldstate.ConfigDBName].Writes = append(dbsUpdates[worldstate.ConfigDBName].Writes, entries.nodeUpdates.Writes...)
			dbsUpdates[worldstate.ConfigDBName].Deletes = append(dbsUpdates[worldstate.ConfigDBName].Deletes, entries.nodeUpdates.Deletes...)
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

func (c *committer) applyBlockOnStateTrie(worldStateUpdates map[string]*worldstate.DBUpdates) error {
	return ApplyBlockOnStateTrie(c.stateTrie, worldStateUpdates)
}

func (c *committer) commitTrie(height uint64) error {
	return c.stateTrie.Commit(height)
}

func ApplyBlockOnStateTrie(trie *mptrie.MPTrie, worldStateUpdates map[string]*worldstate.DBUpdates) error {
	for dbName, dbUpdate := range worldStateUpdates {
		for _, dbWrite := range dbUpdate.Writes {
			key, err := state.ConstructCompositeKey(dbName, dbWrite.Key)
			if err != nil {
				return err
			}
			// TODO: should we add Metadata to value
			value := dbWrite.Value
			err = trie.Update(key, value)
			if err != nil {
				return err
			}
		}
		for _, dbDelete := range dbUpdate.Deletes {
			key, err := state.ConstructCompositeKey(dbName, dbDelete)
			if err != nil {
				return err
			}
			_, err = trie.Delete(key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func AddDBEntriesForDataTx(tx *types.DataTx, version *types.Version, dbsUpdates map[string]*worldstate.DBUpdates) {
	for _, ops := range tx.DbOperations {
		updates, ok := dbsUpdates[ops.DbName]
		if !ok {
			updates = &worldstate.DBUpdates{}
			dbsUpdates[ops.DbName] = updates
		}

		for _, write := range ops.DataWrites {
			kv := &worldstate.KVWithMetadata{
				Key:   write.Key,
				Value: write.Value,
				Metadata: &types.Metadata{
					Version:       version,
					AccessControl: write.Acl,
				},
			}
			updates.Writes = append(updates.Writes, kv)
		}

		for _, d := range ops.DataDeletes {
			updates.Deletes = append(updates.Deletes, d.Key)
		}
	}
}

func constructDBEntriesForDBAdminTx(tx *types.DBAdministrationTx, version *types.Version, db worldstate.DB) (*worldstate.DBUpdates, error) {
	var indexForExistingDBs []*worldstate.KVWithMetadata

	toCreateDBs, err := createEntriesForNewDBs(tx.CreateDbs, tx.DbsIndex, version)
	if err != nil {
		return nil, err
	}

	indexForExistingDBs, toDeleteIndexDBs, err := createEntriesForIndexUpdates(tx.DbsIndex, db, version)
	if err != nil {
		return nil, err
	}

	return &worldstate.DBUpdates{
		Writes:  append(toCreateDBs, indexForExistingDBs...),
		Deletes: append(tx.DeleteDbs, toDeleteIndexDBs...),
	}, nil
}

func createEntriesForNewDBs(newDBs []string, dbsIndex map[string]*types.DBIndex, version *types.Version) ([]*worldstate.KVWithMetadata, error) {
	var toCreateDBs []*worldstate.KVWithMetadata
	var err error

	for _, dbName := range newDBs {
		createDB := &worldstate.KVWithMetadata{
			Key: dbName,
			Metadata: &types.Metadata{
				Version: version,
			},
		}
		toCreateDBs = append(toCreateDBs, createDB)

		dbIndex, indexIsDefined := dbsIndex[dbName]
		if indexIsDefined {
			createDB.Value, err = json.Marshal(dbIndex.GetAttributeAndType())
			if err != nil {
				return nil, errors.Wrap(err, "error while marshaling index for database ["+dbName+"]")
			}

			// for each DB, if index is defined, we need to create an
			// index DB to store index entries for that DB
			indexDB := &worldstate.KVWithMetadata{
				Key: stateindex.IndexDB(dbName),
				Metadata: &types.Metadata{
					Version: version,
				},
			}
			toCreateDBs = append(toCreateDBs, indexDB)

			// delete the processed index. This will leave us with
			// new index for the existing database
			delete(dbsIndex, dbName)
		}
	}

	return toCreateDBs, nil
}

func createEntriesForIndexUpdates(
	dbsIndex map[string]*types.DBIndex,
	db worldstate.DB,
	version *types.Version,
) ([]*worldstate.KVWithMetadata, []string, error) {
	var indexForExistingDBs []*worldstate.KVWithMetadata
	var toDeleteDBs []string
	var err error

	for dbName, dbIndex := range dbsIndex {
		indexExist := db.Exist(stateindex.IndexDB(dbName))
		deleteExistingIndex := dbIndex == nil || dbIndex.GetAttributeAndType() == nil

		updateDBIndex := &worldstate.KVWithMetadata{
			Key:   dbName,
			Value: nil,
			Metadata: &types.Metadata{
				Version: version,
			},
		}

		if !indexExist && deleteExistingIndex {
			continue
		} else if indexExist && deleteExistingIndex {
			toDeleteDBs = append(toDeleteDBs, stateindex.IndexDB(dbName))
		} else if indexExist && !deleteExistingIndex {
			updateDBIndex.Value, err = json.Marshal(dbIndex.GetAttributeAndType())
			if err != nil {
				return nil, nil, errors.Wrap(err, "error while marshaling index for database ["+dbName+"]")
			}
		} else { // !indexExist && !deleteExistingIndex
			updateDBIndex.Value, err = json.Marshal(dbIndex.GetAttributeAndType())
			if err != nil {
				return nil, nil, errors.Wrap(err, "error while marshaling index for database ["+dbName+"]")
			}

			// as there is no existing index, we need to create the index database
			indexDB := &worldstate.KVWithMetadata{
				Key: stateindex.IndexDB(dbName),
				Metadata: &types.Metadata{
					Version: version,
				},
			}
			indexForExistingDBs = append(indexForExistingDBs, indexDB)
		}
		indexForExistingDBs = append(indexForExistingDBs, updateDBIndex)
	}

	return indexForExistingDBs, toDeleteDBs, nil
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

	// calculate the new max RaftID from the peers and old config
	maxID := oldConfig.GetConsensusConfig().GetRaftConfig().GetMaxRaftId()
	// avoid mutating the tx, as it continues to be processed later
	newConfigClone := proto.Clone(tx.NewConfig).(*types.ClusterConfig)
	for _, m := range newConfigClone.GetConsensusConfig().GetMembers() {
		if id := m.GetRaftId(); id > maxID {
			maxID = id
		}
	}
	newConfigClone.ConsensusConfig.RaftConfig.MaxRaftId = maxID

	newConfigSerialized, err := proto.Marshal(newConfigClone)
	if err != nil {
		return nil, errors.Wrap(err, "error while marshaling new configuration")
	}

	configUpdates := &worldstate.DBUpdates{
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

func constructProvenanceEntriesForDataTx(db worldstate.DB, tx *types.DataTx, version *types.Version) ([]*provenance.TxDataForProvenance, error) {
	txpData := make([]*provenance.TxDataForProvenance, len(tx.DbOperations))

	for i, ops := range tx.DbOperations {
		pData := &provenance.TxDataForProvenance{
			IsValid:            true,
			DBName:             ops.DbName,
			UserID:             tx.MustSignUserIds[0],
			TxID:               tx.TxId,
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
					AccessControl: write.Acl,
				},
			}
			pData.Writes = append(pData.Writes, kv)

			// we assume a block to write a key only once. If more than
			// one transaction in a block writes to the same key (blind write),
			// only the first valid transaction gets committed while others get
			// invalidated. Hence, the old version of the key can only exist in
			// the committed state and not in the pending writes of previous
			// transactions within the block
			v, err := db.GetVersion(ops.DbName, write.Key)
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
			v, err := db.GetVersion(ops.DbName, d.Key)
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
		UserID:  tx.UserId,
		TxID:    tx.TxId,
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

	adminsPData, err := identity.ConstructProvenanceEntriesForClusterAdmins(tx.UserId, tx.TxId, updates.adminUpdates, db)
	if err != nil {
		return nil, errors.WithMessage(err, "error while constructing provenance entries for cluster admins")
	}

	nodesPData, err := identity.ConstructProvenanceEntriesForNodes(tx.UserId, tx.TxId, updates.nodeUpdates, db)
	if err != nil {
		return nil, errors.WithMessage(err, "error while constructing provenance entries for nodes")
	}

	return []*provenance.TxDataForProvenance{
		configTxPData,
		adminsPData,
		nodesPData,
	}, nil
}
