package blockprocessor

import (
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type committer struct {
	db         worldstate.DB
	blockStore *blockstore.Store
	// TODO
	// 1. Provenance Store
	// 2. Proof Store
}

func newCommitter(conf *Config) *committer {
	return &committer{
		db:         conf.DB,
		blockStore: conf.BlockStore,
	}
}

func (c *committer) commitBlock(block *types.Block, blockValidationInfo []*types.ValidationInfo) error {
	if err := c.commitToBlockStore(block); err != nil {
		return errors.WithMessagef(err, "error while committing block %d to the block store", block.Header.Number)
	}

	return c.commitToStateDB(block, blockValidationInfo)
	//TODO: add code to commit to provenance store
}

func (c *committer) commitToBlockStore(block *types.Block) error {
	return c.blockStore.Commit(block)
}

func (c *committer) commitToStateDB(block *types.Block, blockValidationInfo []*types.ValidationInfo) error {
	dbsUpdates := []*worldstate.DBUpdates{}
	for txNum, txValidationInfo := range blockValidationInfo {
		if txValidationInfo.Flag != types.Flag_VALID {
			continue
		}

		tx := block.TransactionEnvelopes[txNum].Payload
		if len(tx.Writes) == 0 {
			// maybe the http server can be made to throw
			// error when there is no write in a given
			// transaction. maybe it is good to record
			// only reads but couldn't think of a good
			// use-case and a trust model.
			// TODO: discuss with the team and make a
			// decision
			continue
		}

		version := &types.Version{
			BlockNum: block.Header.Number,
			TxNum:    uint64(txNum),
		}

		// TODO: move worldstate.UsersDBName and ConfigDBName to
		// the repo library and pkg types as they are common to
		// both server and sdk -- issue 97
		switch {
		case tx.DBName == worldstate.UsersDBName:
			dbsUpdates = append(
				dbsUpdates,
				identity.ConstructDBEntriesForUsers(tx, version),
			)
		// TODO: construct node and cluster identity entries when
		// the dbName is _config -- issue 98
		default:
			dbsUpdates = append(
				dbsUpdates,
				constructDBEntriesForData(tx, version),
			)
		}
	}

	if err := c.db.Commit(dbsUpdates); err != nil {
		return errors.WithMessagef(err, "failed to commit block %d to state database", block.Header.Number)
	}
	return nil
}

func constructDBEntriesForData(tx *types.Transaction, version *types.Version) *worldstate.DBUpdates {
	var kvWrites []*worldstate.KVWithMetadata
	var kvDeletes []string

	for _, write := range tx.Writes {
		if write.IsDelete {
			kvDeletes = append(kvDeletes, write.Key)
			continue
		}

		kv := &worldstate.KVWithMetadata{
			Key:   write.Key,
			Value: write.Value,
			Metadata: &types.Metadata{
				Version: version,
			},
		}
		kvWrites = append(kvWrites, kv)
	}

	return &worldstate.DBUpdates{
		DBName:  tx.DBName,
		Writes:  kvWrites,
		Deletes: kvDeletes,
	}
}
