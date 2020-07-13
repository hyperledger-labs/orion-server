package committer

import (
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type Committer struct {
	db worldstate.DB
	// TODO
	// 1. Block Store
	// 2. Provenance Store
	// 3. Proof Store
}

func NewCommitter(db worldstate.DB) *Committer {
	return &Committer{
		db: db,
	}
}

func (c *Committer) Commit(block *api.Block, blockValidationInfo []*api.ValidationInfo) error {
	return c.commitToStateDB(block, blockValidationInfo)
	//TODO: add code to commit to block store and provenance store
}

func (c *Committer) commitToStateDB(block *api.Block, blockValidationInfo []*api.ValidationInfo) error {
	dbsUpdates := []*worldstate.DBUpdates{}
	for txNum, txValidationInfo := range blockValidationInfo {
		if txValidationInfo.Flag != api.Flag_VALID {
			continue
		}

		tx := block.TransactionEnvelopes[txNum].Payload
		kvWrites := []*worldstate.KV{}
		kvDeletes := []string{}

		for _, write := range tx.Writes {
			if write.IsDelete {
				kvDeletes = append(kvDeletes, write.Key)
				continue
			}

			kv := &worldstate.KV{
				Key: write.Key,
				Value: &api.Value{
					Value: write.Value,
					Metadata: &api.Metadata{
						Version: &api.Version{
							BlockNum: block.Header.Number,
							TxNum:    uint64(txNum),
						},
					},
				},
			}
			kvWrites = append(kvWrites, kv)
		}

		dbUpdate := &worldstate.DBUpdates{
			DBName:  tx.DBName,
			Writes:  kvWrites,
			Deletes: kvDeletes,
		}
		dbsUpdates = append(dbsUpdates, dbUpdate)
	}

	if err := c.db.Commit(dbsUpdates); err != nil {
		return errors.WithMessagef(err, "failed to commit block %d to state database", block.Header.Number)
	}
	return nil
}
