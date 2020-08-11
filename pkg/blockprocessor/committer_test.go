package blockprocessor

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type committerTestEnv struct {
	db        *leveldb.LevelDB
	path      string
	committer *committer
	cleanup   func()
}

func newCommitterTestEnv(t *testing.T) *committerTestEnv {
	dir, err := ioutil.TempDir("/tmp", "committer")
	require.NoError(t, err)

	path := filepath.Join(dir, "leveldb")
	db, err := leveldb.New(path)
	if err != nil {
		if err := os.RemoveAll(dir); err != nil {
			t.Logf("failed to remove directory %s, %v", dir, err)
		}
		t.Fatalf("failed to create leveldb with path %s", path)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close the db instance, %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove directory %s, %v", dir, err)
		}
	}

	return &committerTestEnv{
		db:        db,
		path:      path,
		committer: newCommitter(db),
		cleanup:   cleanup,
	}
}

func TestStateDBCommitter(t *testing.T) {
	t.Parallel()

	setup := func(db worldstate.DB) []*worldstate.DBUpdates {
		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "db1",
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   "db1-key1",
						Value: []byte("db1-value1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    1,
							},
						},
					},
					{
						Key:   "db1-key2",
						Value: []byte("db1-value2"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    2,
							},
						},
					},
				},
			},
			{
				DBName: "db2",
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   "db2-key1",
						Value: []byte("db2-value1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    3,
							},
						},
					},
					{
						Key:   "db2-key2",
						Value: []byte("db2-value2"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    4,
							},
						},
					},
				},
			},
		}
		require.NoError(t, db.Create("db1"))
		require.NoError(t, db.Create("db2"))
		require.NoError(t, db.Commit(dbsUpdates))
		return dbsUpdates
	}

	t.Run("commit block to replace all existing entries", func(t *testing.T) {
		t.Parallel()
		env := newCommitterTestEnv(t)
		defer env.cleanup()
		initialKVsPerDB := setup(env.db)

		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, metadata, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.Equal(t, kv.Value, val)
				require.True(t, proto.Equal(kv.Metadata, metadata))
			}
		}

		// create a block to update all existing entries in the database
		// In db1, we update db1-key1, db1-key2
		// In db2, we update db2-key1, db2-key2
		block := &types.Block{
			Header: &types.BlockHeader{
				Number: 2,
			},
			TransactionEnvelopes: []*types.TransactionEnvelope{
				{
					Payload: &types.Transaction{
						DBName: "db1",
						Writes: []*types.KVWrite{
							{
								Key:   "db1-key1",
								Value: []byte("new-value-1"),
							},
						},
					},
				},
				{
					Payload: &types.Transaction{
						DBName: "db1",
						Writes: []*types.KVWrite{
							{
								Key:   "db1-key2",
								Value: []byte("new-value-2"),
							},
						},
					},
				},
				{
					Payload: &types.Transaction{
						DBName: "db2",
						Writes: []*types.KVWrite{
							{
								Key:   "db2-key1",
								Value: []byte("new-value-1"),
							},
						},
					},
				},
				{
					Payload: &types.Transaction{
						DBName: "db2",
						Writes: []*types.KVWrite{
							{
								Key:   "db2-key2",
								Value: []byte("new-value-2"),
							},
						},
					},
				},
			},
		}

		validationInfo := []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_VALID,
			},
		}

		require.NoError(t, env.committer.commitBlock(block, validationInfo))

		// as the last block commit has updated all existing entries,
		// kvs in initialKVsPerDB should not match with the committed versions
		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, metadata, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.NotEqual(t, kv.Value, val)
				require.False(t, proto.Equal(kv.Metadata, metadata))
			}
		}

		val, metadata, err := env.db.Get("db1", "db1-key1")
		require.NoError(t, err)
		expectedVal := []byte("new-value-1")
		expectedMetadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    0,
			},
		}
		require.Equal(t, expectedVal, val)
		require.True(t, proto.Equal(expectedMetadata, metadata))

		val, metadata, err = env.db.Get("db1", "db1-key2")
		require.NoError(t, err)
		expectedVal = []byte("new-value-2")
		expectedMetadata = &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
		}
		require.Equal(t, expectedVal, val)
		require.True(t, proto.Equal(expectedMetadata, metadata))

		val, metadata, err = env.db.Get("db2", "db2-key1")
		require.NoError(t, err)
		expectedVal = []byte("new-value-1")
		expectedMetadata = &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    2,
			},
		}
		require.Equal(t, expectedVal, val)
		require.True(t, proto.Equal(expectedMetadata, metadata))

		val, metadata, err = env.db.Get("db2", "db2-key2")
		require.NoError(t, err)
		expectedVal = []byte("new-value-2")
		expectedMetadata = &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    3,
			},
		}
		require.Equal(t, expectedVal, val)
		require.True(t, proto.Equal(expectedMetadata, metadata))
	})

	t.Run("commit block to delete all existing entries", func(t *testing.T) {
		t.Parallel()
		env := newCommitterTestEnv(t)
		defer env.cleanup()
		initialKVsPerDB := setup(env.db)

		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, metadata, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.Equal(t, kv.Value, val)
				require.True(t, proto.Equal(kv.Metadata, metadata))
			}
		}

		// create a block to delete all existing entries in the database
		// In db1, we delete db1-key1, db1-key2
		// In db2, we delete db2-key1, db2-key2
		block := &types.Block{
			Header: &types.BlockHeader{
				Number: 2,
			},
			TransactionEnvelopes: []*types.TransactionEnvelope{
				{
					Payload: &types.Transaction{
						DBName: "db1",
						Writes: []*types.KVWrite{
							{
								Key:      "db1-key1",
								IsDelete: true,
							},
							{
								Key:      "db1-key2",
								IsDelete: true,
							},
						},
					},
				},
				{
					Payload: &types.Transaction{
						DBName: "db2",
						Writes: []*types.KVWrite{
							{
								Key:      "db2-key1",
								IsDelete: true,
							},
							{
								Key:      "db2-key2",
								IsDelete: true,
							},
						},
					},
				},
			},
		}

		validationInfo := []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_VALID,
			},
		}

		require.NoError(t, env.committer.commitBlock(block, validationInfo))

		// as the last block commit has deleted all existing entries,
		// kvs in initialKVsPerDB should not match with the committed versions
		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, metadata, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.NotEqual(t, kv.Value, val)
				require.False(t, proto.Equal(kv.Metadata, metadata))
			}
		}

		val, metadata, err := env.db.Get("db1", "db1-key1")
		require.NoError(t, err)
		require.Nil(t, val)
		require.Nil(t, metadata)

		val, metadata, err = env.db.Get("db1", "db1-key2")
		require.NoError(t, err)
		require.Nil(t, val)
		require.Nil(t, metadata)

		val, metadata, err = env.db.Get("db1", "db2-key1")
		require.NoError(t, err)
		require.Nil(t, val)
		require.Nil(t, metadata)

		val, metadata, err = env.db.Get("db1", "db2-key2")
		require.NoError(t, err)
		require.Nil(t, val)
		require.Nil(t, metadata)
	})

	t.Run("commit block to only insert new entries", func(t *testing.T) {
		t.Parallel()
		env := newCommitterTestEnv(t)
		defer env.cleanup()
		initialKVsPerDB := setup(env.db)

		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, metadata, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.Equal(t, kv.Value, val)
				require.True(t, proto.Equal(kv.Metadata, metadata))
			}
		}

		// create a block to insert new entries without touching the
		// existing entries in the database
		// In db1, insert db1-key3, db1-key4
		// In db2, insert db2-key3, db2-key4
		block := &types.Block{
			Header: &types.BlockHeader{
				Number: 2,
			},
			TransactionEnvelopes: []*types.TransactionEnvelope{
				{
					Payload: &types.Transaction{
						DBName: "db1",
						Writes: []*types.KVWrite{
							{
								Key:   "db1-key3",
								Value: []byte("value-3"),
							},
							{
								Key:   "db1-key4",
								Value: []byte("value-4"),
							},
						},
					},
				},
				{
					Payload: &types.Transaction{
						DBName: "db2",
						Writes: []*types.KVWrite{
							{
								Key:   "db2-key3",
								Value: []byte("value-3"),
							},
							{
								Key:   "db2-key4",
								Value: []byte("value-4"),
							},
						},
					},
				},
			},
		}

		validationInfo := []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_VALID,
			},
		}

		require.NoError(t, env.committer.commitBlock(block, validationInfo))

		// as the last block commit has not modified existing entries,
		// kvs in initialKVsPerDB should match with the committed versions
		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, metadata, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.Equal(t, kv.Value, val)
				require.True(t, proto.Equal(kv.Metadata, metadata))
			}
		}

		val, metadata, err := env.db.Get("db1", "db1-key3")
		require.NoError(t, err)
		expectedVal := []byte("value-3")
		expectedMetadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    0,
			},
		}
		require.Equal(t, expectedVal, val)
		require.True(t, proto.Equal(expectedMetadata, metadata))

		val, metadata, err = env.db.Get("db1", "db1-key4")
		require.NoError(t, err)
		expectedVal = []byte("value-4")
		expectedMetadata = &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    0,
			},
		}
		require.Equal(t, expectedVal, val)
		require.True(t, proto.Equal(expectedMetadata, metadata))

		val, metadata, err = env.db.Get("db2", "db2-key3")
		require.NoError(t, err)
		expectedVal = []byte("value-3")
		expectedMetadata = &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
		}
		require.Equal(t, expectedVal, val)
		require.True(t, proto.Equal(expectedMetadata, metadata))

		val, metadata, err = env.db.Get("db2", "db2-key4")
		require.NoError(t, err)
		expectedVal = []byte("value-4")
		expectedMetadata = &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
		}
		require.Equal(t, expectedVal, val)
		require.True(t, proto.Equal(expectedMetadata, metadata))
	})

	t.Run("commit block to update and delete existing entries while inserting new", func(t *testing.T) {
		t.Parallel()
		env := newCommitterTestEnv(t)
		defer env.cleanup()
		initialKVsPerDB := setup(env.db)

		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, metadata, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.Equal(t, kv.Value, val)
				require.True(t, proto.Equal(kv.Metadata, metadata))
			}
		}

		// create a block to update & delete existing entries in the database
		// add a new entry
		// In db1, we delete db1-key1, update db1-key2, newly add db1-key3
		// In db2, we update db2-key1, delete db2-key2, newly add db2-key3
		block := &types.Block{
			Header: &types.BlockHeader{
				Number: 10,
			},
			TransactionEnvelopes: []*types.TransactionEnvelope{
				{
					// we mark this transaction valid
					Payload: &types.Transaction{
						DBName: "db1",
						Writes: []*types.KVWrite{
							{
								Key:      "db1-key1",
								IsDelete: true,
							},
							{
								Key:   "db1-key2",
								Value: []byte("new-value-2"),
							},
							{
								Key:   "db1-key3",
								Value: []byte("value-3"),
							},
							{
								Key:      "db1-key4",
								IsDelete: true,
							},
						},
					},
				},
				{
					// we mark this transaction invalid
					Payload: &types.Transaction{
						DBName: "db3",
						Writes: []*types.KVWrite{
							{
								Key:   "db3-key2",
								Value: []byte("value-2"),
							},
						},
					},
				},
				{
					// we mark this transaction valid
					Payload: &types.Transaction{
						DBName: "db2",
						Reads: []*types.KVRead{
							{
								Key: "db2-key1",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    3,
								},
							},
						},
						Writes: []*types.KVWrite{
							{
								Key:   "db2-key1",
								Value: []byte("new-value-1"),
							},
						},
					},
				},
				{
					// we mark this transaction valid
					Payload: &types.Transaction{
						DBName: "db2",
						Writes: []*types.KVWrite{
							{
								Key:      "db2-key2",
								IsDelete: true,
							},
							{
								Key:   "db2-key3",
								Value: []byte("value-3"),
							},
						},
					},
				},
				{
					// we mark this transaction valid
					Payload: &types.Transaction{
						DBName: "db2",
						Writes: []*types.KVWrite{},
					},
				},
				{
					// we mark this transaction invalid
					Payload: &types.Transaction{
						DBName: "db2",
						Writes: []*types.KVWrite{
							{
								Key:      "db2-key2",
								IsDelete: true,
							},
							{
								Key:   "db2-key3",
								Value: []byte("value-3"),
							},
						},
					},
				},
			},
		}

		validationInfo := []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_INVALID_DB_NOT_EXIST,
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_INVALID_MVCC_CONFLICT,
			},
		}

		require.NoError(t, env.committer.commitBlock(block, validationInfo))

		// as the last block commit has either updated or deleted
		// existing entries, kvs in initialKVsPerDB should not
		// match with the committed versions
		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, metadata, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.NotEqual(t, kv.Value, val)
				require.False(t, proto.Equal(kv.Metadata, metadata))
			}
		}

		// In db1, we delete db1-key1, update db1-key2, newly add db1-key3
		val, metadata, err := env.db.Get("db1", "db1-key1")
		require.NoError(t, err)
		require.Nil(t, val)
		require.Nil(t, metadata)

		val, metadata, err = env.db.Get("db1", "db1-key2")
		require.NoError(t, err)
		expectedVal := []byte("new-value-2")
		expectedMetadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 10,
				TxNum:    0,
			},
		}
		require.Equal(t, expectedVal, val)
		require.True(t, proto.Equal(expectedMetadata, metadata))

		val, metadata, err = env.db.Get("db1", "db1-key3")
		require.NoError(t, err)
		expectedVal = []byte("value-3")
		expectedMetadata = &types.Metadata{
			Version: &types.Version{
				BlockNum: 10,
				TxNum:    0,
			},
		}
		require.Equal(t, expectedVal, val)
		require.True(t, proto.Equal(expectedMetadata, metadata))

		// In db2, we update db2-key1, delete db2-key2, newly add db2-key3
		val, metadata, err = env.db.Get("db2", "db2-key1")
		require.NoError(t, err)
		expectedVal = []byte("new-value-1")
		expectedMetadata = &types.Metadata{
			Version: &types.Version{
				BlockNum: 10,
				TxNum:    2,
			},
		}
		require.Equal(t, expectedVal, val)
		require.True(t, proto.Equal(expectedMetadata, metadata))

		val, metadata, err = env.db.Get("db2", "db2-key2")
		require.NoError(t, err)
		require.Nil(t, val)

		val, metadata, err = env.db.Get("db2", "db2-key3")
		require.NoError(t, err)
		expectedVal = []byte("value-3")
		expectedMetadata = &types.Metadata{
			Version: &types.Version{
				BlockNum: 10,
				TxNum:    3,
			},
		}
		require.Equal(t, expectedVal, val)
		require.True(t, proto.Equal(expectedMetadata, metadata))
	})

	t.Run("commit block and expect error", func(t *testing.T) {
		t.Parallel()
		env := newCommitterTestEnv(t)
		defer env.cleanup()

		block := &types.Block{
			Header: &types.BlockHeader{
				Number: 2,
			},
			TransactionEnvelopes: []*types.TransactionEnvelope{
				{
					Payload: &types.Transaction{
						DBName: "db1",
						Writes: []*types.KVWrite{
							{
								Key:   "db1-key3",
								Value: []byte("value-3"),
							},
						},
					},
				},
			},
		}

		validationInfo := []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}

		require.EqualError(t, env.committer.commitBlock(block, validationInfo), "failed to commit block 2 to state database: database db1 does not exist")
	})
}
