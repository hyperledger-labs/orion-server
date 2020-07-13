package committer

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type testEnv struct {
	db        *leveldb.LevelDB
	path      string
	committer *Committer
	cleanup   func()
}

func (env *testEnv) init(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "ledger")
	require.NoError(t, err)
	env.path = filepath.Join(dir, "leveldb")

	env.cleanup = func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("failed to remove directory %s", dir)
		}
		if err := os.RemoveAll(env.path); err != nil {
			t.Fatalf("failed to remove directory %s", dir)
		}
	}

	db, err := leveldb.NewLevelDB(env.path)
	if err != nil {
		env.cleanup()
		t.Fatalf("failed to create leveldb with path %s", env.path)
	}

	env.db = db
	env.committer = NewCommitter(db)
}

var env testEnv

func TestStateDBCommitter(t *testing.T) {
	setup := func(db worldstate.DB) []*worldstate.DBUpdates {
		db1val1 := &api.Value{
			Value: []byte("db1-value1"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 1,
					TxNum:    1,
				},
			},
		}
		db1val2 := &api.Value{
			Value: []byte("db1-value2"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 1,
					TxNum:    2,
				},
			},
		}
		db2val1 := &api.Value{
			Value: []byte("db2-value1"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 1,
					TxNum:    3,
				},
			},
		}
		db2val2 := &api.Value{
			Value: []byte("db2-value2"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 1,
					TxNum:    4,
				},
			},
		}
		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "db1",
				Writes: []*worldstate.KV{
					{
						Key:   "db1-key1",
						Value: db1val1,
					},
					{
						Key:   "db1-key2",
						Value: db1val2,
					},
				},
			},
			{
				DBName: "db2",
				Writes: []*worldstate.KV{
					{
						Key:   "db2-key1",
						Value: db2val1,
					},
					{
						Key:   "db2-key2",
						Value: db2val2,
					},
				},
			},
		}
		require.NoError(t, env.db.Create("db1"))
		require.NoError(t, env.db.Create("db2"))
		require.NoError(t, env.db.Commit(dbsUpdates))
		return dbsUpdates
	}

	t.Run("commit block to replace all existing entries", func(t *testing.T) {
		env.init(t)
		defer env.cleanup()
		initialKVsPerDB := setup(env.db)

		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.True(t, proto.Equal(kv.Value, val))
			}
		}

		// create a block to update all existing entries in the database
		// In db1, we update db1-key1, db1-key2
		// In db2, we update db2-key1, db2-key2
		block := &api.Block{
			Header: &api.BlockHeader{
				Number: 2,
			},
			TransactionEnvelopes: []*api.TransactionEnvelope{
				{
					Payload: &api.Transaction{
						DBName: "db1",
						Writes: []*api.KVWrite{
							{
								Key:   "db1-key1",
								Value: []byte("new-value-1"),
							},
						},
					},
				},
				{
					Payload: &api.Transaction{
						DBName: "db1",
						Writes: []*api.KVWrite{
							{
								Key:   "db1-key2",
								Value: []byte("new-value-2"),
							},
						},
					},
				},
				{
					Payload: &api.Transaction{
						DBName: "db2",
						Writes: []*api.KVWrite{
							{
								Key:   "db2-key1",
								Value: []byte("new-value-1"),
							},
						},
					},
				},
				{
					Payload: &api.Transaction{
						DBName: "db2",
						Writes: []*api.KVWrite{
							{
								Key:   "db2-key2",
								Value: []byte("new-value-2"),
							},
						},
					},
				},
			},
		}

		validationInfo := []*api.ValidationInfo{
			{
				Flag: api.Flag_VALID,
			},
			{
				Flag: api.Flag_VALID,
			},
			{
				Flag: api.Flag_VALID,
			},
			{
				Flag: api.Flag_VALID,
			},
		}

		require.NoError(t, env.committer.Commit(block, validationInfo))

		// as the last block commit has updated all existing entries,
		// kvs in initialKVsPerDB should not match with the committed versions
		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.False(t, proto.Equal(kv.Value, val))
			}
		}

		val, err := env.db.Get("db1", "db1-key1")
		require.NoError(t, err)
		expectedVal := &api.Value{
			Value: []byte("new-value-1"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 2,
					TxNum:    0,
				},
			},
		}
		require.True(t, proto.Equal(expectedVal, val))
		val, err = env.db.Get("db1", "db1-key2")
		require.NoError(t, err)
		expectedVal = &api.Value{
			Value: []byte("new-value-2"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 2,
					TxNum:    1,
				},
			},
		}
		require.True(t, proto.Equal(expectedVal, val))
		val, err = env.db.Get("db2", "db2-key1")
		require.NoError(t, err)
		expectedVal = &api.Value{
			Value: []byte("new-value-1"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 2,
					TxNum:    2,
				},
			},
		}
		require.True(t, proto.Equal(expectedVal, val))

		val, err = env.db.Get("db2", "db2-key2")
		require.NoError(t, err)
		expectedVal = &api.Value{
			Value: []byte("new-value-2"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 2,
					TxNum:    3,
				},
			},
		}
		require.True(t, proto.Equal(expectedVal, val))
	})

	t.Run("commit block to delete all existing entries", func(t *testing.T) {
		env.init(t)
		defer env.cleanup()
		initialKVsPerDB := setup(env.db)

		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.True(t, proto.Equal(kv.Value, val))
			}
		}

		// create a block to delete all existing entries in the database
		// In db1, we delete db1-key1, db1-key2
		// In db2, we delete db2-key1, db2-key2
		block := &api.Block{
			Header: &api.BlockHeader{
				Number: 2,
			},
			TransactionEnvelopes: []*api.TransactionEnvelope{
				{
					Payload: &api.Transaction{
						DBName: "db1",
						Writes: []*api.KVWrite{
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
					Payload: &api.Transaction{
						DBName: "db2",
						Writes: []*api.KVWrite{
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

		validationInfo := []*api.ValidationInfo{
			{
				Flag: api.Flag_VALID,
			},
			{
				Flag: api.Flag_VALID,
			},
		}

		require.NoError(t, env.committer.Commit(block, validationInfo))

		// as the last block commit has deleted all existing entries,
		// kvs in initialKVsPerDB should not match with the committed versions
		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.False(t, proto.Equal(kv.Value, val))
			}
		}

		val, err := env.db.Get("db1", "db1-key1")
		require.NoError(t, err)
		require.Nil(t, val)

		val, err = env.db.Get("db1", "db1-key2")
		require.NoError(t, err)
		require.Nil(t, val)

		val, err = env.db.Get("db1", "db2-key1")
		require.NoError(t, err)
		require.Nil(t, val)

		val, err = env.db.Get("db1", "db2-key2")
		require.NoError(t, err)
		require.Nil(t, val)
	})

	t.Run("commit block to only insert new entries", func(t *testing.T) {
		env.init(t)
		defer env.cleanup()
		initialKVsPerDB := setup(env.db)

		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.True(t, proto.Equal(kv.Value, val))
			}
		}

		// create a block to insert new entries without touching the
		// existing entries in the database
		// In db1, insert db1-key3, db1-key4
		// In db2, insert db2-key3, db2-key4
		block := &api.Block{
			Header: &api.BlockHeader{
				Number: 2,
			},
			TransactionEnvelopes: []*api.TransactionEnvelope{
				{
					Payload: &api.Transaction{
						DBName: "db1",
						Writes: []*api.KVWrite{
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
					Payload: &api.Transaction{
						DBName: "db2",
						Writes: []*api.KVWrite{
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

		validationInfo := []*api.ValidationInfo{
			{
				Flag: api.Flag_VALID,
			},
			{
				Flag: api.Flag_VALID,
			},
		}

		require.NoError(t, env.committer.Commit(block, validationInfo))

		// as the last block commit has not modified existing entries,
		// kvs in initialKVsPerDB should match with the committed versions
		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.True(t, proto.Equal(kv.Value, val))
			}
		}

		val, err := env.db.Get("db1", "db1-key3")
		require.NoError(t, err)
		expectedVal := &api.Value{
			Value: []byte("value-3"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 2,
					TxNum:    0,
				},
			},
		}
		require.True(t, proto.Equal(expectedVal, val))

		val, err = env.db.Get("db1", "db1-key4")
		require.NoError(t, err)
		expectedVal = &api.Value{
			Value: []byte("value-4"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 2,
					TxNum:    0,
				},
			},
		}
		require.True(t, proto.Equal(expectedVal, val))

		val, err = env.db.Get("db2", "db2-key3")
		require.NoError(t, err)
		expectedVal = &api.Value{
			Value: []byte("value-3"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 2,
					TxNum:    1,
				},
			},
		}
		require.True(t, proto.Equal(expectedVal, val))

		val, err = env.db.Get("db2", "db2-key4")
		require.NoError(t, err)
		expectedVal = &api.Value{
			Value: []byte("value-4"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 2,
					TxNum:    1,
				},
			},
		}
		require.True(t, proto.Equal(expectedVal, val))
	})

	t.Run("commit block to update and delete existing entries while inserting new", func(t *testing.T) {
		env.init(t)
		defer env.cleanup()
		initialKVsPerDB := setup(env.db)

		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.True(t, proto.Equal(kv.Value, val))
			}
		}

		// create a block to update & delete existing entries in the database
		// add a new entry
		// In db1, we delete db1-key1, update db1-key2, newly add db1-key3
		// In db2, we update db2-key1, delete db2-key2, newly add db2-key3
		block := &api.Block{
			Header: &api.BlockHeader{
				Number: 10,
			},
			TransactionEnvelopes: []*api.TransactionEnvelope{
				{
					// we mark this transaction valid
					Payload: &api.Transaction{
						DBName: "db1",
						Writes: []*api.KVWrite{
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
					Payload: &api.Transaction{
						DBName: "db3",
						Writes: []*api.KVWrite{
							{
								Key:   "db3-key2",
								Value: []byte("value-2"),
							},
						},
					},
				},
				{
					// we mark this transaction valid
					Payload: &api.Transaction{
						DBName: "db2",
						Reads: []*api.KVRead{
							{
								Key: "db2-key1",
								Version: &api.Version{
									BlockNum: 1,
									TxNum:    3,
								},
							},
						},
						Writes: []*api.KVWrite{
							{
								Key:   "db2-key1",
								Value: []byte("new-value-1"),
							},
						},
					},
				},
				{
					// we mark this transaction valid
					Payload: &api.Transaction{
						DBName: "db2",
						Writes: []*api.KVWrite{
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
					Payload: &api.Transaction{
						DBName: "db2",
						Writes: []*api.KVWrite{},
					},
				},
				{
					// we mark this transaction invalid
					Payload: &api.Transaction{
						DBName: "db2",
						Writes: []*api.KVWrite{
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

		validationInfo := []*api.ValidationInfo{
			{
				Flag: api.Flag_VALID,
			},
			{
				Flag: api.Flag_INVALID_DB_NOT_EXIST,
			},
			{
				Flag: api.Flag_VALID,
			},
			{
				Flag: api.Flag_VALID,
			},
			{
				Flag: api.Flag_VALID,
			},
			{
				Flag: api.Flag_INVALID_MVCC_CONFLICT,
			},
		}

		require.NoError(t, env.committer.Commit(block, validationInfo))

		// as the last block commit has either updated or deleted
		// existing entries, kvs in initialKVsPerDB should not
		// match with the committed versions
		for _, kvs := range initialKVsPerDB {
			for _, kv := range kvs.Writes {
				val, err := env.db.Get(kvs.DBName, kv.Key)
				require.NoError(t, err)
				require.False(t, proto.Equal(kv.Value, val))
			}
		}

		// In db1, we delete db1-key1, update db1-key2, newly add db1-key3
		val, err := env.db.Get("db1", "db1-key1")
		require.NoError(t, err)
		require.Nil(t, val)

		val, err = env.db.Get("db1", "db1-key2")
		require.NoError(t, err)
		expectedVal := &api.Value{
			Value: []byte("new-value-2"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 10,
					TxNum:    0,
				},
			},
		}
		require.True(t, proto.Equal(expectedVal, val))

		val, err = env.db.Get("db1", "db1-key3")
		require.NoError(t, err)
		expectedVal = &api.Value{
			Value: []byte("value-3"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 10,
					TxNum:    0,
				},
			},
		}
		require.True(t, proto.Equal(expectedVal, val))

		// In db2, we update db2-key1, delete db2-key2, newly add db2-key3
		val, err = env.db.Get("db2", "db2-key1")
		require.NoError(t, err)
		expectedVal = &api.Value{
			Value: []byte("new-value-1"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 10,
					TxNum:    2,
				},
			},
		}
		require.True(t, proto.Equal(expectedVal, val))

		val, err = env.db.Get("db2", "db2-key2")
		require.NoError(t, err)
		require.Nil(t, val)

		val, err = env.db.Get("db2", "db2-key3")
		require.NoError(t, err)
		expectedVal = &api.Value{
			Value: []byte("value-3"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 10,
					TxNum:    3,
				},
			},
		}
		require.True(t, proto.Equal(expectedVal, val))
	})

	t.Run("commit block and expect error", func(t *testing.T) {
		env.init(t)
		defer env.cleanup()

		block := &api.Block{
			Header: &api.BlockHeader{
				Number: 2,
			},
			TransactionEnvelopes: []*api.TransactionEnvelope{
				{
					Payload: &api.Transaction{
						DBName: "db1",
						Writes: []*api.KVWrite{
							{
								Key:   "db1-key3",
								Value: []byte("value-3"),
							},
						},
					},
				},
			},
		}

		validationInfo := []*api.ValidationInfo{
			{
				Flag: api.Flag_VALID,
			},
		}

		require.EqualError(t, env.committer.Commit(block, validationInfo), "failed to commit block 2 to state database: database db1 does not exist")
	})
}
