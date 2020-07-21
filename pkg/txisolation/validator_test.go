package txisolation

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

type testEnv struct {
	db        *leveldb.LevelDB
	path      string
	validator *Validator
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
	env.validator = NewValidator(db)
}

func TestMVCCValidator(t *testing.T) {
	setup := func(db worldstate.DB) {
		val1 := &types.Value{
			Value: []byte("value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
			},
		}

		val2 := &types.Value{
			Value: []byte("value2"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    2,
				},
			},
		}

		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "db1",
				Writes: []*worldstate.KV{
					{
						Key:   "key1",
						Value: val1,
					},
					{
						Key:   "key2",
						Value: val2,
					},
				},
			},
		}

		require.NoError(t, db.Create("db1"))
		require.NoError(t, db.Commit(dbsUpdates))
	}

	t.Run("mvccValidation, valid transaction", func(t *testing.T) {
		t.Parallel()
		env := &testEnv{}
		env.init(t)
		defer env.cleanup()
		setup(env.db)

		tx := &types.Transaction{
			DBName: "db1",
			Reads: []*types.KVRead{
				{
					Key: "key1",
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    1,
					},
				},
				{
					Key: "key2",
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    2,
					},
				},
				{
					Key:     "key3",
					Version: nil,
				},
			},
		}

		pendingWrites := map[string]bool{
			"key4": true,
			"key5": true,
		}

		valInfo, err := env.validator.mvccValidation(tx, pendingWrites)
		require.NoError(t, err)
		require.True(t, proto.Equal(&types.ValidationInfo{Flag: types.Flag_VALID}, valInfo))
	})

	t.Run("mvccValidation, invalid transaction due to conflict with pending writes", func(t *testing.T) {
		t.Parallel()
		env := &testEnv{}
		env.init(t)
		defer env.cleanup()
		setup(env.db)

		tx := &types.Transaction{
			DBName: "db1",
			Reads: []*types.KVRead{
				{
					Key: "key1",
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    1,
					},
				},
			},
		}

		pendingWrites := map[string]bool{
			"key1": true,
		}

		valInfo, err := env.validator.mvccValidation(tx, pendingWrites)
		require.NoError(t, err)
		require.True(t, proto.Equal(&types.ValidationInfo{Flag: types.Flag_INVALID_MVCC_CONFLICT}, valInfo))
	})

	t.Run("mvccValidation, invalid transaction due to mismatch in the committed version", func(t *testing.T) {
		t.Parallel()
		env := &testEnv{}
		env.init(t)
		defer env.cleanup()
		setup(env.db)

		tx := &types.Transaction{
			DBName: "db1",
			Reads: []*types.KVRead{
				{
					Key: "key1",
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    1,
					},
				},
				{
					Key: "key3",
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    2,
					},
				},
			},
		}

		valInfo, err := env.validator.mvccValidation(tx, map[string]bool{})
		require.NoError(t, err)
		require.True(t, proto.Equal(&types.ValidationInfo{Flag: types.Flag_INVALID_MVCC_CONFLICT}, valInfo))
	})

	t.Run("mvccValidation, error", func(t *testing.T) {
		t.Parallel()
		env := &testEnv{}
		env.init(t)
		defer env.cleanup()
		tx := &types.Transaction{
			DBName: "db1",
			Reads: []*types.KVRead{
				{
					Key:     "key3",
					Version: nil,
				},
			},
		}

		valInfo, err := env.validator.mvccValidation(tx, map[string]bool{})
		require.EqualError(t, err, "database db1 does not exist")
		require.Nil(t, valInfo)
	})
}

func TestValidator(t *testing.T) {
	setup := func(db worldstate.DB) {
		db1val1 := &types.Value{
			Value: []byte("db1-value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
			},
		}

		db1val2 := &types.Value{
			Value: []byte("db1-value2"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    2,
				},
			},
		}

		db2val1 := &types.Value{
			Value: []byte("db2-value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    3,
				},
			},
		}

		db2val2 := &types.Value{
			Value: []byte("db2-value2"),
			Metadata: &types.Metadata{
				Version: &types.Version{
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

		require.NoError(t, db.Create("db1"))
		require.NoError(t, db.Create("db2"))
		require.NoError(t, db.Commit(dbsUpdates))
	}

	t.Run("validate block", func(t *testing.T) {
		t.Parallel()
		env := &testEnv{}
		env.init(t)
		defer env.cleanup()
		setup(env.db)

		block := &types.Block{
			TransactionEnvelopes: []*types.TransactionEnvelope{
				{
					// valid transaction
					Payload: &types.Transaction{
						DBName: "db1",
						Reads: []*types.KVRead{
							{
								Key: "db1-key1",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    1,
								},
							},
							{
								Key: "db1-key2",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    2,
								},
							},
							{
								Key:     "db1-key3",
								Version: nil,
							},
						},
						Writes: []*types.KVWrite{
							{
								Key:   "db1-key1",
								Value: []byte("value-1"),
							},
						},
					},
				},
				{
					// invalid transaction because db1-key3 does not exist
					// and hence the committedVersion would be nil
					Payload: &types.Transaction{
						DBName: "db1",
						Reads: []*types.KVRead{
							{
								Key: "db1-key2",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    1,
								},
							},
							{
								Key: "db1-key3",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    2,
								},
							},
						},
					},
				},
				{
					// valid transaction
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
							{
								Key: "db2-key2",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    4,
								},
							},
							{
								Key:     "db2-key3",
								Version: nil,
							},
						},
						Writes: []*types.KVWrite{
							{
								Key:   "db2-key1",
								Value: []byte("value-2"),
							},
						},
					},
				},
				{
					// invalid transaction because db2-key3 does not exist
					// and hence the committedVersion would be nil
					Payload: &types.Transaction{
						DBName: "db2",
						Reads: []*types.KVRead{
							{
								Key: "db2-key2",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    1,
								},
							},
							{
								Key: "db2-key3",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    2,
								},
							},
						},
					},
				},
				{
					// invalid transaction as the db3 does not exist
					Payload: &types.Transaction{
						DBName: "db3",
					},
				},
				{
					// invalid transaction as it conflicts with the
					// first transaction in the block
					Payload: &types.Transaction{
						DBName: "db1",
						Reads: []*types.KVRead{
							{
								Key: "db1-key1",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    1,
								},
							},
						},
					},
				},
				{
					// invalid transaction as it conflicts with the
					// third transaction in the block
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
					},
				},
			},
		}

		expectedValidationInfo := []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_INVALID_MVCC_CONFLICT,
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_INVALID_MVCC_CONFLICT,
			},
			{
				Flag: types.Flag_INVALID_DB_NOT_EXIST,
			},
			{
				Flag: types.Flag_INVALID_MVCC_CONFLICT,
			},
			{
				Flag: types.Flag_INVALID_MVCC_CONFLICT,
			},
		}

		valInfo, err := env.validator.ValidateBlock(block)
		require.NoError(t, err)
		require.Equal(t, expectedValidationInfo, valInfo)
	})
}
