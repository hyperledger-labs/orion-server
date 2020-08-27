package blockprocessor

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type validatorTestEnv struct {
	db        *leveldb.LevelDB
	path      string
	validator *validator
	cleanup   func()
}

func newValidatorTestEnv(t *testing.T) *validatorTestEnv {
	dir, err := ioutil.TempDir("/tmp", "validator")
	require.NoError(t, err)
	path := filepath.Join(dir, "leveldb")

	db, err := leveldb.Open(path)
	if err != nil {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove directory %s, %v", dir, err)
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

	return &validatorTestEnv{
		db:   db,
		path: path,
		validator: newValidator(
			&Config{
				DB: db,
			},
		),
		cleanup: cleanup,
	}
}

func TestMVCCValidator(t *testing.T) {
	t.Parallel()

	setup := func(db worldstate.DB) {
		createDB := []*worldstate.DBUpdates{
			{
				DBName: worldstate.DatabasesDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key: "db1",
					},
				},
			},
		}
		require.NoError(t, db.Commit(createDB))

		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "db1",
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   "key1",
						Value: []byte("value1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    1,
							},
						},
					},
					{
						Key:   "key2",
						Value: []byte("value2"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    2,
							},
						},
					},
				},
			},
		}

		require.NoError(t, db.Commit(dbsUpdates))
	}

	t.Run("mvccValidation, valid transaction", func(t *testing.T) {
		t.Parallel()
		env := newValidatorTestEnv(t)
		defer env.cleanup()
		setup(env.db)

		tx := &types.Transaction{
			DBName: "db1",
			Reads: []*types.KVRead{
				{
					Key: "key1",
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    1,
					},
				},
				{
					Key: "key2",
					Version: &types.Version{
						BlockNum: 2,
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
		env := newValidatorTestEnv(t)
		defer env.cleanup()
		setup(env.db)

		tx := &types.Transaction{
			DBName: "db1",
			Reads: []*types.KVRead{
				{
					Key: "key1",
					Version: &types.Version{
						BlockNum: 2,
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
		env := newValidatorTestEnv(t)
		defer env.cleanup()
		setup(env.db)

		tx := &types.Transaction{
			DBName: "db1",
			Reads: []*types.KVRead{
				{
					Key: "key1",
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    1,
					},
				},
				{
					Key: "key3",
					Version: &types.Version{
						BlockNum: 2,
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
		env := newValidatorTestEnv(t)
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
	t.Parallel()

	setup := func(db worldstate.DB) {
		userWithLessPrivilege := &types.User{
			ID: "userWithLessPrivilege",
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					"db1": types.Privilege_ReadWrite,
					"db2": types.Privilege_ReadWrite,
				},
			},
		}

		u1, err := proto.Marshal(userWithLessPrivilege)
		require.NoError(t, err)

		userWithMorePrivilege := &types.User{
			ID: "userWithMorePrivilege",
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					"db1": types.Privilege_ReadWrite,
					"db2": types.Privilege_ReadWrite,
					"db3": types.Privilege_ReadWrite,
				},
				UserAdministration:    true,
				ClusterAdministration: true,
				DBAdministration:      true,
			},
		}

		u2, err := proto.Marshal(userWithMorePrivilege)
		require.NoError(t, err)

		createUser := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + "userWithLessPrivilege",
						Value: u1,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    1,
							},
						},
					},
					{
						Key:   string(identity.UserNamespace) + "userWithMorePrivilege",
						Value: u2,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    1,
							},
						},
					},
				},
			},
		}
		require.NoError(t, db.Commit(createUser))

		createDB := []*worldstate.DBUpdates{
			{
				DBName: worldstate.DatabasesDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key: "db1",
					},
					{
						Key: "db2",
					},
					{
						Key: "db3",
					},
				},
			},
		}
		require.NoError(t, db.Commit(createDB))

		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "db1",
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   "db1-key1",
						Value: []byte("db1-value1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    1,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"userWithLessPrivilege": true,
								},
								ReadWriteUsers: map[string]bool{
									"userWithMorePrivilege": true,
								},
							},
						},
					},
					{
						Key:   "db1-key2",
						Value: []byte("db1-value2"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    2,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"userWithLessPrivilege": true,
								},
								ReadWriteUsers: map[string]bool{
									"userWithMorePrivilege": true,
								},
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
						Value: []byte("db2-value2"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    3,
							},
						},
					},
					{
						Key:   "db2-key2",
						Value: []byte("db2-value2"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    4,
							},
						},
					},
				},
			},
		}

		require.NoError(t, db.Commit(dbsUpdates))
	}

	t.Run("validate block -- db and mvcc check", func(t *testing.T) {
		t.Parallel()
		env := newValidatorTestEnv(t)
		defer env.cleanup()
		setup(env.db)

		block := &types.Block{
			Header: &types.BlockHeader{
				Number: 2,
			},
			TransactionEnvelopes: []*types.TransactionEnvelope{
				{
					// valid transaction
					Payload: &types.Transaction{
						UserID: []byte("userWithMorePrivilege"),
						DBName: "db1",
						Reads: []*types.KVRead{
							{
								Key: "db1-key1",
								Version: &types.Version{
									BlockNum: 2,
									TxNum:    1,
								},
							},
							{
								Key: "db1-key2",
								Version: &types.Version{
									BlockNum: 2,
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
						UserID: []byte("userWithLessPrivilege"),
						DBName: "db1",
						Reads: []*types.KVRead{
							{
								Key: "db1-key2",
								Version: &types.Version{
									BlockNum: 2,
									TxNum:    1,
								},
							},
							{
								Key: "db1-key3",
								Version: &types.Version{
									BlockNum: 2,
									TxNum:    2,
								},
							},
						},
					},
				},
				{
					// valid transaction
					Payload: &types.Transaction{
						UserID: []byte("userWithMorePrivilege"),
						DBName: "db2",
						Reads: []*types.KVRead{
							{
								Key: "db2-key1",
								Version: &types.Version{
									BlockNum: 2,
									TxNum:    3,
								},
							},
							{
								Key: "db2-key2",
								Version: &types.Version{
									BlockNum: 2,
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
						UserID: []byte("userWithLessPrivilege"),
						DBName: "db2",
						Reads: []*types.KVRead{
							{
								Key: "db2-key2",
								Version: &types.Version{
									BlockNum: 2,
									TxNum:    1,
								},
							},
							{
								Key: "db2-key3",
								Version: &types.Version{
									BlockNum: 2,
									TxNum:    2,
								},
							},
						},
					},
				},
				{
					// invalid transaction as the db3 does not exist
					Payload: &types.Transaction{
						UserID: []byte("userWithLessPrivilege"),
						DBName: "db4",
					},
				},
				{
					// invalid transaction as it conflicts with the
					// first transaction in the block
					Payload: &types.Transaction{
						UserID: []byte("userWithLessPrivilege"),
						DBName: "db1",
						Reads: []*types.KVRead{
							{
								Key: "db1-key1",
								Version: &types.Version{
									BlockNum: 2,
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
						UserID: []byte("userWithLessPrivilege"),
						DBName: "db2",
						Reads: []*types.KVRead{
							{
								Key: "db2-key1",
								Version: &types.Version{
									BlockNum: 2,
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

		valInfo, err := env.validator.validateBlock(block)
		require.NoError(t, err)
		require.Equal(t, expectedValidationInfo, valInfo)
	})

	t.Run("validate block -- permission check", func(t *testing.T) {
		t.Parallel()
		env := newValidatorTestEnv(t)
		defer env.cleanup()
		setup(env.db)

		block := &types.Block{
			Header: &types.BlockHeader{
				Number: 2,
			},
			TransactionEnvelopes: []*types.TransactionEnvelope{
				{
					// invalid transaction as the user has no permission
					// to write db1-key1
					Payload: &types.Transaction{
						UserID: []byte("userWithLessPrivilege"),
						DBName: "db1",
						Writes: []*types.KVWrite{
							{
								Key:   "db1-key1",
								Value: []byte("value-1"),
							},
						},
					},
				},
				{
					// valid transaction
					Payload: &types.Transaction{
						UserID: []byte("userWithMorePrivilege"),
						DBName: "db1",
						Writes: []*types.KVWrite{
							{
								Key:   "db1-key1",
								Value: []byte("value-1"),
							},
						},
					},
				},
				{
					// invalid transaction as the user has no permission
					// to manage databases
					Payload: &types.Transaction{
						Type:   types.Transaction_DB,
						UserID: []byte("userWithLessPrivilege"),
						DBName: worldstate.DatabasesDBName,
						Writes: []*types.KVWrite{
							{
								Key: "db3",
							},
						},
					},
				},
				{
					// valid transaction
					Payload: &types.Transaction{
						Type:   types.Transaction_DB,
						UserID: []byte("userWithMorePrivilege"),
						DBName: worldstate.DatabasesDBName,
						Writes: []*types.KVWrite{
							{
								Key: "db3",
							},
						},
					},
				},
				{
					// invalid transaction as the user has no permission
					// to manage users
					Payload: &types.Transaction{
						Type:   types.Transaction_USER,
						UserID: []byte("userWithLessPrivilege"),
						DBName: worldstate.UsersDBName,
						Writes: []*types.KVWrite{
							{
								Key:   "user3",
								Value: []byte("user3"),
							},
						},
					},
				},
				{
					// valid transaction
					Payload: &types.Transaction{
						Type:   types.Transaction_USER,
						UserID: []byte("userWithMorePrivilege"),
						DBName: worldstate.UsersDBName,
						Writes: []*types.KVWrite{
							{
								Key:   "user3",
								Value: []byte("user3"),
							},
						},
					},
				},
				{
					// invalid transaction as the user has no permission
					// to manage the cluster
					Payload: &types.Transaction{
						Type:   types.Transaction_CONFIG,
						UserID: []byte("userWithLessPrivilege"),
						DBName: worldstate.ConfigDBName,
						Writes: []*types.KVWrite{
							{
								Key:   "config",
								Value: []byte("config"),
							},
						},
					},
				},
				{
					// valid transaction
					Payload: &types.Transaction{
						Type:   types.Transaction_CONFIG,
						UserID: []byte("userWithMorePrivilege"),
						DBName: worldstate.ConfigDBName,
						Writes: []*types.KVWrite{
							{
								Key:   "config",
								Value: []byte("config"),
							},
						},
					},
				},
				{
					// invalid transaction as the user has no permission
					// to read-write on the DB
					Payload: &types.Transaction{
						UserID: []byte("userWithLessPrivilege"),
						DBName: "db3",
						Writes: []*types.KVWrite{
							{
								Key:   "key4",
								Value: []byte("value4"),
							},
						},
					},
				},
				{
					// valid transaction
					Payload: &types.Transaction{
						UserID: []byte("userWithMorePrivilege"),
						DBName: "db3",
						Writes: []*types.KVWrite{
							{
								Key:   "key4",
								Value: []byte("value4"),
							},
						},
					},
				},
			},
		}

		expectedValidationInfo := []*types.ValidationInfo{
			{
				Flag: types.Flag_INVALID_NO_PERMISSION,
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_INVALID_NO_PERMISSION,
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_INVALID_NO_PERMISSION,
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_INVALID_NO_PERMISSION,
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag: types.Flag_INVALID_NO_PERMISSION,
			},
			{
				Flag: types.Flag_VALID,
			},
		}

		valInfo, err := env.validator.validateBlock(block)
		require.NoError(t, err)
		require.Equal(t, expectedValidationInfo, valInfo)
	})
}
