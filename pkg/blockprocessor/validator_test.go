package blockprocessor

import (
	"encoding/json"
	"encoding/pem"
	"fmt"
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
		require.True(t, proto.Equal(
			&types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [db1]",
			},
			valInfo,
		))
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
		require.True(t, proto.Equal(
			&types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the key [key3] in database [db1] changed",
			},
			valInfo,
		))
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
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the key [db1-key2] in database [db1] changed",
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the key [db2-key2] in database [db2] changed",
			},
			{
				Flag:            types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
				ReasonIfInvalid: "the database [db4] does not exist",
			},
			{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [db1-key1] in database [db1]",
			},
			{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [db2-key1] in database [db2]",
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

		cert, err := ioutil.ReadFile("./testdata/sample.cert")
		require.NoError(t, err)
		dcCert, _ := pem.Decode(cert)

		validConfig := &types.ClusterConfig{
			Nodes: []*types.NodeConfig{
				{
					ID:          "node1",
					Address:     "127.0.0.1",
					Port:        1234,
					Certificate: dcCert.Bytes,
				},
			},
			Admins: []*types.Admin{
				{
					ID:          "admin1",
					Certificate: dcCert.Bytes,
				},
			},
		}
		validConfigSerialized, err := json.Marshal(validConfig)
		require.NoError(t, err)

		validUser := &types.User{
			ID:          "user3",
			Certificate: dcCert.Bytes,
		}
		vaildUserSerialized, err := json.Marshal(validUser)
		require.NoError(t, err)

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
								Key: "db4",
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
					// invalid transaction as the value assigned for
					// the user is invalid
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
					// valid transaction
					Payload: &types.Transaction{
						Type:   types.Transaction_USER,
						UserID: []byte("userWithMorePrivilege"),
						DBName: worldstate.UsersDBName,
						Writes: []*types.KVWrite{
							{
								Key:   "user3",
								Value: vaildUserSerialized,
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
					// invalid transaction as the value assigned to
					// the config value is invalid
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
					// valid transaction
					Payload: &types.Transaction{
						Type:   types.Transaction_CONFIG,
						UserID: []byte("userWithMorePrivilege"),
						DBName: worldstate.ConfigDBName,
						Writes: []*types.KVWrite{
							{
								Key:   "config",
								Value: validConfigSerialized,
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
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [userWithLessPrivilege] has no write permission on key [db1-key1] present in the database [db1]",
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [userWithLessPrivilege] has no privilege to perform database administrative operations",
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [userWithLessPrivilege] has no privilege to perform user administrative operations",
			},
			{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "unmarshal error while retrieving user [user3] entry from the transaction: invalid character 'u' looking for beginning of value",
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [userWithLessPrivilege] has no privilege to perform cluster administrative operations",
			},
			{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "unmarshal error while retrieving new configuration from the transaction",
			},
			{
				Flag: types.Flag_VALID,
			},
			{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [userWithLessPrivilege] has no write permission on database [db3]",
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

func TestValidateDBEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		tx             *types.Transaction
		expectedResult *types.ValidationInfo
	}{
		{
			name: "dbname is empty",
			tx: &types.Transaction{
				Type: types.Transaction_DB,
				Writes: []*types.KVWrite{
					{
						Key: "",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database name cannot be empty",
			},
		},
		{
			name: "system DB cannot be administered",
			tx: &types.Transaction{
				Type: types.Transaction_DB,
				Writes: []*types.KVWrite{
					{
						Key: worldstate.ConfigDBName,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database name [" + worldstate.ConfigDBName + "] is a system database which cannot be administered",
			},
		},
		{
			name: "db in the delete list does not exist",
			tx: &types.Transaction{
				Type: types.Transaction_DB,
				Writes: []*types.KVWrite{
					{
						Key:      "db3",
						IsDelete: true,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [db3] does not exist in the cluster and hence, it cannot be deleted",
			},
		},
		{
			name: "duplicate entries in the delete list",
			tx: &types.Transaction{
				Type: types.Transaction_DB,
				Writes: []*types.KVWrite{
					{
						Key:      "db2",
						IsDelete: true,
					},
					{
						Key:      "db2",
						IsDelete: true,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [db2] is duplicated in the delete list",
			},
		},
		{
			name: "database to be created already exist",
			tx: &types.Transaction{
				Type: types.Transaction_DB,
				Writes: []*types.KVWrite{
					{
						Key: "db2",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [db2] already exists in the cluster and hence, it cannot be created",
			},
		},
		{
			name: "duplicate entries in the db creation list",
			tx: &types.Transaction{
				Type: types.Transaction_DB,
				Writes: []*types.KVWrite{
					{
						Key: "db3",
					},
					{
						Key: "db3",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [db3] is duplicated in the create list",
			},
		},
		{
			name: "correct entries",
			tx: &types.Transaction{
				Type: types.Transaction_DB,
				Writes: []*types.KVWrite{
					{
						Key: "db3",
					},
					{
						Key: "db4",
					},
					{
						Key:      "db1",
						IsDelete: true,
					},
					{
						Key:      "db2",
						IsDelete: true,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

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
					},
				},
			}
			require.NoError(t, env.db.Commit(createDB))

			valRes := env.validator.validateDBEntries(tt.tx)
			require.True(t, proto.Equal(tt.expectedResult, valRes))
		})
	}
}

func TestValidateDataEntries(t *testing.T) {
	t.Parallel()

	setup := func(db worldstate.DB) {
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
				},
			},
		}
		require.NoError(t, db.Commit(createDB))
		require.True(t, db.Exist("_users"))

		u2 := &types.User{
			ID: "user2",
		}
		u3 := &types.User{
			ID: "user3",
		}
		u4 := &types.User{
			ID: "user4",
		}

		user2, err := proto.Marshal(u2)
		require.NoError(t, err)
		user3, err := proto.Marshal(u3)
		require.NoError(t, err)
		user4, err := proto.Marshal(u4)
		require.NoError(t, err)

		createUsers := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + "user2",
						Value: user2,
					},
					{
						Key:   string(identity.UserNamespace) + "user3",
						Value: user3,
					},
					{
						Key:   string(identity.UserNamespace) + "user4",
						Value: user4,
					},
				},
			},
		}
		require.NoError(t, db.Commit(createUsers))
	}

	tests := []struct {
		name           string
		tx             *types.Transaction
		expectedResult *types.ValidationInfo
	}{
		{
			name: "the user in read acl list does not exist",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key: "key1",
						ACL: &types.AccessControl{
							ReadUsers: map[string]bool{
								"user1": true,
							},
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("the user [user1] defined in the access control for the key [key1] does not exist"),
			},
		},
		{
			name: "the user in write acl list does not exist",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key: "key1",
						ACL: &types.AccessControl{
							ReadWriteUsers: map[string]bool{
								"user1": true,
							},
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: fmt.Sprintf("the user [user1] defined in the access control for the key [key1] does not exist"),
			},
		},
		{
			name: "correct entries",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key: "key1",
						ACL: &types.AccessControl{
							ReadUsers: map[string]bool{
								"user2": true,
								"user3": true,
							},
							ReadWriteUsers: map[string]bool{
								"user4": true,
							},
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()
			setup(env.db)

			valRes, err := env.validator.validateDataEntries(tt.tx)
			require.NoError(t, err)
			require.True(t, proto.Equal(tt.expectedResult, valRes))
		})
	}
}

func TestValidateUserEntries(t *testing.T) {
	t.Parallel()

	userWithEmptyID := &types.User{
		ID: "",
	}
	userWithEmptyIDSerialized, err := json.Marshal(userWithEmptyID)
	require.NoError(t, err)

	userWithEmptyCert := &types.User{
		ID:          "user1",
		Certificate: nil,
	}
	userWithEmptyCertSerialized, err := json.Marshal(userWithEmptyCert)
	require.NoError(t, err)

	cert, err := ioutil.ReadFile("./testdata/sample.cert")
	require.NoError(t, err)

	dcCert, _ := pem.Decode(cert)
	user := &types.User{
		ID:          "user1",
		Certificate: dcCert.Bytes,
	}
	userSerialized, err := json.Marshal(user)
	require.NoError(t, err)

	tests := []struct {
		name           string
		tx             *types.Transaction
		expectedResult *types.ValidationInfo
	}{
		{
			name: "unmarshal error",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "user1",
						Value: nil,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "unmarshal error while retrieving user [user1] entry from the transaction: unexpected end of JSON input",
			},
		},
		{
			name: "user id is empty",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "",
						Value: userWithEmptyIDSerialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user with empty ID. A valid userID must be non empty string",
			},
		},
		{
			name: "certificate is nil",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "user1",
						Value: userWithEmptyCertSerialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [user1] has an invalid certificate: asn1: syntax error: sequence truncated",
			},
		},
		{
			name: "correct entries",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "user1",
						Value: userSerialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			valRes := env.validator.validateUserEntries(tt.tx)
			require.Equal(t, tt.expectedResult, valRes)
		})
	}
}

func TestValidateConfigEntries(t *testing.T) {
	cert, err := ioutil.ReadFile("./testdata/sample.cert")
	require.NoError(t, err)
	dcCert, _ := pem.Decode(cert)

	configWithNoAdmins := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				ID:          "node1",
				Certificate: dcCert.Bytes,
				Address:     "127.0.0.50",
			},
		},
	}
	configWithNoAdminSerialized, err := json.Marshal(configWithNoAdmins)
	require.NoError(t, err)

	configWithNoNodes := &types.ClusterConfig{
		Admins: []*types.Admin{
			{
				ID:          "admin1",
				Certificate: dcCert.Bytes,
			},
		},
	}
	configWithNoNodesSerialized, err := json.Marshal(configWithNoNodes)
	require.NoError(t, err)

	configWithEmptyNodeID := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				ID: "",
			},
		},
		Admins: []*types.Admin{
			{
				ID: "",
			},
		},
	}
	configWithEmptyNodeIDSerialized, err := json.Marshal(configWithEmptyNodeID)
	require.NoError(t, err)

	configWithEmptyNodeCert := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				ID:          "node1",
				Address:     "127.0.0.1",
				Certificate: nil,
			},
		},
		Admins: []*types.Admin{
			{
				ID: "",
			},
		},
	}
	configWithEmptyNodeCertSerialized, err := json.Marshal(configWithEmptyNodeCert)
	require.NoError(t, err)

	configWithEmptyNodeAddress := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				ID:          "node1",
				Certificate: dcCert.Bytes,
				Address:     "",
			},
		},
		Admins: []*types.Admin{
			{
				ID: "",
			},
		},
	}
	configWithEmptyNodeAddressSerialized, err := json.Marshal(configWithEmptyNodeAddress)
	require.NoError(t, err)

	configWithInvalidNodeAddress1 := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				ID:          "node1",
				Certificate: dcCert.Bytes,
				Address:     "127.0",
			},
		},
		Admins: []*types.Admin{
			{
				ID: "",
			},
		},
	}
	configWithInvalidNodeAddress1Serialized, err := json.Marshal(configWithInvalidNodeAddress1)
	require.NoError(t, err)

	configWithInvalidNodeAddress2 := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				ID:          "node1",
				Certificate: dcCert.Bytes,
				Address:     "127.0.0.500",
			},
		},
		Admins: []*types.Admin{
			{
				ID: "",
			},
		},
	}
	configWithInvalidNodeAddress2Serialized, err := json.Marshal(configWithInvalidNodeAddress2)
	require.NoError(t, err)

	configWithEmptyAdminID := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				ID:          "node1",
				Certificate: dcCert.Bytes,
				Address:     "127.0.0.50",
			},
		},
		Admins: []*types.Admin{
			{
				ID: "",
			},
		},
	}
	configWithEmptyAdminIDSerialized, err := json.Marshal(configWithEmptyAdminID)
	require.NoError(t, err)

	configWithEmptyAdminCert := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				ID:          "node1",
				Certificate: dcCert.Bytes,
				Address:     "127.0.0.50",
			},
		},
		Admins: []*types.Admin{
			{
				ID:          "admin1",
				Certificate: nil,
			},
		},
	}
	configWithEmptyAdminCertSerialized, err := json.Marshal(configWithEmptyAdminCert)
	require.NoError(t, err)

	config := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				ID:          "node1",
				Certificate: dcCert.Bytes,
				Address:     "127.0.0.50",
			},
		},
		Admins: []*types.Admin{
			{
				ID:          "admin1",
				Certificate: dcCert.Bytes,
			},
		},
	}
	configSerialized, err := json.Marshal(config)
	require.NoError(t, err)

	tests := []struct {
		name           string
		tx             *types.Transaction
		expectedResult *types.ValidationInfo
	}{
		{
			name: "node entries are empty",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "config",
						Value: configWithNoNodesSerialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "node entries are empty. There must be at least single node in the cluster",
			},
		},
		{
			name: "admin entries are empty",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "config",
						Value: configWithNoAdminSerialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "admin entries are empty. There must be at least single admin in the cluster",
			},
		},
		{
			name: "unmarshal error",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "config",
						Value: nil,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "unmarshal error while retrieving new configuration from the transaction",
			},
		},
		{
			name: "nodeID is empty",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "config",
						Value: configWithEmptyNodeIDSerialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the nodeID cannot be empty",
			},
		},
		{
			name: "certificate is empty",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "config",
						Value: configWithEmptyNodeCertSerialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [node1] has an invalid certificate: asn1: syntax error: sequence truncated",
			},
		},
		{
			name: "node address is empty",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "config",
						Value: configWithEmptyNodeAddressSerialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [node1] has an empty ip address",
			},
		},
		{
			name: "node address is invalid",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "config",
						Value: configWithInvalidNodeAddress1Serialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [node1] has an invalid ip address [127.0]",
			},
		},
		{
			name: "node address is invalid",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "config",
						Value: configWithInvalidNodeAddress2Serialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the node [node1] has an invalid ip address [127.0.0.500]",
			},
		},
		{
			name: "admin id is empty",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "config",
						Value: configWithEmptyAdminIDSerialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the adminID cannot be empty",
			},
		},
		{
			name: "admin cert is invalid",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "config",
						Value: configWithEmptyAdminCertSerialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the admin [admin1] has an invalid certificate: asn1: syntax error: sequence truncated",
			},
		},
		{
			name: "correct entries",
			tx: &types.Transaction{
				Writes: []*types.KVWrite{
					{
						Key:   "config",
						Value: configSerialized,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			valRes := env.validator.validateConfigEntries(tt.tx)
			require.Equal(t, tt.expectedResult, valRes)
			require.True(t, proto.Equal(tt.expectedResult, valRes))
		})
	}
}
