package blockprocessor

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

func TestValidateDataTx(t *testing.T) {
	t.Parallel()

	addUserWithCorrectPrivilege := func(db worldstate.DB) {
		user := &types.User{
			ID: "operatingUser",
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					worldstate.DefaultDBName: types.Privilege_ReadWrite,
				},
			},
		}
		userSerialized, err := proto.Marshal(user)
		require.NoError(t, err)

		userAdd := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + "operatingUser",
						Value: userSerialized,
					},
				},
			},
		}

		require.NoError(t, db.Commit(userAdd, 1))
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		tx             *types.DataTx
		pendingUpdates map[string]bool
		expectedResult *types.ValidationInfo
	}{
		{
			name:  "invalid: database does not exist",
			setup: func(db worldstate.DB) {},
			tx: &types.DataTx{
				UserID: "operatingUser",
				DBName: "db1",
				DataWrites: []*types.DataWrite{
					{
						Key: "key1",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
				ReasonIfInvalid: "the database [db1] does not exist in the cluster",
			},
		},
		{
			name:  "invalid: system database name cannot be used in a transaction",
			setup: func(db worldstate.DB) {},
			tx: &types.DataTx{
				UserID: "operatingUser",
				DBName: worldstate.ConfigDBName,
				DataWrites: []*types.DataWrite{
					{
						Key: "key1",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the database [" + worldstate.ConfigDBName + "] is a system database and no user can write to a system database via data transaction. Use appropriate transaction type to modify the system database",
			},
		},
		{
			name: "invalid: user does not have rw permission on the db1",
			setup: func(db worldstate.DB) {
				user := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", nil, nil),
						},
					},
				}

				require.NoError(t, db.Commit(user, 1))
			},
			tx: &types.DataTx{
				UserID: "operatingUser",
				DBName: worldstate.DefaultDBName,
				DataWrites: []*types.DataWrite{
					{
						Key: "key1",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no read-write permission on the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "invalid: incorrect fields in the data write",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
			},
			tx: &types.DataTx{
				UserID: "operatingUser",
				DBName: worldstate.DefaultDBName,
				DataWrites: []*types.DataWrite{
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
				ReasonIfInvalid: "the user [user1] defined in the access control for the key [key1] does not exist",
			},
		},
		{
			name: "invalid: incorrect fields in the data delete",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
			},
			tx: &types.DataTx{
				UserID: "operatingUser",
				DBName: worldstate.DefaultDBName,
				DataWrites: []*types.DataWrite{
					{
						Key: "key1",
					},
				},
				DataDeletes: []*types.DataDelete{
					{
						Key: "key2",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [key2] does not exist in the database and hence, it cannot be deleted",
			},
		},
		{
			name: "invalid: duplicate entry",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			tx: &types.DataTx{
				UserID: "operatingUser",
				DBName: worldstate.DefaultDBName,
				DataWrites: []*types.DataWrite{
					{
						Key: "key1",
					},
				},
				DataDeletes: []*types.DataDelete{
					{
						Key: "key1",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [key1] is being updated as well as deleted. Only one operation per key is allowed within a transaction",
			},
		},
		{
			name: "invalid: acl check on read fails",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			tx: &types.DataTx{
				UserID: "operatingUser",
				DBName: worldstate.DefaultDBName,
				DataReads: []*types.DataRead{
					{
						Key: "key1",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no read permission on key [key1] present in the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "invalid: acl check on write fails",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			tx: &types.DataTx{
				UserID: "operatingUser",
				DBName: worldstate.DefaultDBName,
				DataWrites: []*types.DataWrite{
					{
						Key: "key1",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no write permission on key [key1] present in the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "invalid: acl check on delete fails",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			tx: &types.DataTx{
				UserID: "operatingUser",
				DBName: worldstate.DefaultDBName,
				DataDeletes: []*types.DataDelete{
					{
						Key: "key1",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no write permission on key [key1] present in the database [" + worldstate.DefaultDBName + "]. Hence, the user cannot delete the key",
			},
		},
		{
			name: "invalid: mvccValidation fails",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
			},
			tx: &types.DataTx{
				UserID: "operatingUser",
				DBName: worldstate.DefaultDBName,
				DataReads: []*types.DataRead{
					{
						Key: "key1",
					},
				},
			},
			pendingUpdates: map[string]bool{
				"key1": true,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "valid",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"operatingUser": true,
										},
									},
								},
							},
							{
								Key: "key2",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"operatingUser": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			tx: &types.DataTx{
				UserID: "operatingUser",
				DBName: worldstate.DefaultDBName,
				DataReads: []*types.DataRead{
					{
						Key: "key1",
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
				DataWrites: []*types.DataWrite{
					{
						Key:   "key1",
						Value: []byte("new-val"),
					},
				},
				DataDeletes: []*types.DataDelete{
					{
						Key: "key2",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: no read/write/delete",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
			},
			tx: &types.DataTx{
				UserID: "operatingUser",
				DBName: worldstate.DefaultDBName,
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

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.validate(tt.tx, tt.pendingUpdates)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateFieldsInDataWrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		dataWrites     []*types.DataWrite
		expectedResult *types.ValidationInfo
	}{
		{
			name:  "invalid: an empty entry in the write",
			setup: func(db worldstate.DB) {},
			dataWrites: []*types.DataWrite{
				nil,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the write list",
			},
		},
		{
			name:  "invalid: user defined in the read acl does not exist",
			setup: func(db worldstate.DB) {},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
					ACL: &types.AccessControl{
						ReadUsers: map[string]bool{
							"user1": true,
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [user1] defined in the access control for the key [key1] does not exist",
			},
		},
		{
			name:  "invalid: user defined in the write acl does not exist",
			setup: func(db worldstate.DB) {},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
					ACL: &types.AccessControl{
						ReadWriteUsers: map[string]bool{
							"user1": true,
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [user1] defined in the access control for the key [key1] does not exist",
			},
		},
		{
			name: "valid",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "user1", nil, nil),
							constructUserForTest(t, "user2", nil, nil),
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
					ACL: &types.AccessControl{
						ReadUsers: map[string]bool{
							"user1": true,
						},
						ReadWriteUsers: map[string]bool{
							"user2": true,
						},
					},
				},
				{
					Key: "key2",
					ACL: &types.AccessControl{
						ReadUsers: map[string]bool{
							"user1": true,
						},
						ReadWriteUsers: map[string]bool{
							"user2": true,
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

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.validateFieldsInDataWrites(tt.dataWrites)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateFieldsInDataDeletes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		dataDeletes    []*types.DataDelete
		expectedResult *types.ValidationInfo
	}{
		{
			name:  "invalid: an empty entry in the delete",
			setup: func(db worldstate.DB) {},
			dataDeletes: []*types.DataDelete{
				nil,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the delete list",
			},
		},
		{
			name:  "invalid: entry to be deleted does not exist",
			setup: func(db worldstate.DB) {},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [key1] does not exist in the database and hence, it cannot be deleted",
			},
		},
		{
			name: "valid",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
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

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.validateFieldsInDataDeletes(worldstate.DefaultDBName, tt.dataDeletes)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateUniquenessInDataWritesAndDeletes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		dataWrites     []*types.DataWrite
		dataDeletes    []*types.DataDelete
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: duplicate entry in the writes",
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
				{
					Key: "key1",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [key1] is duplicated in the write list. The keys in the write list must be unique",
			},
		},
		{
			name: "invalid: duplicate entry in the deletes",
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
				{
					Key: "key1",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [key1] is duplicated in the delete list. The keys in the delete list must be unique",
			},
		},
		{
			name: "invalid: the same entry is present in both write and delete list",
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [key1] is being updated as well as deleted. Only one operation per key is allowed within a transaction",
			},
		},
		{
			name: "valid",
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key2",
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

			result := validateUniquenessInDataWritesAndDeletes(tt.dataWrites, tt.dataDeletes)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateAClOnDataReads(t *testing.T) {
	sampleVersion := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		dataReads      []*types.DataRead
		operatingUser  string
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: user does not have the permission",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataReads: []*types.DataRead{
				{
					Key: "key1",
				},
			},
			operatingUser: "operatingUser",
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no read permission on key [key1] present in the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "valid: acl check passes as the user has read permission",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadUsers: map[string]bool{
											"operatingUser": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataReads: []*types.DataRead{
				{
					Key: "key1",
				},
			},
			operatingUser: "operatingUser",
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: acl check passes as the user has read-write permission itself",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"operatingUser": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataReads: []*types.DataRead{
				{
					Key: "key1",
				},
			},
			operatingUser: "operatingUser",
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: no acl",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataReads: []*types.DataRead{
				{
					Key: "key1",
				},
			},
			operatingUser: "operatingUser",
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:          "valid: empty reads",
			setup:         func(db worldstate.DB) {},
			dataReads:     nil,
			operatingUser: "operatingUser",
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

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.validateACLOnDataReads(tt.operatingUser, worldstate.DefaultDBName, tt.dataReads)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateAClOnDataWrites(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		dataWrites     []*types.DataWrite
		operatingUser  string
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: user does not have the permission",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			operatingUser: "operatingUser",
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no write permission on key [key1] present in the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "valid: acl check passes",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"operatingUser": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			operatingUser: "operatingUser",
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: no acl",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			operatingUser: "operatingUser",
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:          "valid: empty writes",
			setup:         func(db worldstate.DB) {},
			dataWrites:    nil,
			operatingUser: "operatingUser",
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

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.validateACLOnDataWrites(tt.operatingUser, worldstate.DefaultDBName, tt.dataWrites)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateAClOnDataDeletes(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		dataDeletes    []*types.DataDelete
		operatingUser  string
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: user does not have the permission",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			operatingUser: "operatingUser",
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no write permission on key [key1] present in the database [" + worldstate.DefaultDBName + "]. Hence, the user cannot delete the key",
			},
		},
		{
			name: "valid: acl check passes",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"operatingUser": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			operatingUser: "operatingUser",
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: no acl",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			operatingUser: "operatingUser",
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:          "valid: empty delete",
			setup:         func(db worldstate.DB) {},
			dataDeletes:   nil,
			operatingUser: "operatingUser",
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

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.validateACLOnDataDeletes(tt.operatingUser, worldstate.DefaultDBName, tt.dataDeletes)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestMVCCOnDataTx(t *testing.T) {
	t.Parallel()

	version1 := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}

	version2 := &types.Version{
		BlockNum: 2,
		TxNum:    1,
	}

	version3 := &types.Version{
		BlockNum: 3,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		dataReads      []*types.DataRead
		pendingUpdates map[string]bool
		expectedResult *types.ValidationInfo
	}{
		{
			name:  "invalid: conflict within the block",
			setup: func(db worldstate.DB) {},
			dataReads: []*types.DataRead{
				{
					Key:     "key1",
					Version: version1,
				},
			},
			pendingUpdates: map[string]bool{
				"key1": true,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name:  "invalid: committed version does not exist",
			setup: func(db worldstate.DB) {},
			dataReads: []*types.DataRead{
				{
					Key:     "key1",
					Version: version1,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the key [key1] in database [" + worldstate.DefaultDBName + "] changed",
			},
		},
		{
			name: "invalid: committed version does not match the read version",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: version2,
								},
							},
							{
								Key: "key2",
								Metadata: &types.Metadata{
									Version: version3,
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataReads: []*types.DataRead{
				{
					Key:     "key1",
					Version: version2,
				},
				{
					Key:     "key2",
					Version: version1,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the key [key2] in database [" + worldstate.DefaultDBName + "] changed",
			},
		},
		{
			name: "valid",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: version2,
								},
							},
							{
								Key: "key2",
								Metadata: &types.Metadata{
									Version: version3,
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataReads: []*types.DataRead{
				{
					Key:     "key1",
					Version: version2,
				},
				{
					Key:     "key2",
					Version: version3,
				},
				{
					Key:     "key3",
					Version: nil,
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

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.mvccValidation(worldstate.DefaultDBName, tt.dataReads, tt.pendingUpdates)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}
