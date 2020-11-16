package blockprocessor

import (
	"encoding/pem"
	"io/ioutil"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

func TestValidateUsedAdminTx(t *testing.T) {
	t.Parallel()

	cert, err := ioutil.ReadFile("./testdata/sample.cert")
	require.NoError(t, err)
	dcCert, _ := pem.Decode(cert)

	nonAdminUser := &types.User{
		ID: "nonAdminUser",
	}
	nonAdminUserSerialized, err := proto.Marshal(nonAdminUser)
	require.NoError(t, err)

	adminUser := &types.User{
		ID: "adminUser",
		Privilege: &types.Privilege{
			UserAdministration: true,
		},
	}
	adminUserSerialized, err := proto.Marshal(adminUser)
	require.NoError(t, err)

	sampleVersion := &types.Version{
		BlockNum: 2,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		tx             *types.UserAdministrationTx
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: submitter does not have user admin privilege",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "nonAdminUser",
								Value: nonAdminUserSerialized,
							},
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			tx: &types.UserAdministrationTx{
				UserID: "nonAdminUser",
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [nonAdminUser] has no privilege to perform user administrative operations",
			},
		},
		{
			name: "invalid: userID in the write list is empty",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "adminUser",
								Value: adminUserSerialized,
							},
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			tx: &types.UserAdministrationTx{
				UserID: "adminUser",
				UserWrites: []*types.UserWrite{
					{
						User: &types.User{
							ID: "",
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user in the write list with an empty ID. A valid userID must be an non-empty string",
			},
		},
		{
			name: "invalid: userID in the delete list is empty",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "adminUser",
								Value: adminUserSerialized,
							},
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			tx: &types.UserAdministrationTx{
				UserID: "adminUser",
				UserDeletes: []*types.UserDelete{
					{
						UserID: "",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user in the delete list with an empty ID. A valid userID must be an non-empty string",
			},
		},
		{
			name: "invalid: duplicate userID in the delete list",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "adminUser",
								Value: adminUserSerialized,
							},
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			tx: &types.UserAdministrationTx{
				UserID: "adminUser",
				UserDeletes: []*types.UserDelete{
					{
						UserID: "user1",
					},
					{
						UserID: "user1",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two users with the same userID [user1] in the delete list. The userIDs in the delete list must be unique",
			},
		},
		{
			name: "invalid: acl on reads does not pass",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "adminUser",
								Value: adminUserSerialized,
							},
							constructUserForTest(t, "user1", sampleVersion, &types.AccessControl{
								ReadUsers: map[string]bool{
									"user2": true,
								},
							}),
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			tx: &types.UserAdministrationTx{
				UserID: "adminUser",
				UserReads: []*types.UserRead{
					{
						UserID: "user1",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [adminUser] has no read permission on the user [user1]",
			},
		},
		{
			name: "invalid: acl on write does not pass",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "adminUser",
								Value: adminUserSerialized,
							},
							constructUserForTest(t, "user1", sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"user2": true,
								},
							}),
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			tx: &types.UserAdministrationTx{
				UserID: "adminUser",
				UserWrites: []*types.UserWrite{
					{
						User: &types.User{
							ID:          "user1",
							Certificate: dcCert.Bytes,
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [adminUser] has no write permission on the user [user1]",
			},
		},
		{
			name: "invalid: acl on deletes does not pass",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "adminUser",
								Value: adminUserSerialized,
							},
							constructUserForTest(t, "user1", sampleVersion, &types.AccessControl{
								ReadWriteUsers: map[string]bool{
									"user2": true,
								},
							}),
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			tx: &types.UserAdministrationTx{
				UserID: "adminUser",
				UserDeletes: []*types.UserDelete{
					{
						UserID: "user1",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [adminUser] has no write permission on the user [user1]. Hence, the delete operation cannot be performed",
			},
		},
		{
			name: "invalid: mvcc validation does not pass",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "adminUser",
								Value: adminUserSerialized,
							},
							constructUserForTest(t, "user1", sampleVersion, &types.AccessControl{
								ReadUsers: map[string]bool{
									"adminUser": true,
								},
							}),
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			tx: &types.UserAdministrationTx{
				UserID: "adminUser",
				UserReads: []*types.UserRead{
					{
						UserID: "user1",
						Version: &types.Version{
							BlockNum: 100,
							TxNum:    1000,
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the user [user1] has changed",
			},
		},
		{
			name: "valid",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "adminUser",
								Value: adminUserSerialized,
							},
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			tx: &types.UserAdministrationTx{
				UserID: "adminUser",
				UserReads: []*types.UserRead{
					{
						UserID: "user1",
					},
					{
						UserID: "user2",
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

			result, err := env.validator.userAdminTxValidator.validate(tt.tx)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateEntryFieldsInWrites(t *testing.T) {
	t.Parallel()

	cert, err := ioutil.ReadFile("./testdata/sample.cert")
	require.NoError(t, err)
	dcCert, _ := pem.Decode(cert)

	tests := []struct {
		name           string
		userWrites     []*types.UserWrite
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: nil entry in the write list",
			userWrites: []*types.UserWrite{
				nil,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the write list",
			},
		},
		{
			name: "invalid: nil user entry in the write list",
			userWrites: []*types.UserWrite{
				{},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty user entry in the write list",
			},
		},
		{
			name: "invalid: a userID in the write list is empty",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user in the write list with an empty ID. A valid userID must be an non-empty string",
			},
		},
		{
			name: "invalid: db present in the premission list does not exist",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
						Privilege: &types.Privilege{
							DBPermission: map[string]types.Privilege_Access{
								"db1": types.Privilege_Read,
							},
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
				ReasonIfInvalid: "the database [db1] present in the db permission list does not exist in the cluster",
			},
		},
		{
			name: "invalid: certificate is not valid",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID:          "user1",
						Certificate: []byte("random"),
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [user1] in the write list has an invalid certificate: Error = asn1: structure error: tags don't match (16 vs {class:1 tag:18 length:97 isCompound:true}) {optional:false explicit:false application:false private:false defaultValue:<nil> tag:<nil> stringType:0 timeType:0 set:false omitEmpty:false} certificate @2",
			},
		},
		{
			name: "valid: entries are correct",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID:          "user1",
						Certificate: dcCert.Bytes,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: entries are correct and db exist too",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
						Privilege: &types.Privilege{
							DBPermission: map[string]types.Privilege_Access{
								"bdb": types.Privilege_Read,
							},
						},
						Certificate: dcCert.Bytes,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:       "valid: no writes",
			userWrites: nil,
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

			result := env.validator.userAdminTxValidator.validateFieldsInUserWrites(tt.userWrites)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateEntryFieldsInDeletes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		userDeletes    []*types.UserDelete
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: an empty userID in the delete list",
			userDeletes: []*types.UserDelete{
				nil,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the delete list",
			},
		},
		{
			name: "invalid: a userID in the delete list is empty",
			userDeletes: []*types.UserDelete{
				{
					UserID: "",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an user in the delete list with an empty ID. A valid userID must be an non-empty string",
			},
		},
		{
			name: "valid: entries are correct",
			userDeletes: []*types.UserDelete{
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:        "valid: no deletes",
			userDeletes: nil,
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := validateFieldsInUserDeletes(tt.userDeletes)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateUniquenessInEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		userWrites     []*types.UserWrite
		userDeletes    []*types.UserDelete
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: a userID is duplicated in the write list",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
					},
				},
				{
					User: &types.User{
						ID: "user1",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two users with the same userID [user1] in the write list. The userIDs in the write list must be unique",
			},
		},
		{
			name: "invalid: duplicate userID in the delete list",
			userDeletes: []*types.UserDelete{
				{
					UserID: "user2",
				},
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there are two users with the same userID [user2] in the delete list. The userIDs in the delete list must be unique",
			},
		},
		{
			name: "invalid: a userID present in both write and delete list",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
					},
				},
			},
			userDeletes: []*types.UserDelete{
				{
					UserID: "user1",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [user1] is present in both write and delete list. Only one operation per key is allowed within a transaction",
			},
		},
		{
			name: "valid: entries are correct",
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
					},
				},
			},
			userDeletes: []*types.UserDelete{
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:        "valid: no writes and deletes",
			userWrites:  nil,
			userDeletes: nil,
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := validateUniquenessInUserWritesAndDeletes(tt.userWrites, tt.userDeletes)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateACLOnUserReads(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 2,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		operatingUser  string
		setup          func(db worldstate.DB)
		userReads      []*types.UserRead
		expectedResult *types.ValidationInfo
	}{
		{
			name:          "invalid: operatingUser does not have read permission on user2",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", sampleVersion, nil),
							constructUserForTest(t, "user1", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"operatingUser": true,
									},
								},
							),
							constructUserForTest(t, "user2", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"user1": true,
									},
								},
							),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userReads: []*types.UserRead{
				{
					UserID: "user1",
				},
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no read permission on the user [user2]",
			},
		},
		{
			name:          "valid: acl check passes",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", sampleVersion, nil),
							constructUserForTest(t, "user1", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"operatingUser": true,
									},
								},
							),
							constructUserForTest(t, "user2", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"operatingUser": true,
									},
								},
							),
							constructUserForTest(t, "user3", sampleVersion, nil),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userReads: []*types.UserRead{
				{
					UserID: "user1",
				},
				{
					UserID: "user2",
				},
				{
					UserID: "user3",
				},
				{
					UserID: "user4",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:          "valid: empty reads",
			operatingUser: "operatingUser",
			setup:         func(db worldstate.DB) {},
			userReads:     nil,
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

			result, err := env.validator.userAdminTxValidator.validateACLOnUserReads(tt.operatingUser, tt.userReads)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateACLOnUserWrites(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 2,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		operatingUser  string
		setup          func(db worldstate.DB)
		userWrites     []*types.UserWrite
		expectedResult *types.ValidationInfo
	}{
		{
			name:          "invalid: operatingUser does not have write permission on user2",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", sampleVersion, nil),
							constructUserForTest(t, "user1", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"operatingUser": true,
									},
								},
							),
							constructUserForTest(t, "user2", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"user1": true,
									},
								},
							),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
					},
				},
				{
					User: &types.User{
						ID: "user2",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no write permission on the user [user2]",
			},
		},
		{
			name:          "valid: acl check passes",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", sampleVersion, nil),
							constructUserForTest(t, "user1", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"operatingUser": true,
									},
								},
							),
							constructUserForTest(t, "user2", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"operatingUser": true,
									},
								},
							),
							constructUserForTest(t, "user3", sampleVersion, nil),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID: "user1",
					},
				},
				{
					User: &types.User{
						ID: "user2",
					},
				},
				{
					User: &types.User{
						ID: "user3",
					},
				},
				{
					User: &types.User{
						ID: "user4",
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:          "valid: empty writes",
			operatingUser: "operatingUser",
			setup:         func(db worldstate.DB) {},
			userWrites:    nil,
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

			result, err := env.validator.userAdminTxValidator.validateACLOnUserWrites(tt.operatingUser, tt.userWrites)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateACLOnUserDeletes(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 2,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		operatingUser  string
		setup          func(db worldstate.DB)
		userDeletes    []*types.UserDelete
		expectedResult *types.ValidationInfo
	}{
		{
			name:          "invalid: operatingUser does not have write permission on user2",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", sampleVersion, nil),
							constructUserForTest(t, "user1", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"operatingUser": true,
									},
								},
							),
							constructUserForTest(t, "user2", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"user1": true,
									},
								},
							),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userDeletes: []*types.UserDelete{
				{
					UserID: "user1",
				},
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [operatingUser] has no write permission on the user [user2]. Hence, the delete operation cannot be performed",
			},
		},
		{
			name:          "invalid: user2 present in the delete list does not exist",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", sampleVersion, nil),
							constructUserForTest(t, "user1", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"operatingUser": true,
									},
								},
							),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userDeletes: []*types.UserDelete{
				{
					UserID: "user1",
				},
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [user2] present in the delete list does not exist",
			},
		},
		{
			name:          "valid: acl check passes",
			operatingUser: "operatingUser",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "operatingUser", sampleVersion, nil),
							constructUserForTest(t, "user1", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"operatingUser": true,
									},
								},
							),
							constructUserForTest(t, "user2", sampleVersion,
								&types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"operatingUser": true,
									},
								},
							),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userDeletes: []*types.UserDelete{
				{
					UserID: "user1",
				},
				{
					UserID: "user2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:          "valid: empty deletes",
			operatingUser: "operatingUser",
			setup:         func(db worldstate.DB) {},
			userDeletes:   nil,
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

			result, err := env.validator.userAdminTxValidator.validateACLOnUserDeletes(tt.operatingUser, tt.userDeletes)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestMVCCOnUserAdminTx(t *testing.T) {
	t.Parallel()

	version1 := &types.Version{
		BlockNum: 1,
		TxNum:    0,
	}

	version2 := &types.Version{
		BlockNum: 2,
		TxNum:    0,
	}

	version3 := &types.Version{
		BlockNum: 3,
		TxNum:    0,
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		userReads      []*types.UserRead
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: no committed state for user2",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "user1", version1, nil),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userReads: []*types.UserRead{
				{
					UserID:  "user1",
					Version: version1,
				},
				{
					UserID:  "user2",
					Version: version1,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the user [user2] has changed",
			},
		},
		{
			name: "invalid: committed state does not match",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "user1", version1, nil),
							constructUserForTest(t, "user2", version3, nil),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userReads: []*types.UserRead{
				{
					UserID:  "user1",
					Version: version1,
				},
				{
					UserID:  "user2",
					Version: version2,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the user [user2] has changed",
			},
		},
		{
			name: "valid: reads matches committed version",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "user1", version1, nil),
							constructUserForTest(t, "user2", version3, nil),
						},
					},
				}
				require.NoError(t, db.Commit(newUsers, 1))
			},
			userReads: []*types.UserRead{
				{
					UserID:  "user1",
					Version: version1,
				},
				{
					UserID:  "user2",
					Version: version3,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:  "valid: user does not exist and would match the nil version",
			setup: func(db worldstate.DB) {},
			userReads: []*types.UserRead{
				{
					UserID: "user1",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:      "valid: reads is nil",
			setup:     func(db worldstate.DB) {},
			userReads: nil,
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

			result, err := env.validator.userAdminTxValidator.mvccValidation(tt.userReads)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func constructUserForTest(
	t *testing.T,
	userID string,
	version *types.Version,
	acl *types.AccessControl,
) *worldstate.KVWithMetadata {
	user := &types.User{
		ID: userID,
	}
	userSerialized, err := proto.Marshal(user)
	require.NoError(t, err)

	userEntry := &worldstate.KVWithMetadata{
		Key:   string(identity.UserNamespace) + userID,
		Value: userSerialized,
		Metadata: &types.Metadata{
			Version:       version,
			AccessControl: acl,
		},
	}

	return userEntry
}
