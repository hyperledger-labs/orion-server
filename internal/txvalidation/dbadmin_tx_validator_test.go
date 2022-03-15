// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package txvalidation

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestValidateDBAdminTx(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"userWithMorePrivilege", "userWithLessPrivilege"})
	adminCert, adminSigner := testutils.LoadTestCrypto(t, cryptoDir, "userWithMorePrivilege")
	nonAdminCert, nonAdminSigner := testutils.LoadTestCrypto(t, cryptoDir, "userWithLessPrivilege")

	sampleMetadataData := &types.Metadata{
		Version: &types.Version{
			BlockNum: 2,
			TxNum:    1,
		},
	}

	userWithLessPrivilege := &types.User{
		Id:          "userWithLessPrivilege",
		Certificate: nonAdminCert.Raw,
	}
	userWithLessPrivilegeSerialized, err := proto.Marshal(userWithLessPrivilege)
	require.NoError(t, err)

	underPrivilegedUser := map[string]*worldstate.DBUpdates{
		worldstate.UsersDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:      string(identity.UserNamespace) + "userWithLessPrivilege",
					Value:    userWithLessPrivilegeSerialized,
					Metadata: sampleMetadataData,
				},
			},
		},
	}

	userWithMorePrivilege := &types.User{
		Id:          "userWithMorePrivilege",
		Certificate: adminCert.Raw,
		Privilege: &types.Privilege{
			Admin: true,
		},
	}
	userWithMorePrivilegeSerialized, err := proto.Marshal(userWithMorePrivilege)
	require.NoError(t, err)

	privilegedUser := map[string]*worldstate.DBUpdates{
		worldstate.UsersDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:      string(identity.UserNamespace) + "userWithMorePrivilege",
					Value:    userWithMorePrivilegeSerialized,
					Metadata: sampleMetadataData,
				},
			},
		},
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		txEnv          *types.DBAdministrationTxEnvelope
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: signature verification failure",
			setup: func(db worldstate.DB) {
				require.NoError(t, db.Commit(privilegedUser, 1))
			},
			txEnv: testutils.SignedDBAdministrationTxEnvelope(t, nonAdminSigner, &types.DBAdministrationTx{
				UserId:    "userWithMorePrivilege",
				CreateDbs: []string{"db1", "db2"},
				DeleteDbs: []string{"db3", "db4"},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_UNAUTHORISED,
				ReasonIfInvalid: "signature verification failed: x509: ECDSA verification failure",
			},
		},
		{
			name: "invalid: user does not have db admin privilege",
			setup: func(db worldstate.DB) {
				require.NoError(t, db.Commit(underPrivilegedUser, 1))
			},
			txEnv: testutils.SignedDBAdministrationTxEnvelope(t, nonAdminSigner,
				&types.DBAdministrationTx{
					UserId:    "userWithLessPrivilege",
					CreateDbs: []string{"db1"},
				}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the user [userWithLessPrivilege] has no privilege to perform database administrative operations",
			},
		},
		{
			name: "invalid: createDBs list has duplicate",
			setup: func(db worldstate.DB) {
				require.NoError(t, db.Commit(privilegedUser, 1))
			},
			txEnv: testutils.SignedDBAdministrationTxEnvelope(t, adminSigner,
				&types.DBAdministrationTx{
					UserId:    "userWithMorePrivilege",
					CreateDbs: []string{"db1", "db1"},
				}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [db1] is duplicated in the create list",
			},
		},
		{
			name: "invalid: deleteDBs list has a non-existing db",
			setup: func(db worldstate.DB) {
				require.NoError(t, db.Commit(privilegedUser, 1))
			},
			txEnv: testutils.SignedDBAdministrationTxEnvelope(t, adminSigner, &types.DBAdministrationTx{
				UserId:    "userWithMorePrivilege",
				DeleteDbs: []string{"db1", "db1"},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [db1] does not exist in the cluster and hence, it cannot be deleted",
			},
		},
		{
			name: "invalid: createDBs list has an invalid db name",
			setup: func(db worldstate.DB) {
				require.NoError(t, db.Commit(privilegedUser, 1))
			},
			txEnv: testutils.SignedDBAdministrationTxEnvelope(t, adminSigner,
				&types.DBAdministrationTx{
					UserId:    "userWithMorePrivilege",
					CreateDbs: []string{"db1", "db1/abc"},
				}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database name [db1/abc] is not valid",
			},
		},
		{
			name: "invalid: deleteDBs list has an invalid db name",
			setup: func(db worldstate.DB) {
				require.NoError(t, db.Commit(privilegedUser, 1))
			},
			txEnv: testutils.SignedDBAdministrationTxEnvelope(t, adminSigner, &types.DBAdministrationTx{
				UserId:    "userWithMorePrivilege",
				DeleteDbs: []string{"db1/abc/def", "db1"},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database name [db1/abc/def] is not valid",
			},
		},
		{
			name: "invalid: db does not exist already and also does not appear in the createDB list",
			setup: func(db worldstate.DB) {
				require.NoError(t, db.Commit(privilegedUser, 1))
			},
			txEnv: testutils.SignedDBAdministrationTxEnvelope(t, adminSigner, &types.DBAdministrationTx{
				UserId: "userWithMorePrivilege",
				DbsIndex: map[string]*types.DBIndex{
					"db1": {
						AttributeAndType: map[string]types.IndexAttributeType{
							"attr1": types.IndexAttributeType_STRING,
						},
					},
				},
			}),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "index definion provided for database [db1] cannot be processed as the database neither exists nor is in the create DB list",
			},
		},
		{
			name: "valid transaction",
			setup: func(db worldstate.DB) {
				require.NoError(t, db.Commit(privilegedUser, 1))

				createDB := map[string]*worldstate.DBUpdates{
					worldstate.DatabasesDBName: {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "db3",
							},
							{
								Key: "db4",
							},
						},
					},
				}
				require.NoError(t, db.Commit(createDB, 1))
			},
			txEnv: testutils.SignedDBAdministrationTxEnvelope(t, adminSigner, &types.DBAdministrationTx{
				UserId:    "userWithMorePrivilege",
				CreateDbs: []string{"db1", "db2"},
				DeleteDbs: []string{"db3", "db4"},
				DbsIndex: map[string]*types.DBIndex{
					"db1": {
						AttributeAndType: map[string]types.IndexAttributeType{
							"attr1": types.IndexAttributeType_STRING,
						},
					},
				},
			}),
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

			result, err := env.validator.dbAdminTxValidator.validate(tt.txEnv)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateCreateDBEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		toCreateDBs    []string
		expectedResult *types.ValidationInfo
	}{
		{
			name:        "invalid: dbname is empty",
			toCreateDBs: []string{""},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the name of the database to be created cannot be empty",
			},
		},
		{
			name:        "invalid: system database cannot be created",
			toCreateDBs: []string{worldstate.ConfigDBName},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [" + worldstate.ConfigDBName + "] is a system database which cannot be created as it exist by default",
			},
		},
		{
			name:        "invalid: default worldstate database cannot be created",
			toCreateDBs: []string{worldstate.DefaultDBName},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [" + worldstate.DefaultDBName + "] is the system created default database for storing states and it cannot be created as it exist by default",
			},
		},
		{
			name:        "invalid: existing database cannot be created",
			toCreateDBs: []string{"db1"},
			setup: func(db worldstate.DB) {
				createDB := map[string]*worldstate.DBUpdates{
					worldstate.DatabasesDBName: {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "db1",
							},
						},
					},
				}
				require.NoError(t, db.Commit(createDB, 1))
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [db1] already exists in the cluster and hence, it cannot be created",
			},
		},
		{
			name:        "invalid: database is duplicated in the create list",
			toCreateDBs: []string{"db1", "db1"},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [db1] is duplicated in the create list",
			},
		},
		{
			name:        "valid",
			toCreateDBs: []string{"db1", "db2"},
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
			if tt.setup != nil {
				tt.setup(env.db)
			}

			result := env.validator.dbAdminTxValidator.validateCreateDBEntries(tt.toCreateDBs)
			require.True(t, proto.Equal(tt.expectedResult, result))
		})
	}
}

func TestValidateDeleteDBEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		toDeleteDBs    []string
		expectedResult *types.ValidationInfo
	}{
		{
			name:        "invalid: dbname is empty",
			toDeleteDBs: []string{""},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the name of the database to be deleted cannot be empty",
			},
		},
		{
			name:        "invalid: system database cannot be deleted",
			toDeleteDBs: []string{worldstate.ConfigDBName},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [" + worldstate.ConfigDBName + "] is a system database which cannot be deleted",
			},
		},
		{
			name:        "invalid: default worldstate database cannot be deleted",
			toDeleteDBs: []string{worldstate.DefaultDBName},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [" + worldstate.DefaultDBName + "] is the system created default database to store states and it cannot be deleted",
			},
		},
		{
			name:        "invalid: non-existing database cannot be deleted",
			toDeleteDBs: []string{"db3"},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [db3] does not exist in the cluster and hence, it cannot be deleted",
			},
		},
		{
			name: "invalid: database is duplicated in the delete list",
			setup: func(db worldstate.DB) {
				createDB := map[string]*worldstate.DBUpdates{
					worldstate.DatabasesDBName: {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "db1",
							},
						},
					},
				}
				require.NoError(t, db.Commit(createDB, 1))
			},
			toDeleteDBs: []string{"db1", "db1"},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [db1] is duplicated in the delete list",
			},
		},
		{
			name: "valid",
			setup: func(db worldstate.DB) {
				createDB := map[string]*worldstate.DBUpdates{
					worldstate.DatabasesDBName: {
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
				require.NoError(t, db.Commit(createDB, 1))
			},
			toDeleteDBs: []string{"db1", "db2"},
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
			if tt.setup != nil {
				tt.setup(env.db)
			}

			result := env.validator.dbAdminTxValidator.validateDeleteDBEntries(tt.toDeleteDBs)
			require.True(t, proto.Equal(tt.expectedResult, result))
		})
	}
}

func TestValidateIndexDBEntries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		toCreateDBs    []string
		toDeleteDBs    []string
		dbsIndex       map[string]*types.DBIndex
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: db does not exist already and also does not appear in the createDB list",
			dbsIndex: map[string]*types.DBIndex{
				"db1": {
					AttributeAndType: map[string]types.IndexAttributeType{
						"attr1": types.IndexAttributeType_STRING,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "index definion provided for database [db1] cannot be processed as the database neither exists nor is in the create DB list",
			},
		},
		{
			name:        "valid: db does not exist already but appears in the createDB list",
			toCreateDBs: []string{"db1"},
			dbsIndex: map[string]*types.DBIndex{
				"db1": {
					AttributeAndType: map[string]types.IndexAttributeType{
						"attr1": types.IndexAttributeType_STRING,
						"attr2": types.IndexAttributeType_NUMBER,
						"attr3": types.IndexAttributeType_BOOLEAN,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "invalid: db exist but appears in the deleteDB list too",
			setup: func(db worldstate.DB) {
				createDB := map[string]*worldstate.DBUpdates{
					worldstate.DatabasesDBName: {
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
				require.NoError(t, db.Commit(createDB, 1))
			},
			toDeleteDBs: []string{"db1", "db2"},
			dbsIndex: map[string]*types.DBIndex{
				"db1": {
					AttributeAndType: map[string]types.IndexAttributeType{
						"attr1": types.IndexAttributeType_STRING,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "index definion provided for database [db1] cannot be processed as the database is present in the delete list",
			},
		},
		{
			name: "invalid: index update on an existing database",
			setup: func(db worldstate.DB) {
				createDB := map[string]*worldstate.DBUpdates{worldstate.DatabasesDBName: {Writes: []*worldstate.KVWithMetadata{{Key: "db1"}}}}
				require.NoError(t, db.Commit(createDB, 1))
			},
			toCreateDBs: []string{},
			toDeleteDBs: []string{},
			dbsIndex: map[string]*types.DBIndex{
				"db1": {
					AttributeAndType: map[string]types.IndexAttributeType{"attr1": types.IndexAttributeType_STRING},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "index update to an existing database is not allowed",
			},
		},
		{
			name:        "invalid: unknown attribute type",
			toCreateDBs: []string{"db1"},
			dbsIndex: map[string]*types.DBIndex{
				"db1": {
					AttributeAndType: map[string]types.IndexAttributeType{
						"attr1": types.IndexAttributeType_STRING,
						"attr2": types.IndexAttributeType_NUMBER,
						"attr3": 10,
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "invalid type provided for the attribute [attr3]",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()
			if tt.setup != nil {
				tt.setup(env.db)
			}

			result := env.validator.dbAdminTxValidator.validateIndexEntries(tt.dbsIndex, tt.toCreateDBs, tt.toDeleteDBs)
			require.True(t, proto.Equal(tt.expectedResult, result))
		})
	}
}
