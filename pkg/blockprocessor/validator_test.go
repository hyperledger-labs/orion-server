package blockprocessor

import (
	"encoding/pem"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/common/logger"
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
	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("/tmp", "validator")
	require.NoError(t, err)
	path := filepath.Join(dir, "leveldb")

	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: path,
			Logger:    logger,
		},
	)
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
				DB:     db,
				Logger: logger,
			},
		),
		cleanup: cleanup,
	}
}

func TestValidateGenesisBlock(t *testing.T) {
	t.Parallel()

	cert, err := ioutil.ReadFile("./testdata/sample.cert")
	require.NoError(t, err)
	dcCert, _ := pem.Decode(cert)

	t.Run("genesis block with invalid config tx", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name          string
			genesisBlock  *types.Block
			expectedError string
		}{
			{
				name: "node certificate is invalid",
				genesisBlock: &types.Block{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
					},
					Payload: &types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: &types.ConfigTxEnvelope{
							Payload: &types.ConfigTx{
								NewConfig: &types.ClusterConfig{
									Nodes: []*types.NodeConfig{
										{
											ID:          "node1",
											Address:     "127.0.0.1",
											Certificate: []byte("random"),
										},
									},
									Admins: []*types.Admin{
										{
											ID:          "admin1",
											Certificate: dcCert.Bytes,
										},
									},
								},
							},
						},
					},
				},
				expectedError: "genesis block cannot be invalid: reason for invalidation [the node [node1] has an invalid certificate",
			},
			{
				name: "admin certificate is invalid",
				genesisBlock: &types.Block{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
					},
					Payload: &types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: &types.ConfigTxEnvelope{
							Payload: &types.ConfigTx{
								NewConfig: &types.ClusterConfig{
									Nodes: []*types.NodeConfig{
										{
											ID:          "node1",
											Address:     "127.0.0.1",
											Certificate: dcCert.Bytes,
										},
									},
									Admins: []*types.Admin{
										{
											ID:          "admin1",
											Certificate: []byte("random"),
										},
									},
								},
							},
						},
					},
				},
				expectedError: "genesis block cannot be invalid: reason for invalidation [the admin [admin1] has an invalid certificate",
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				env := newValidatorTestEnv(t)
				defer env.cleanup()

				results, err := env.validator.validateBlock(tt.genesisBlock)
				require.Contains(t, err.Error(), tt.expectedError)
				require.Nil(t, results)
			})
		}
	})

	t.Run("genesis block with valid config tx", func(t *testing.T) {
		t.Parallel()

		genesisBlock := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 1,
				},
			},
			Payload: &types.Block_ConfigTxEnvelope{
				ConfigTxEnvelope: &types.ConfigTxEnvelope{
					Payload: &types.ConfigTx{
						NewConfig: &types.ClusterConfig{
							Nodes: []*types.NodeConfig{
								{
									ID:          "node1",
									Address:     "127.0.0.1",
									Certificate: dcCert.Bytes,
								},
							},
							Admins: []*types.Admin{
								{
									ID:          "admin1",
									Certificate: dcCert.Bytes,
								},
							},
						},
					},
				},
			},
		}
		expectedResults := []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}

		env := newValidatorTestEnv(t)
		defer env.cleanup()

		results, err := env.validator.validateBlock(genesisBlock)
		require.NoError(t, err)
		require.Equal(t, expectedResults, results)
	})
}

func TestValidateDataBlock(t *testing.T) {
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
		name            string
		setup           func(db worldstate.DB)
		block           *types.Block
		expectedResults []*types.ValidationInfo
	}{
		{
			name: "data block with valid and invalid transactions",
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
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_DataTxEnvelopes{
					DataTxEnvelopes: &types.DataTxEnvelopes{
						Envelopes: []*types.DataTxEnvelope{
							{
								Payload: &types.DataTx{
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
							},
							{
								Payload: &types.DataTx{
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
								},
							},
							{
								Payload: &types.DataTx{
									UserID: "operatingUser",
									DBName: worldstate.DefaultDBName,
									DataReads: []*types.DataRead{
										{
											Key: "key2",
											Version: &types.Version{
												BlockNum: 1,
												TxNum:    1,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
				{
					Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
					ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [" + worldstate.DefaultDBName + "]",
				},
				{
					Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
					ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key2] in database [" + worldstate.DefaultDBName + "]",
				},
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

			results, err := env.validator.validateBlock(tt.block)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResults, results)
		})
	}
}

func TestValidateUserBlock(t *testing.T) {
	t.Parallel()

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
		name            string
		setup           func(db worldstate.DB)
		block           *types.Block
		expectedResults []*types.ValidationInfo
	}{
		{
			name: "user block with an invalid transaction",
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
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_UserAdministrationTxEnvelope{
					UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
						Payload: &types.UserAdministrationTx{
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
					},
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
					ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the user [user1] has changed",
				},
			},
		},
		{
			name: "user block with an valid transaction",
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
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_UserAdministrationTxEnvelope{
					UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
						Payload: &types.UserAdministrationTx{
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
					},
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
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

			results, err := env.validator.validateBlock(tt.block)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResults, results)
		})
	}
}

func TestValidateDBBlock(t *testing.T) {
	t.Parallel()

	setup := func(db worldstate.DB) {
		userWithMorePrivilege := &types.User{
			ID: "userWithMorePrivilege",
			Privilege: &types.Privilege{
				DBAdministration: true,
			},
		}
		userWithMorePrivilegeSerialized, err := proto.Marshal(userWithMorePrivilege)
		require.NoError(t, err)

		privilegedUser := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + "userWithMorePrivilege",
						Value: userWithMorePrivilegeSerialized,
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
		require.NoError(t, db.Commit(privilegedUser, 1))

		dbs := []*worldstate.DBUpdates{
			{
				DBName: worldstate.DatabasesDBName,
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
		require.NoError(t, db.Commit(dbs, 1))
	}

	tests := []struct {
		name            string
		block           *types.Block
		expectedResults []*types.ValidationInfo
	}{
		{
			name: "db block with an invalid transaction",
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_DBAdministrationTxEnvelope{
					DBAdministrationTxEnvelope: &types.DBAdministrationTxEnvelope{
						Payload: &types.DBAdministrationTx{
							UserID:    "userWithMorePrivilege",
							DeleteDBs: []string{"db1"},
						},
					},
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the database [db1] does not exist in the cluster and hence, it cannot be deleted",
				},
			},
		},
		{
			name: "db block with a valid transaction",
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_DBAdministrationTxEnvelope{
					DBAdministrationTxEnvelope: &types.DBAdministrationTxEnvelope{
						Payload: &types.DBAdministrationTx{
							UserID:    "userWithMorePrivilege",
							CreateDBs: []string{"db1", "db2"},
							DeleteDBs: []string{"db3", "db4"},
						},
					},
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
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

			results, err := env.validator.validateBlock(tt.block)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResults, results)
		})
	}
}

func TestValidateConfigBlock(t *testing.T) {
	t.Parallel()

	cert, err := ioutil.ReadFile("./testdata/sample.cert")
	require.NoError(t, err)
	dcCert, _ := pem.Decode(cert)

	setup := func(db worldstate.DB) {
		adminUser := &types.User{
			ID: "adminUser",
			Privilege: &types.Privilege{
				ClusterAdministration: true,
			},
		}
		adminUserSerialized, err := proto.Marshal(adminUser)
		require.NoError(t, err)

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
	}

	tests := []struct {
		name            string
		block           *types.Block
		expectedResults []*types.ValidationInfo
	}{
		{
			name: "config block with an invalid transaction",
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_ConfigTxEnvelope{
					ConfigTxEnvelope: &types.ConfigTxEnvelope{
						Payload: &types.ConfigTx{
							UserID: "adminUser",
							ReadOldConfigVersion: &types.Version{
								BlockNum: 100,
								TxNum:    100,
							},
							NewConfig: &types.ClusterConfig{
								Nodes: []*types.NodeConfig{
									{
										ID:          "node1",
										Address:     "127.0.0.1",
										Certificate: dcCert.Bytes,
									},
								},
								Admins: []*types.Admin{
									{
										ID:          "admin1",
										Certificate: dcCert.Bytes,
									},
								},
							},
						},
					},
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
					ReasonIfInvalid: "mvcc conflict has occurred as the read old configuration does not match the committed version",
				},
			},
		},
		{
			name: "config block with a valid transaction",
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_ConfigTxEnvelope{
					ConfigTxEnvelope: &types.ConfigTxEnvelope{
						Payload: &types.ConfigTx{
							UserID:               "adminUser",
							ReadOldConfigVersion: nil,
							NewConfig: &types.ClusterConfig{
								Nodes: []*types.NodeConfig{
									{
										ID:          "node1",
										Address:     "127.0.0.1",
										Certificate: dcCert.Bytes,
									},
								},
								Admins: []*types.Admin{
									{
										ID:          "admin1",
										Certificate: dcCert.Bytes,
									},
								},
							},
						},
					},
				},
			},
			expectedResults: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
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

			results, err := env.validator.validateBlock(tt.block)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResults, results)
		})
	}
}
