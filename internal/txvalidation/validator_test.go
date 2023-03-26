// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package txvalidation

import (
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

type validatorTestEnv struct {
	db        *leveldb.LevelDB
	path      string
	validator *Validator
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

	dir := t.TempDir()
	path := filepath.Join(dir, "leveldb")

	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: path,
			Logger:    logger,
		},
	)
	if err != nil {
		t.Fatalf("failed to create leveldb with path %s", path)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close the db instance, %v", err)
		}
	}

	return &validatorTestEnv{
		db:   db,
		path: path,
		validator: NewValidator(
			&Config{
				DB:      db,
				Logger:  logger,
				Metrics: utils.NewTxProcessingMetrics(nil),
			},
		),
		cleanup: cleanup,
	}
}

func TestValidateGenesisBlock(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"admin1", "node1"})
	adminCert, _ := testutils.LoadTestCrypto(t, cryptoDir, "admin1")
	nodeCert, _ := testutils.LoadTestCrypto(t, cryptoDir, "node1")
	caCert, _ := testutils.LoadTestCA(t, cryptoDir, testutils.RootCAFileName)

	//TODO test when admin and node cert are not signed by correct CA

	t.Run("genesis block with invalid config tx", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name          string
			genesisBlock  *types.Block
			expectedError string
		}{
			{
				name: "root CA config is missing",
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
											Id:          "node1",
											Address:     "127.0.0.1",
											Port:        6090,
											Certificate: nodeCert.Raw,
										},
									},
									Admins: []*types.Admin{
										{
											Id:          "admin1",
											Certificate: adminCert.Raw,
										},
									},
								},
							},
						},
					},
				},
				expectedError: "genesis block cannot be invalid: reason for invalidation [CA config is empty. At least one root CA is required]",
			},
			{
				name: "root CA cert is invalid",
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
											Id:          "node1",
											Address:     "127.0.0.1",
											Port:        6090,
											Certificate: nodeCert.Raw,
										},
									},
									Admins: []*types.Admin{
										{
											Id:          "admin1",
											Certificate: adminCert.Raw,
										},
									},
									CertAuthConfig: &types.CAConfig{
										Roots: [][]byte{[]byte("bad-certificate")},
									},
								},
							},
						},
					},
				},
				expectedError: "genesis block cannot be invalid: reason for invalidation [CA certificate collection cannot be created: x509: malformed certificate",
			},
			{
				name: "root CA cert is not from a CA",
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
											Id:          "node1",
											Address:     "127.0.0.1",
											Port:        6090,
											Certificate: nodeCert.Raw,
										},
									},
									Admins: []*types.Admin{
										{
											Id:          "admin1",
											Certificate: adminCert.Raw,
										},
									},
									CertAuthConfig: &types.CAConfig{
										Roots: [][]byte{adminCert.Raw},
									},
								},
							},
						},
					},
				},
				expectedError: "genesis block cannot be invalid: reason for invalidation [CA certificate collection cannot be created: certificate is missing the CA property, SN:",
			},
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
											Id:          "node1",
											Address:     "127.0.0.1",
											Port:        6090,
											Certificate: []byte("random"),
										},
									},
									Admins: []*types.Admin{
										{
											Id:          "admin1",
											Certificate: adminCert.Raw,
										},
									},
									CertAuthConfig: &types.CAConfig{
										Roots: [][]byte{caCert.Raw},
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
											Id:          "node1",
											Address:     "127.0.0.1",
											Port:        6090,
											Certificate: nodeCert.Raw,
										},
									},
									Admins: []*types.Admin{
										{
											Id:          "admin1",
											Certificate: []byte("random"),
										},
									},
									CertAuthConfig: &types.CAConfig{
										Roots: [][]byte{caCert.Raw},
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

				results, err := env.validator.ValidateBlock(tt.genesisBlock)
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
									Id:          "node1",
									Address:     "127.0.0.1",
									Port:        6090,
									Certificate: nodeCert.Raw,
								},
							},
							Admins: []*types.Admin{
								{
									Id:          "admin1",
									Certificate: adminCert.Raw,
								},
							},
							CertAuthConfig: &types.CAConfig{
								Roots: [][]byte{caCert.Raw},
							},
							ConsensusConfig: &types.ConsensusConfig{
								Algorithm: "raft",
								Members: []*types.PeerConfig{
									{
										NodeId:   "node1",
										RaftId:   1,
										PeerHost: "10.10.10.10",
										PeerPort: 7090,
									},
								},
								RaftConfig: &types.RaftConfig{
									TickInterval:   "100ms",
									ElectionTicks:  100,
									HeartbeatTicks: 10,
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

		results, err := env.validator.ValidateBlock(genesisBlock)
		require.NoError(t, err)
		require.Equal(t, expectedResults, results)
	})
}

func TestValidateDataBlock(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"operatingUser"})
	userCert, userSigner := testutils.LoadTestCrypto(t, cryptoDir, "operatingUser")

	addUserWithCorrectPrivilege := func(db worldstate.DB) {
		user := &types.User{
			Id:          "operatingUser",
			Certificate: userCert.Raw,
			Privilege: &types.Privilege{
				DbPermission: map[string]types.Privilege_Access{
					worldstate.DefaultDBName: types.Privilege_ReadWrite,
					"db1":                    types.Privilege_ReadWrite,
				},
			},
		}
		userSerialized, err := proto.Marshal(user)
		require.NoError(t, err)

		userAdd := map[string]*worldstate.DBUpdates{
			worldstate.UsersDBName: {
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
				db1 := map[string]*worldstate.DBUpdates{
					worldstate.DatabasesDBName: {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "db1",
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
				require.NoError(t, db.Commit(db1, 1))

				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
					"db1": {
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
							{
								Key: "key2",
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
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
				},
				Payload: &types.Block_DataTxEnvelopes{
					DataTxEnvelopes: &types.DataTxEnvelopes{
						Envelopes: []*types.DataTxEnvelope{
							testutils.SignedDataTxEnvelope(t, []crypto.Signer{userSigner}, &types.DataTx{
								MustSignUserIds: []string{"operatingUser"},
								DbOperations: []*types.DBOperation{
									{
										DbName: worldstate.DefaultDBName,
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
									{
										DbName: "db1",
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
							}),
							testutils.SignedDataTxEnvelope(t, []crypto.Signer{userSigner}, &types.DataTx{
								MustSignUserIds: []string{"operatingUser"},
								DbOperations: []*types.DBOperation{
									{
										DbName: worldstate.DefaultDBName,
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
							}),
							testutils.SignedDataTxEnvelope(t, []crypto.Signer{userSigner}, &types.DataTx{
								MustSignUserIds: []string{"operatingUser"},
								DbOperations: []*types.DBOperation{
									{
										DbName: worldstate.DefaultDBName,
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
							}),
							testutils.SignedDataTxEnvelope(t, []crypto.Signer{userSigner}, &types.DataTx{
								MustSignUserIds: []string{"operatingUser"},
								DbOperations: []*types.DBOperation{
									{
										DbName: worldstate.DefaultDBName,
									},
									{
										DbName: "db2",
									},
								},
							}),
							testutils.SignedDataTxEnvelope(t, []crypto.Signer{userSigner}, &types.DataTx{
								MustSignUserIds: []string{"operatingUser"},
								DbOperations: []*types.DBOperation{
									{
										DbName: worldstate.DefaultDBName,
										DataWrites: []*types.DataWrite{
											{
												Key:   "key1",
												Value: []byte("new-val"),
											},
										},
									},
								},
							}),
							testutils.SignedDataTxEnvelope(t, []crypto.Signer{userSigner}, &types.DataTx{
								MustSignUserIds: []string{"operatingUser"},
								DbOperations: []*types.DBOperation{
									{
										DbName: "db1",
										DataDeletes: []*types.DataDelete{
											{
												Key: "key1",
											},
										},
									},
								},
							}),
							testutils.SignedDataTxEnvelope(t, []crypto.Signer{userSigner}, &types.DataTx{
								MustSignUserIds: []string{"operatingUser"},
								DbOperations: []*types.DBOperation{
									{
										DbName: "db1",
										DataDeletes: []*types.DataDelete{
											{
												Key: "key5",
											},
										},
									},
								},
							}),
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
				{
					Flag:            types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
					ReasonIfInvalid: "the database [db2] does not exist in the cluster",
				},
				{
					Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
					ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [" + worldstate.DefaultDBName + "]. Within a block, a key can be modified only once",
				},
				{
					Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
					ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [db1]. Within a block, a key can be modified only once",
				},
				{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the key [key5] does not exist in the database and hence, it cannot be deleted",
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

			results, err := env.validator.ValidateBlock(tt.block)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResults, results)
		})
	}
}

func TestValidateUserBlock(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"adminUser"})
	adminCert, adminSigner := testutils.LoadTestCrypto(t, cryptoDir, "adminUser")
	caCert, _ := testutils.LoadTestCA(t, cryptoDir, testutils.RootCAFileName)
	require.True(t, caCert.IsCA)

	adminUser := &types.User{
		Id:          "adminUser",
		Certificate: adminCert.Raw,
		Privilege: &types.Privilege{
			Admin: true,
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
				newUsers := map[string]*worldstate.DBUpdates{
					worldstate.UsersDBName: {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(identity.UserNamespace) + "adminUser",
								Value: adminUserSerialized,
							},
							constructUserForTest(t, "user1", nil, nil, sampleVersion, &types.AccessControl{
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
					UserAdministrationTxEnvelope: testutils.SignedUserAdministrationTxEnvelope(t, adminSigner,
						&types.UserAdministrationTx{
							UserId: "adminUser",
							UserReads: []*types.UserRead{
								{
									UserId: "user1",
									Version: &types.Version{
										BlockNum: 100,
										TxNum:    1000,
									},
								},
							},
						},
					),
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
				newUsers := map[string]*worldstate.DBUpdates{
					worldstate.UsersDBName: {
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
					UserAdministrationTxEnvelope: testutils.SignedUserAdministrationTxEnvelope(t, adminSigner,
						&types.UserAdministrationTx{
							UserId: "adminUser",
							UserReads: []*types.UserRead{
								{
									UserId: "user1",
								},
								{
									UserId: "user2",
								},
							},
						},
					),
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
			setupClusterConfigCA(t, env, caCert)
			tt.setup(env.db)

			results, err := env.validator.ValidateBlock(tt.block)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResults, results)
		})
	}
}

func TestValidateDBBlock(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"userWithMorePrivilege", "userWithLessPrivilege"})
	adminCert, adminSigner := testutils.LoadTestCrypto(t, cryptoDir, "userWithMorePrivilege")

	setup := func(db worldstate.DB) {
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

		dbs := map[string]*worldstate.DBUpdates{
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
				Payload: &types.Block_DbAdministrationTxEnvelope{
					DbAdministrationTxEnvelope: testutils.SignedDBAdministrationTxEnvelope(t, adminSigner,
						&types.DBAdministrationTx{
							UserId:    "userWithMorePrivilege",
							DeleteDbs: []string{"db1"},
						}),
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
				Payload: &types.Block_DbAdministrationTxEnvelope{
					DbAdministrationTxEnvelope: testutils.SignedDBAdministrationTxEnvelope(t, adminSigner,
						&types.DBAdministrationTx{
							UserId:    "userWithMorePrivilege",
							CreateDbs: []string{"db1", "db2"},
							DeleteDbs: []string{"db3", "db4"},
						}),
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

			results, err := env.validator.ValidateBlock(tt.block)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResults, results)
		})
	}
}

func TestValidateConfigBlock(t *testing.T) {
	t.Parallel()

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"adminUser", "admin1", "node1"})
	userCert, userSigner := testutils.LoadTestCrypto(t, cryptoDir, "adminUser")
	adminCert, _ := testutils.LoadTestCrypto(t, cryptoDir, "admin1")
	nodeCert, _ := testutils.LoadTestCrypto(t, cryptoDir, "node1")
	caCert, _ := testutils.LoadTestCA(t, cryptoDir, testutils.RootCAFileName)

	setup := func(db worldstate.DB) {
		adminUser := &types.User{
			Id:          "adminUser",
			Certificate: userCert.Raw,
			Privilege: &types.Privilege{
				Admin: true,
			},
		}
		adminUserSerialized, err := proto.Marshal(adminUser)
		require.NoError(t, err)

		config := &types.ClusterConfig{
			Nodes: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "127.0.0.1",
					Port:        6090,
					Certificate: nodeCert.Raw,
				},
			},
			ConsensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					{
						NodeId:   "node1",
						RaftId:   1,
						PeerHost: "127.0.0.1",
						PeerPort: 7090,
					},
				},
				Observers: nil,
				RaftConfig: &types.RaftConfig{
					TickInterval:   "100ms",
					ElectionTicks:  100,
					HeartbeatTicks: 10,
				},
			},
		}
		configSerialized, err := proto.Marshal(config)
		require.NoError(t, err)

		dbUpdates := map[string]*worldstate.DBUpdates{
			worldstate.UsersDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + "adminUser",
						Value: adminUserSerialized,
					},
				},
			},
			worldstate.ConfigDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   worldstate.ConfigKey,
						Value: configSerialized,
						Metadata: &types.Metadata{
							Version: &types.Version{BlockNum: 1, TxNum: 1},
						},
					},
				},
			},
		}

		require.NoError(t, db.Commit(dbUpdates, 1))
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
					ConfigTxEnvelope: testutils.SignedConfigTxEnvelope(t, userSigner,
						&types.ConfigTx{
							UserId: "adminUser",
							ReadOldConfigVersion: &types.Version{
								BlockNum: 100,
								TxNum:    100,
							},
							NewConfig: &types.ClusterConfig{
								Nodes: []*types.NodeConfig{
									{
										Id:          "node1",
										Address:     "127.0.0.1",
										Port:        6090,
										Certificate: nodeCert.Raw,
									},
								},
								Admins: []*types.Admin{
									{
										Id:          "admin1",
										Certificate: adminCert.Raw,
									},
								},
								CertAuthConfig: &types.CAConfig{
									Roots: [][]byte{caCert.Raw},
								},
								ConsensusConfig: &types.ConsensusConfig{
									Algorithm: "raft",
									Members: []*types.PeerConfig{
										{
											NodeId:   "node1",
											RaftId:   1,
											PeerHost: "10.10.10.10",
											PeerPort: 7090,
										},
									},
									RaftConfig: &types.RaftConfig{
										TickInterval:   "100ms",
										ElectionTicks:  100,
										HeartbeatTicks: 10,
									},
								},
							},
						}),
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
					ConfigTxEnvelope: testutils.SignedConfigTxEnvelope(t, userSigner,
						&types.ConfigTx{
							UserId: "adminUser",
							ReadOldConfigVersion: &types.Version{
								BlockNum: 1,
								TxNum:    1,
							},
							NewConfig: &types.ClusterConfig{
								Nodes: []*types.NodeConfig{
									{
										Id:          "node1",
										Address:     "127.0.0.1",
										Port:        6090,
										Certificate: nodeCert.Raw,
									},
								},
								Admins: []*types.Admin{
									{
										Id:          "admin1",
										Certificate: adminCert.Raw,
									},
									{
										Id:          "admin2", //<<< changed
										Certificate: adminCert.Raw,
									},
								},
								CertAuthConfig: &types.CAConfig{
									Roots: [][]byte{caCert.Raw},
								},
								ConsensusConfig: &types.ConsensusConfig{
									Algorithm: "raft",
									Members: []*types.PeerConfig{
										{
											NodeId:   "node1",
											RaftId:   1,
											PeerHost: "127.0.0.1",
											PeerPort: 7090,
										},
									},
									Observers: nil,
									RaftConfig: &types.RaftConfig{
										TickInterval:   "100ms",
										ElectionTicks:  100,
										HeartbeatTicks: 10,
									},
								},
							},
						}),
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

			results, err := env.validator.ValidateBlock(tt.block)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResults, results, "%+v", results)
		})
	}
}
