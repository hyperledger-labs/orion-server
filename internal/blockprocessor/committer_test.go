package blockprocessor

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/blockstore"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/internal/worldstate/leveldb"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type committerTestEnv struct {
	db              *leveldb.LevelDB
	dbPath          string
	blockStore      *blockstore.Store
	blockStorePath  string
	identityQuerier *identity.Querier
	committer       *committer
	cleanup         func()
}

func newCommitterTestEnv(t *testing.T) *committerTestEnv {
	lc := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(lc)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("/tmp", "committer")
	require.NoError(t, err)

	dbPath := filepath.Join(dir, "leveldb")
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    logger,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, rmErr)
		}
		t.Fatalf("error while creating leveldb, %v", err)
	}

	blockStorePath := filepath.Join(dir, "blockstore")
	blockStore, err := blockstore.Open(
		&blockstore.Config{
			StoreDir: blockStorePath,
			Logger:   logger,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, rmErr)
		}
		t.Fatalf("error while creating blockstore, %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("error while closing the db instance, %v", err)
		}

		if err := blockStore.Close(); err != nil {
			t.Errorf("error while closing blockstore, %v", err)
		}

		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("error while removing directory %s, %v", dir, err)
		}
	}

	c := &Config{
		DB:         db,
		BlockStore: blockStore,
		Logger:     logger,
	}
	return &committerTestEnv{
		db:              db,
		dbPath:          dbPath,
		blockStore:      blockStore,
		blockStorePath:  blockStorePath,
		identityQuerier: identity.NewQuerier(db),
		committer:       newCommitter(c),
		cleanup:         cleanup,
	}
}

func TestCommitter(t *testing.T) {
	t.Parallel()

	t.Run("commit block to block store and state db", func(t *testing.T) {
		t.Parallel()

		env := newCommitterTestEnv(t)
		defer env.cleanup()

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
		require.NoError(t, env.db.Commit(createDB, 1))

		block1 := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 1,
				},
				ValidationInfo: []*types.ValidationInfo{
					{
						Flag: types.Flag_VALID,
					},
				},
			},
			Payload: &types.Block_DataTxEnvelopes{
				DataTxEnvelopes: &types.DataTxEnvelopes{
					Envelopes: []*types.DataTxEnvelope{
						{
							Payload: &types.DataTx{
								DBName: "db1",
								DataWrites: []*types.DataWrite{
									{
										Key:   "db1-key1",
										Value: []byte("value-1"),
									},
								},
							},
						},
					},
				},
			},
		}

		err := env.committer.commitBlock(block1)
		require.NoError(t, err)

		height, err := env.blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(1), height)

		block, err := env.blockStore.Get(1)
		require.NoError(t, err)
		require.True(t, proto.Equal(block, block1))

		val, metadata, err := env.db.Get("db1", "db1-key1")
		require.NoError(t, err)

		expectedMetadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    0,
			},
		}
		require.True(t, proto.Equal(expectedMetadata, metadata))
		require.Equal(t, val, []byte("value-1"))
	})
}

func TestBlockStoreCommitter(t *testing.T) {
	t.Parallel()

	getSampleBlock := func(number uint64) *types.Block {
		return &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: number,
				},
				ValidationInfo: []*types.ValidationInfo{
					{
						Flag: types.Flag_VALID,
					},
					{
						Flag: types.Flag_VALID,
					},
				},
			},
			Payload: &types.Block_DataTxEnvelopes{
				DataTxEnvelopes: &types.DataTxEnvelopes{
					Envelopes: []*types.DataTxEnvelope{
						{
							Payload: &types.DataTx{
								DBName: "db1",
								DataWrites: []*types.DataWrite{
									{
										Key:   fmt.Sprintf("db1-key%d", number),
										Value: []byte(fmt.Sprintf("value-%d", number)),
									},
								},
							},
						},
						{
							Payload: &types.DataTx{
								DBName: "db2",
								DataWrites: []*types.DataWrite{
									{
										Key:   fmt.Sprintf("db2-key%d", number),
										Value: []byte(fmt.Sprintf("value-%d", number)),
									},
								},
							},
						},
					},
				},
			},
		}
	}

	t.Run("commit multiple blocks to the block store and query the same", func(t *testing.T) {
		t.Parallel()

		env := newCommitterTestEnv(t)
		defer env.cleanup()

		var expectedBlocks []*types.Block

		for blockNumber := uint64(1); blockNumber <= 100; blockNumber++ {
			block := getSampleBlock(blockNumber)
			require.NoError(t, env.committer.commitToBlockStore(block))
			expectedBlocks = append(expectedBlocks, block)
		}

		for blockNumber := uint64(1); blockNumber <= 100; blockNumber++ {
			block, err := env.blockStore.Get(blockNumber)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedBlocks[blockNumber-1], block))
		}

		height, err := env.blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(100), height)
	})

	t.Run("commit unexpected block to the block store", func(t *testing.T) {
		t.Parallel()

		env := newCommitterTestEnv(t)
		defer env.cleanup()

		block := getSampleBlock(10)
		err := env.committer.commitToBlockStore(block)
		require.EqualError(t, err, "expected block number [1] but received [10]")
	})
}

func TestStateDBCommitterForDataBlock(t *testing.T) {
	t.Parallel()

	setup := func(db worldstate.DB) {
		data := []*worldstate.DBUpdates{
			{
				DBName: worldstate.DefaultDBName,
				Writes: []*worldstate.KVWithMetadata{
					constructDataEntryForTest("key1", []byte("value1"), &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
						AccessControl: &types.AccessControl{
							ReadWriteUsers: map[string]bool{
								"user1": true,
							},
						},
					}),
					constructDataEntryForTest("key2", []byte("value2"), nil),
					constructDataEntryForTest("key3", []byte("value3"), nil),
				},
			},
		}

		require.NoError(t, db.Commit(data, 1))
	}

	expectedDataBefore := []*worldstate.KVWithMetadata{
		constructDataEntryForTest("key1", []byte("value1"), &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    1,
			},
			AccessControl: &types.AccessControl{
				ReadWriteUsers: map[string]bool{
					"user1": true,
				},
			},
		}),
		constructDataEntryForTest("key2", []byte("value2"), nil),
		constructDataEntryForTest("key3", []byte("value3"), nil),
	}

	tests := []struct {
		name              string
		txs               []*types.DataTxEnvelope
		valInfo           []*types.ValidationInfo
		expectedDataAfter []*worldstate.KVWithMetadata
	}{
		{
			name: "update existing, add new, and delete existing",
			txs: []*types.DataTxEnvelope{
				{
					Payload: &types.DataTx{
						DBName: worldstate.DefaultDBName,
						DataWrites: []*types.DataWrite{
							{
								Key:   "key2",
								Value: []byte("new-value2"),
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
					},
				},
				{
					Payload: &types.DataTx{
						DBName: worldstate.DefaultDBName,
						DataWrites: []*types.DataWrite{
							{
								Key:   "key3",
								Value: []byte("new-value3"),
							},
						},
					},
				},
				{
					Payload: &types.DataTx{
						DBName: worldstate.DefaultDBName,
						DataWrites: []*types.DataWrite{
							{
								Key:   "key4",
								Value: []byte("value4"),
							},
						},
					},
				},
				{
					Payload: &types.DataTx{
						DBName: worldstate.DefaultDBName,
						DataDeletes: []*types.DataDelete{
							{
								Key: "key1",
							},
						},
					},
				},
				{
					Payload: &types.DataTx{
						DBName: worldstate.DefaultDBName,
						DataDeletes: []*types.DataDelete{
							{
								Key: "key2",
							},
						},
					},
				},
			},
			valInfo: []*types.ValidationInfo{
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
				{
					Flag: types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				},
			},
			expectedDataAfter: []*worldstate.KVWithMetadata{
				constructDataEntryForTest("key2", []byte("new-value2"), &types.Metadata{
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    0,
					},
					AccessControl: &types.AccessControl{
						ReadUsers: map[string]bool{
							"user1": true,
						},
						ReadWriteUsers: map[string]bool{
							"user2": true,
						},
					},
				}),
				constructDataEntryForTest("key3", []byte("new-value3"), &types.Metadata{
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    1,
					},
				}),
				constructDataEntryForTest("key4", []byte("value4"), &types.Metadata{
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    2,
					},
				}),
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCommitterTestEnv(t)
			defer env.cleanup()

			setup(env.db)

			for _, kv := range expectedDataBefore {
				val, meta, err := env.db.Get(worldstate.DefaultDBName, kv.Key)
				require.NoError(t, err)
				require.Equal(t, kv.Value, val)
				require.Equal(t, kv.Metadata, meta)
			}

			block := &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
					ValidationInfo: tt.valInfo,
				},
				Payload: &types.Block_DataTxEnvelopes{
					DataTxEnvelopes: &types.DataTxEnvelopes{
						Envelopes: tt.txs,
					},
				},
			}
			require.NoError(t, env.committer.commitToStateDB(block))

			for _, kv := range tt.expectedDataAfter {
				val, meta, err := env.db.Get(worldstate.DefaultDBName, kv.Key)
				require.NoError(t, err)
				require.Equal(t, kv.Value, val)
				require.Equal(t, kv.Metadata, meta)
			}
		})
	}
}

func TestStateDBCommitterForUserBlock(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}

	tests := []struct {
		name                string
		setup               func(db worldstate.DB)
		expectedUsersBefore []string
		tx                  *types.UserAdministrationTx
		valInfo             []*types.ValidationInfo
		expectedUsersAfter  []string
	}{
		{
			name: "add and delete users",
			setup: func(db worldstate.DB) {
				users := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "user1", sampleVersion, nil),
							constructUserForTest(t, "user2", sampleVersion, nil),
							constructUserForTest(t, "user3", sampleVersion, nil),
							constructUserForTest(t, "user4", sampleVersion, nil),
						},
					},
				}
				require.NoError(t, db.Commit(users, 1))
			},
			expectedUsersBefore: []string{"user1", "user2", "user3", "user4"},
			tx: &types.UserAdministrationTx{
				UserWrites: []*types.UserWrite{
					{
						User: &types.User{
							ID:          "user4",
							Certificate: []byte("new-certificate~user4"),
						},
					},
					{
						User: &types.User{
							ID:          "user5",
							Certificate: []byte("certificate~user5"),
						},
					},
					{
						User: &types.User{
							ID:          "user6",
							Certificate: []byte("certificate~user6"),
						},
					},
				},
				UserDeletes: []*types.UserDelete{
					{
						UserID: "user1",
					},
					{
						UserID: "user2",
					},
				},
			},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
			expectedUsersAfter: []string{"user3", "user4", "user5", "user6"},
		},
		{
			name: "tx is marked invalid",
			setup: func(db worldstate.DB) {
				users := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "user1", sampleVersion, nil),
						},
					},
				}
				require.NoError(t, db.Commit(users, 1))
			},
			expectedUsersBefore: []string{"user1"},
			tx: &types.UserAdministrationTx{
				UserWrites: []*types.UserWrite{
					{
						User: &types.User{
							ID:          "user2",
							Certificate: []byte("new-certificate~user2"),
						},
					},
				},
				UserDeletes: []*types.UserDelete{
					{
						UserID: "user1",
					},
				},
			},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_INVALID_NO_PERMISSION,
				},
			},
			expectedUsersAfter: []string{"user1"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCommitterTestEnv(t)
			defer env.cleanup()

			tt.setup(env.db)

			for _, user := range tt.expectedUsersBefore {
				exist, err := env.identityQuerier.DoesUserExist(user)
				require.NoError(t, err)
				require.True(t, exist)
			}

			block := &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
					ValidationInfo: tt.valInfo,
				},
				Payload: &types.Block_UserAdministrationTxEnvelope{
					UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
						Payload: tt.tx,
					},
				},
			}
			require.NoError(t, env.committer.commitToStateDB(block))

			for _, user := range tt.expectedUsersAfter {
				exist, err := env.identityQuerier.DoesUserExist(user)
				require.NoError(t, err)
				require.True(t, exist)
			}
		})
	}
}

func TestStateDBCommitterForDBBlock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		setup             func(db worldstate.DB)
		expectedDBsBefore []string
		dbAdminTx         *types.DBAdministrationTx
		expectedDBsAfter  []string
		valInfo           []*types.ValidationInfo
	}{
		{
			name: "create new DBss and delete existing DBs",
			setup: func(db worldstate.DB) {
				createDBs := []*worldstate.DBUpdates{
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

				require.NoError(t, db.Commit(createDBs, 1))
			},
			expectedDBsBefore: []string{"db1", "db2"},
			dbAdminTx: &types.DBAdministrationTx{
				CreateDBs: []string{"db3", "db4"},
				DeleteDBs: []string{"db1", "db2"},
			},
			expectedDBsAfter: []string{"db3", "db4"},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
		{
			name: "tx marked invalid",
			setup: func(db worldstate.DB) {
				createDBs := []*worldstate.DBUpdates{
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

				require.NoError(t, db.Commit(createDBs, 1))
			},
			expectedDBsBefore: []string{"db1", "db2"},
			dbAdminTx: &types.DBAdministrationTx{
				DeleteDBs: []string{"db1", "db2"},
			},
			expectedDBsAfter: []string{"db1", "db2"},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_INVALID_NO_PERMISSION,
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCommitterTestEnv(t)
			defer env.cleanup()

			tt.setup(env.db)

			for _, dbName := range tt.expectedDBsBefore {
				require.True(t, env.db.Exist(dbName))
			}

			block := &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
					ValidationInfo: tt.valInfo,
				},
				Payload: &types.Block_DBAdministrationTxEnvelope{
					DBAdministrationTxEnvelope: &types.DBAdministrationTxEnvelope{
						Payload: tt.dbAdminTx,
					},
				},
			}
			require.NoError(t, env.committer.commitToStateDB(block))

			for _, dbName := range tt.expectedDBsAfter {
				require.True(t, env.db.Exist(dbName))
			}
		})
	}
}

func TestStateDBCommitterForConfigBlock(t *testing.T) {
	t.Parallel()

	generateSampleConfigBlock := func(number uint64, adminsID []string, valInfo []*types.ValidationInfo) *types.Block {
		var admins []*types.Admin
		for _, id := range adminsID {
			admins = append(admins, &types.Admin{
				ID:          id,
				Certificate: []byte("certificate~" + id),
			})
		}

		clusterConfig := &types.ClusterConfig{
			Nodes: []*types.NodeConfig{
				{
					ID:          "bdb-node-1",
					Certificate: []byte("node-cert"),
					Address:     "127.0.0.1",
					Port:        0,
				},
			},
			Admins:            admins,
			RootCACertificate: []byte("root-ca"),
		}

		configBlock := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: number,
				},
				ValidationInfo: valInfo,
			},
			Payload: &types.Block_ConfigTxEnvelope{
				ConfigTxEnvelope: &types.ConfigTxEnvelope{
					Payload: &types.ConfigTx{
						NewConfig: clusterConfig,
					},
				},
			},
		}

		return configBlock
	}

	assertExpectedUsers := func(t *testing.T, q *identity.Querier, expectedUsers []*types.User) {
		for _, expectedUser := range expectedUsers {
			user, _, err := q.GetUser(expectedUser.ID)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedUser, user))
		}
	}

	tests := []struct {
		name                        string
		adminsInCommittedConfigTx   []string
		expectedClusterAdminsBefore []*types.User
		adminsInNewConfigTx         []string
		expectedClusterAdminsAfter  []*types.User
		valInfo                     []*types.ValidationInfo
	}{
		{
			name:                      "no change in the set of admins",
			adminsInCommittedConfigTx: []string{"admin1", "admin2"},
			expectedClusterAdminsBefore: []*types.User{
				constructAdminEntryForTest("admin1"),
				constructAdminEntryForTest("admin2"),
			},
			adminsInNewConfigTx: []string{"admin1", "admin2"},
			expectedClusterAdminsAfter: []*types.User{
				constructAdminEntryForTest("admin1"),
				constructAdminEntryForTest("admin2"),
			},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
		{
			name:                      "add and delete admins",
			adminsInCommittedConfigTx: []string{"admin1", "admin2", "admin3"},
			expectedClusterAdminsBefore: []*types.User{
				constructAdminEntryForTest("admin1"),
				constructAdminEntryForTest("admin2"),
				constructAdminEntryForTest("admin3"),
			},
			adminsInNewConfigTx: []string{"admin3", "admin4", "admin5"},
			expectedClusterAdminsAfter: []*types.User{
				constructAdminEntryForTest("admin3"),
				constructAdminEntryForTest("admin4"),
				constructAdminEntryForTest("admin5"),
			},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
		{
			name:                      "tx is marked invalid",
			adminsInCommittedConfigTx: []string{"admin1"},
			expectedClusterAdminsBefore: []*types.User{
				constructAdminEntryForTest("admin1"),
			},
			adminsInNewConfigTx: []string{"admin1", "admin2"},
			expectedClusterAdminsAfter: []*types.User{
				constructAdminEntryForTest("admin1"),
			},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_INVALID_NO_PERMISSION,
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCommitterTestEnv(t)
			defer env.cleanup()

			var blockNumber uint64

			validationInfo := []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			}
			blockNumber = 1
			configBlock := generateSampleConfigBlock(blockNumber, tt.adminsInCommittedConfigTx, validationInfo)
			env.committer.commitToStateDB(configBlock)
			assertExpectedUsers(t, env.identityQuerier, tt.expectedClusterAdminsBefore)

			blockNumber++
			configBlock = generateSampleConfigBlock(blockNumber, tt.adminsInNewConfigTx, tt.valInfo)
			env.committer.commitToStateDB(configBlock)
			assertExpectedUsers(t, env.identityQuerier, tt.expectedClusterAdminsAfter)
		})
	}
}

func constructDataEntryForTest(key string, value []byte, metadata *types.Metadata) *worldstate.KVWithMetadata {
	return &worldstate.KVWithMetadata{
		Key:      key,
		Value:    value,
		Metadata: metadata,
	}
}

func constructAdminEntryForTest(userID string) *types.User {
	return &types.User{
		ID:          userID,
		Certificate: []byte("certificate~" + userID),
		Privilege: &types.Privilege{
			DBAdministration:      true,
			ClusterAdministration: true,
			UserAdministration:    true,
		},
	}
}
