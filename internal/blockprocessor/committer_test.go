// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	mptrieStore "github.com/hyperledger-labs/orion-server/internal/mptrie/store"
	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/internal/stateindex"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
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

	dir := t.TempDir()

	dbPath := filepath.Join(dir, "leveldb")
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    logger,
		},
	)
	if err != nil {
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
		t.Fatalf("error while creating blockstore, %v", err)
	}

	provenanceStorePath := filepath.Join(dir, "provenancestore")
	provenanceStore, err := provenance.Open(
		&provenance.Config{
			StoreDir: provenanceStorePath,
			Logger:   logger,
		},
	)

	mptrieStorePath := filepath.Join(dir, "statetriestore")
	mptrieStore, err := mptrieStore.Open(
		&mptrieStore.Config{
			StoreDir: mptrieStorePath,
			Logger:   logger,
		},
	)

	if err != nil {
		t.Fatalf("error while creating the block store, %v", err)
	}

	cleanup := func() {
		if err := provenanceStore.Close(); err != nil {
			t.Errorf("error while closing the provenance store, %v", err)
		}

		if err := db.Close(); err != nil {
			t.Errorf("error while closing the db instance, %v", err)
		}

		if err := blockStore.Close(); err != nil {
			t.Errorf("error while closing blockstore, %v", err)
		}
	}

	c := &Config{
		BlockStore:      blockStore,
		DB:              db,
		ProvenanceStore: provenanceStore,
		StateTrieStore:  mptrieStore,
		Logger:          logger,
		Metrics:         utils.NewTxProcessingMetrics(nil),
	}
	env := &committerTestEnv{
		db:              db,
		dbPath:          dbPath,
		blockStore:      blockStore,
		blockStorePath:  blockStorePath,
		identityQuerier: identity.NewQuerier(db),
		committer:       newCommitter(c),
		cleanup:         cleanup,
	}
	_, _, env.committer.stateTrie, err = loadStateTrie(mptrieStore, blockStore)
	require.NoError(t, err)
	return env
}

func TestCommitter(t *testing.T) {
	t.Parallel()

	t.Run("commit block to block store and state db", func(t *testing.T) {
		t.Parallel()

		env := newCommitterTestEnv(t)
		defer env.cleanup()

		createDB := map[string]*worldstate.DBUpdates{
			worldstate.DatabasesDBName: {
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
								MustSignUserIds: []string{"testUser"},
								TxId:            "dataTx1",
								DbOperations: []*types.DBOperation{
									{
										DbName: "db1",
										DataWrites: []*types.DataWrite{
											{
												Key:   "db1-key1",
												Value: []byte("value-1"),
											},
										},
									},
									{
										DbName: "db2",
										DataWrites: []*types.DataWrite{
											{
												Key:   "db2-key1",
												Value: []byte("value-1"),
											},
										},
									},
									{
										DbName: "db3",
										DataWrites: []*types.DataWrite{
											{
												Key:   "db3-key1",
												Value: []byte("value-1"),
											},
										},
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

		for _, db := range []string{"db1", "db2", "db3"} {
			val, metadata, err := env.db.Get(db, db+"-key1")
			require.NoError(t, err)

			expectedMetadata := &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    0,
				},
			}
			require.True(t, proto.Equal(expectedMetadata, metadata))
			require.Equal(t, val, []byte("value-1"))
		}
		stateTrieHash, err := env.committer.stateTrie.Hash()
		require.NoError(t, err)
		require.Equal(t, block.GetHeader().GetStateMerkleTreeRootHash(), stateTrieHash)
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
								TxId: fmt.Sprintf("tx1_%d", number),
								DbOperations: []*types.DBOperation{
									{
										DbName: "db1",
										DataWrites: []*types.DataWrite{
											{
												Key:   fmt.Sprintf("db1-key%d", number),
												Value: []byte(fmt.Sprintf("value-%d", number)),
											},
										},
									},
								},
							},
						},
						{
							Payload: &types.DataTx{
								TxId: fmt.Sprintf("tx1_%d", number),
								DbOperations: []*types.DBOperation{
									{
										DbName: "db2",
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
			_, err := env.committer.commitToBlockStore(block)
			require.NoError(t, err)
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
		_, err := env.committer.commitToBlockStore(block)
		require.EqualError(t, err, "failed to commit block 10 to block store: expected block number [1] but received [10]")
	})
}

func TestStateDBCommitterForDataBlockWithIndex(t *testing.T) {
	t.Parallel()

	expectedDataBefore := []*worldstate.KVWithMetadata{
		{
			Key:   "key1",
			Value: []byte(`{"title":"book1","year":2015,"bestseller":true}`),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 2,
					TxNum:    0,
				},
			},
		},
		{
			Key:   "key2",
			Value: []byte(`{"title":"book2","year":2016,"bestseller":false}`),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 2,
					TxNum:    0,
				},
			},
		},
	}

	encoded2015 := stateindex.EncodeInt64(2015)
	encoded2016 := stateindex.EncodeInt64(2016)
	encoded2018 := stateindex.EncodeInt64(2018)
	encoded2021 := stateindex.EncodeInt64(2021)

	expectedIndexBefore := []*worldstate.KVWithMetadata{
		{
			Key: `{"a":"title","t":1,"vp":2,"v":"book1","kp":2,"k":"key1"}`,
		},
		{
			Key: `{"a":"year","t":0,"vp":2,"v":"` + encoded2015 + `","kp":2,"k":"key1"}`,
		},
		{
			Key: `{"a":"bestseller","t":2,"vp":2,"v":true,"kp":2,"k":"key1"}`,
		},
		{
			Key: `{"a":"title","t":1,"vp":2,"v":"book2","kp":2,"k":"key2"}`,
		},
		{
			Key: `{"a":"year","t":0,"vp":2,"v":"` + encoded2016 + `","kp":2,"k":"key2"}`,
		},
		{
			Key: `{"a":"bestseller","t":2,"vp":2,"v":false,"kp":2,"k":"key2"}`,
		},
	}

	setup := func(c *committer) {
		indexDef := map[string]types.IndexAttributeType{
			"title":      types.IndexAttributeType_STRING,
			"year":       types.IndexAttributeType_NUMBER,
			"bestseller": types.IndexAttributeType_BOOLEAN,
		}
		marshaledIndexDef, err := json.Marshal(indexDef)
		require.NoError(t, err)

		createDbs := map[string]*worldstate.DBUpdates{
			worldstate.DatabasesDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   "db1",
						Value: marshaledIndexDef,
					},
					{
						Key: stateindex.IndexDB("db1"),
					},
				},
			},
		}
		require.NoError(t, c.db.Commit(createDbs, 1))

		dbsUpdates := map[string]*worldstate.DBUpdates{
			"db1": {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   "key1",
						Value: []byte(`{"title":"book1","year":2015,"bestseller":true}`),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    0,
							},
						},
					},
					{
						Key:   "key2",
						Value: []byte(`{"title":"book2","year":2016,"bestseller":false}`),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    0,
							},
						},
					},
				},
			},
		}
		require.NoError(t, c.commitToStateDB(2, dbsUpdates))
	}

	tests := []struct {
		name                string
		txs                 []*types.DataTxEnvelope
		valInfo             []*types.ValidationInfo
		expectedDataBefore  []*worldstate.KVWithMetadata
		expectedIndexBefore []*worldstate.KVWithMetadata
		expectedDataAfter   []*worldstate.KVWithMetadata
		expectedIndexAfter  []*worldstate.KVWithMetadata
	}{
		{
			name: "update existing, add new, and delete existing",
			txs: []*types.DataTxEnvelope{
				{
					Payload: &types.DataTx{
						MustSignUserIds: []string{"testUser"},
						DbOperations: []*types.DBOperation{
							{
								DbName: "db1",
								DataWrites: []*types.DataWrite{
									{
										Key:   "key2",
										Value: []byte(`{"title":"book2","year":2018,"bestseller":true,"newfield":"abc"}`),
									},
								},
							},
						},
					},
				},
				{
					Payload: &types.DataTx{
						MustSignUserIds: []string{"testUser"},
						DbOperations: []*types.DBOperation{
							{
								DbName: "db1",
								DataWrites: []*types.DataWrite{
									{
										Key:   "key3",
										Value: []byte(`{"title":"book3","year":2021,"bestseller":false}`),
									},
								},
							},
						},
					},
				},
				{
					Payload: &types.DataTx{
						MustSignUserIds: []string{"testUser"},
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
			},
			expectedDataBefore:  expectedDataBefore,
			expectedIndexBefore: expectedIndexBefore,
			expectedDataAfter: []*worldstate.KVWithMetadata{
				{
					Key:   "key2",
					Value: []byte(`{"title":"book2","year":2018,"bestseller":true,"newfield":"abc"}`),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    0,
						},
					},
				},
				{
					Key:   "key3",
					Value: []byte(`{"title":"book3","year":2021,"bestseller":false}`),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 3,
							TxNum:    1,
						},
					},
				},
			},
			expectedIndexAfter: []*worldstate.KVWithMetadata{
				{
					Key: `{"a":"title","t":1,"vp":2,"v":"book2","kp":2,"k":"key2"}`,
				},
				{
					Key: `{"a":"year","t":0,"vp":2,"v":"` + encoded2018 + `","kp":2,"k":"key2"}`,
				},
				{
					Key: `{"a":"bestseller","t":2,"vp":2,"v":true,"kp":2,"k":"key2"}`,
				},
				{
					Key: `{"a":"title","t":1,"vp":2,"v":"book3","kp":2,"k":"key3"}`,
				},
				{
					Key: `{"a":"year","t":0,"vp":2,"v":"` + encoded2021 + `","kp":2,"k":"key3"}`,
				},
				{
					Key: `{"a":"bestseller","t":2,"vp":2,"v":false,"kp":2,"k":"key3"}`,
				},
			},
		},
	}

	fetchAllKVsFromDB := func(db worldstate.DB, dbName string) map[string]*worldstate.KVWithMetadata {
		committedData := make(map[string]*worldstate.KVWithMetadata)
		itr, err := db.GetIterator(dbName, "", "")
		defer itr.Release()
		require.NoError(t, err)
		for itr.Next() {
			k := string(itr.Key())
			v := &types.ValueWithMetadata{}
			require.NoError(t, proto.Unmarshal(itr.Value(), v))
			require.NoError(t, err)
			committedData[k] = &worldstate.KVWithMetadata{
				Key:      k,
				Value:    v.Value,
				Metadata: v.Metadata,
			}
		}

		return committedData
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCommitterTestEnv(t)
			defer env.cleanup()

			setup(env.committer)

			committedData := fetchAllKVsFromDB(env.db, "db1")
			require.Equal(t, len(tt.expectedDataBefore), len(committedData))
			for _, expectedKV := range tt.expectedDataBefore {
				require.Equal(t, expectedKV, committedData[expectedKV.Key])
			}

			committedIndex := fetchAllKVsFromDB(env.db, stateindex.IndexDB("db1"))
			require.Equal(t, len(tt.expectedIndexBefore), len(committedIndex))
			for _, expectedIndexKV := range tt.expectedIndexBefore {
				require.Equal(t, expectedIndexKV, committedIndex[expectedIndexKV.Key])
			}

			block := &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 3,
					},
					ValidationInfo: tt.valInfo,
				},
				Payload: &types.Block_DataTxEnvelopes{
					DataTxEnvelopes: &types.DataTxEnvelopes{
						Envelopes: tt.txs,
					},
				},
			}

			dbsUpdates, _, err := env.committer.constructDBAndProvenanceEntries(block)
			require.NoError(t, err)
			require.NoError(t, env.committer.commitToStateDB(2, dbsUpdates))

			committedData = fetchAllKVsFromDB(env.db, "db1")
			require.Equal(t, len(tt.expectedDataAfter), len(committedData))
			for _, expectedKV := range tt.expectedDataAfter {
				require.Equal(t, expectedKV, committedData[expectedKV.Key])
			}

			committedIndex = fetchAllKVsFromDB(env.db, stateindex.IndexDB("db1"))
			require.Equal(t, len(tt.expectedIndexAfter), len(committedIndex))
			for _, expectedIndexKV := range tt.expectedIndexAfter {
				require.Equal(t, expectedIndexKV, committedIndex[expectedIndexKV.Key])
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
		setup               func(env *committerTestEnv)
		expectedUsersBefore []string
		tx                  *types.UserAdministrationTx
		valInfo             []*types.ValidationInfo
		expectedUsersAfter  []string
	}{
		{
			name: "add and delete users",
			setup: func(env *committerTestEnv) {
				user1 := constructUserForTest(t, "user1", sampleVersion)
				user2 := constructUserForTest(t, "user2", sampleVersion)
				user3 := constructUserForTest(t, "user3", sampleVersion)
				user4 := constructUserForTest(t, "user4", sampleVersion)
				users := map[string]*worldstate.DBUpdates{
					worldstate.UsersDBName: {
						Writes: []*worldstate.KVWithMetadata{
							user1,
							user2,
							user3,
							user4,
						},
					},
				}
				require.NoError(t, env.db.Commit(users, 1))

				txsData := []*provenance.TxDataForProvenance{
					{
						IsValid: true,
						DBName:  worldstate.UsersDBName,
						UserID:  "user0",
						TxID:    "tx0",
						Writes: []*types.KVWithMetadata{
							{
								Key:      "user1",
								Value:    user1.Value,
								Metadata: user1.Metadata,
							},
							{
								Key:      "user2",
								Value:    user2.Value,
								Metadata: user2.Metadata,
							},
							{
								Key:      "user3",
								Value:    user3.Value,
								Metadata: user3.Metadata,
							},
							{
								Key:      "user4",
								Value:    user4.Value,
								Metadata: user4.Metadata,
							},
						},
					},
				}
				require.NoError(t, env.committer.provenanceStore.Commit(1, txsData))
			},
			expectedUsersBefore: []string{"user1", "user2", "user3", "user4"},
			tx: &types.UserAdministrationTx{
				UserId: "user0",
				TxId:   "tx1",
				UserWrites: []*types.UserWrite{
					{
						User: &types.User{
							Id:          "user4",
							Certificate: []byte("new-certificate~user4"),
						},
					},
					{
						User: &types.User{
							Id:          "user5",
							Certificate: []byte("certificate~user5"),
						},
					},
					{
						User: &types.User{
							Id:          "user6",
							Certificate: []byte("certificate~user6"),
						},
					},
				},
				UserDeletes: []*types.UserDelete{
					{
						UserId: "user1",
					},
					{
						UserId: "user2",
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
			setup: func(env *committerTestEnv) {
				users := map[string]*worldstate.DBUpdates{
					worldstate.UsersDBName: {
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "user1", sampleVersion),
						},
					},
				}
				require.NoError(t, env.db.Commit(users, 1))
			},
			expectedUsersBefore: []string{"user1"},
			tx: &types.UserAdministrationTx{
				UserId: "user0",
				TxId:   "tx1",
				UserWrites: []*types.UserWrite{
					{
						User: &types.User{
							Id:          "user2",
							Certificate: []byte("new-certificate~user2"),
						},
					},
				},
				UserDeletes: []*types.UserDelete{
					{
						UserId: "user1",
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

			tt.setup(env)

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

			dbsUpdates, provenanceData, err := env.committer.constructDBAndProvenanceEntries(block)
			require.NoError(t, err)
			require.NoError(t, env.committer.commitToDBs(dbsUpdates, provenanceData, block))

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
		name                    string
		setup                   func(db worldstate.DB)
		expectedDBsBefore       []string
		expectedIndexDBsBefore  []string
		expectedIndexBefore     map[string]*types.DBIndex
		dbAdminTx               *types.DBAdministrationTx
		expectedDBsAfter        []string
		expectedIndexAfter      map[string]*types.DBIndex
		expectedIndexDBsAfter   []string
		expectedNoIndexDBsAfter []string
		valInfo                 []*types.ValidationInfo
	}{
		{
			name: "create new DBs and delete existing DBs",
			setup: func(db worldstate.DB) {
				createDBs := map[string]*worldstate.DBUpdates{
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

				require.NoError(t, db.Commit(createDBs, 1))
			},
			expectedDBsBefore: []string{"db1", "db2"},
			expectedIndexBefore: map[string]*types.DBIndex{
				"db1": nil,
				"db2": nil,
			},
			dbAdminTx: &types.DBAdministrationTx{
				CreateDbs: []string{"db3", "db4"},
				DeleteDbs: []string{"db1", "db2"},
				DbsIndex: map[string]*types.DBIndex{
					"db3": {
						AttributeAndType: map[string]types.IndexAttributeType{
							"attr1": types.IndexAttributeType_BOOLEAN,
							"attr2": types.IndexAttributeType_NUMBER,
						},
					},
				},
			},
			expectedDBsAfter: []string{"db3", "db4"},
			expectedIndexAfter: map[string]*types.DBIndex{
				"db3": {
					AttributeAndType: map[string]types.IndexAttributeType{
						"attr1": types.IndexAttributeType_BOOLEAN,
						"attr2": types.IndexAttributeType_NUMBER,
					},
				},
				"db4": nil,
			},
			expectedIndexDBsAfter:   []string{"db3"},
			expectedNoIndexDBsAfter: []string{"db1", "db2", "db4"},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
		{
			name: "change index of an existing DB and create index for new DB",
			setup: func(db worldstate.DB) {
				indexDB1, err := json.Marshal(
					map[string]types.IndexAttributeType{
						"attr1-db1": types.IndexAttributeType_NUMBER,
						"attr2-db1": types.IndexAttributeType_BOOLEAN,
						"attr3-db1": types.IndexAttributeType_STRING,
					},
				)
				require.NoError(t, err)

				indexDB2, err := json.Marshal(
					map[string]types.IndexAttributeType{
						"attr1-db2": types.IndexAttributeType_NUMBER,
						"attr2-db2": types.IndexAttributeType_BOOLEAN,
						"attr3-db2": types.IndexAttributeType_STRING,
					},
				)
				require.NoError(t, err)

				createDBs := map[string]*worldstate.DBUpdates{
					worldstate.DatabasesDBName: {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   "db1",
								Value: indexDB1,
							},
							{
								Key:   "db2",
								Value: indexDB2,
							},
							{
								Key: "db3",
							},
							{
								Key: "db6",
							},
							{
								Key: stateindex.IndexDB("db1"),
							},
							{
								Key: stateindex.IndexDB("db2"),
							},
						},
					},
				}

				require.NoError(t, db.Commit(createDBs, 1))
			},
			expectedDBsBefore:      []string{"db1", "db2", "db3", "db6"},
			expectedIndexDBsBefore: []string{"db1", "db2"},
			expectedIndexBefore: map[string]*types.DBIndex{
				"db1": {
					AttributeAndType: map[string]types.IndexAttributeType{
						"attr1-db1": types.IndexAttributeType_NUMBER,
						"attr2-db1": types.IndexAttributeType_BOOLEAN,
						"attr3-db1": types.IndexAttributeType_STRING,
					},
				},
				"db2": {
					AttributeAndType: map[string]types.IndexAttributeType{
						"attr1-db2": types.IndexAttributeType_NUMBER,
						"attr2-db2": types.IndexAttributeType_BOOLEAN,
						"attr3-db2": types.IndexAttributeType_STRING,
					},
				},
				"db3": nil,
				"db6": nil,
			},
			dbAdminTx: &types.DBAdministrationTx{
				CreateDbs: []string{"db4", "db5"},
				DbsIndex: map[string]*types.DBIndex{
					"db1": {
						AttributeAndType: map[string]types.IndexAttributeType{
							"attr1": types.IndexAttributeType_BOOLEAN,
							"attr2": types.IndexAttributeType_NUMBER,
						},
					},
					"db2": nil,
					"db3": {
						AttributeAndType: map[string]types.IndexAttributeType{
							"attr1": types.IndexAttributeType_STRING,
						},
					},
					"db4": {
						AttributeAndType: map[string]types.IndexAttributeType{
							"attr1": types.IndexAttributeType_NUMBER,
						},
					},
					"db6": nil,
				},
			},
			expectedDBsAfter: []string{"db1", "db2", "db3", "db4", "db5", "db6"},
			expectedIndexAfter: map[string]*types.DBIndex{
				"db1": {
					AttributeAndType: map[string]types.IndexAttributeType{
						"attr1": types.IndexAttributeType_BOOLEAN,
						"attr2": types.IndexAttributeType_NUMBER,
					},
				},
				"db2": nil,
				"db3": {
					AttributeAndType: map[string]types.IndexAttributeType{
						"attr1": types.IndexAttributeType_STRING,
					},
				},
				"db4": {
					AttributeAndType: map[string]types.IndexAttributeType{
						"attr1": types.IndexAttributeType_NUMBER,
					},
				},
				"db5": nil,
				"db6": nil,
			},
			expectedIndexDBsAfter:   []string{"db1", "db3", "db4"},
			expectedNoIndexDBsAfter: []string{"db2", "db5", "db6"},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
		{
			name: "tx marked invalid",
			setup: func(db worldstate.DB) {
				createDBs := map[string]*worldstate.DBUpdates{
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

				require.NoError(t, db.Commit(createDBs, 1))
			},
			expectedDBsBefore: []string{"db1", "db2"},
			expectedIndexBefore: map[string]*types.DBIndex{
				"db1": nil,
				"db2": nil,
			},
			dbAdminTx: &types.DBAdministrationTx{
				DeleteDbs: []string{"db1", "db2"},
			},
			expectedDBsAfter: []string{"db1", "db2"},
			expectedIndexAfter: map[string]*types.DBIndex{
				"db1": nil,
				"db2": nil,
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

			tt.setup(env.db)

			for _, dbName := range tt.expectedDBsBefore {
				require.True(t, env.db.Exist(dbName))
				index, _, err := env.db.GetIndexDefinition(dbName)
				require.NoError(t, err)

				var expectedIndex []byte
				if tt.expectedIndexBefore[dbName] != nil {
					expectedIndex, err = json.Marshal(tt.expectedIndexBefore[dbName].GetAttributeAndType())
				}
				require.NoError(t, err)
				require.Equal(t, expectedIndex, index)
			}

			for _, dbName := range tt.expectedIndexDBsBefore {
				require.True(t, env.db.Exist(stateindex.IndexDB(dbName)))
			}

			block := &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
					ValidationInfo: tt.valInfo,
				},
				Payload: &types.Block_DbAdministrationTxEnvelope{
					DbAdministrationTxEnvelope: &types.DBAdministrationTxEnvelope{
						Payload: tt.dbAdminTx,
					},
				},
			}

			dbsUpdates, provenanceData, err := env.committer.constructDBAndProvenanceEntries(block)
			require.NoError(t, err)
			require.NoError(t, env.committer.commitToDBs(dbsUpdates, provenanceData, block))

			for _, dbName := range tt.expectedDBsAfter {
				require.True(t, env.db.Exist(dbName))
				index, _, err := env.db.GetIndexDefinition(dbName)
				require.NoError(t, err)

				var expectedIndex []byte
				if tt.expectedIndexAfter[dbName] != nil {
					expectedIndex, err = json.Marshal(tt.expectedIndexAfter[dbName].GetAttributeAndType())
				}
				require.NoError(t, err)
				require.Equal(t, expectedIndex, index)
			}

			for _, dbName := range tt.expectedIndexDBsAfter {
				require.True(t, env.db.Exist(stateindex.IndexDB(dbName)))
			}

			for _, dbName := range tt.expectedNoIndexDBsAfter {
				require.False(t, env.db.Exist(stateindex.IndexDB(dbName)))
			}
		})
	}
}

func TestStateDBCommitterForConfigBlock(t *testing.T) {
	t.Parallel()

	generateSampleConfigBlock := func(number uint64, adminsID []string, nodeIDs []uint64, valInfo []*types.ValidationInfo) *types.Block {
		var admins []*types.Admin
		for _, id := range adminsID {
			admins = append(admins, &types.Admin{
				Id:          id,
				Certificate: []byte("certificate~" + id),
			})
		}

		var nodes []*types.NodeConfig
		for _, id := range nodeIDs {
			nodes = append(nodes, constructNodeEntryForTest(id))
		}
		var peers []*types.PeerConfig
		for _, id := range nodeIDs {
			peers = append(peers, constructPeerEntryForTest(id))
		}

		clusterConfig := &types.ClusterConfig{
			Nodes:  nodes,
			Admins: admins,
			CertAuthConfig: &types.CAConfig{
				Roots: [][]byte{[]byte("root-ca")},
			},
			ConsensusConfig: &types.ConsensusConfig{
				Algorithm: "raft",
				Members:   peers,
				Observers: nil,
				RaftConfig: &types.RaftConfig{
					TickInterval:         "100ms",
					ElectionTicks:        10,
					HeartbeatTicks:       1,
					MaxInflightBlocks:    50,
					SnapshotIntervalSize: 1000000,
					MaxRaftId:            0,
				},
			},
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
						TxId:      fmt.Sprintf("tx-%d", number),
						NewConfig: clusterConfig,
					},
				},
			},
		}

		return configBlock
	}

	assertExpectedUsers := func(t *testing.T, q *identity.Querier, expectedUsers []*types.User) {
		for _, expectedUser := range expectedUsers {
			user, _, err := q.GetUser(expectedUser.Id)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedUser, user))
		}
	}

	assertExpectedNodes := func(t *testing.T, q *identity.Querier, expectedNodes []*types.NodeConfig) {
		for _, expectedNode := range expectedNodes {
			node, _, err := q.GetNode(expectedNode.Id)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedNode, node))
		}
	}

	assertExpectedConsensus := func(t *testing.T, db *leveldb.LevelDB, expectedConsensus *types.ConsensusConfig) {
		actualConfig, meta, err := db.GetConfig()
		require.NoError(t, err)
		require.NotNil(t, actualConfig)
		require.NotNil(t, meta)
		require.True(t, proto.Equal(expectedConsensus, actualConfig.GetConsensusConfig()), "actual %s", actualConfig.GetConsensusConfig())
	}

	tests := []struct {
		name                        string
		adminsInCommittedConfigTx   []string
		nodesInCommittedConfigTx    []uint64
		adminsInNewConfigTx         []string
		nodesInNewConfigTx          []uint64
		expectedClusterAdminsBefore []*types.User
		expectedNodesBefore         []*types.NodeConfig
		expectedConsensusBefore     *types.ConsensusConfig
		expectedClusterAdminsAfter  []*types.User
		expectedNodesAfter          []*types.NodeConfig
		expectedConsensusAfter      *types.ConsensusConfig
		valInfo                     []*types.ValidationInfo
	}{
		{
			name:                      "no change in the set of admins and nodes",
			adminsInCommittedConfigTx: []string{"admin1", "admin2"},
			adminsInNewConfigTx:       []string{"admin1", "admin2"},
			nodesInCommittedConfigTx:  []uint64{1, 2},
			nodesInNewConfigTx:        []uint64{1, 2},
			expectedClusterAdminsBefore: []*types.User{
				constructAdminEntryForTest("admin1"),
				constructAdminEntryForTest("admin2"),
			},
			expectedNodesBefore: []*types.NodeConfig{
				constructNodeEntryForTest(1),
				constructNodeEntryForTest(2),
			},
			expectedConsensusBefore: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					constructPeerEntryForTest(1),
					constructPeerEntryForTest(2),
				},
				Observers: nil,
				RaftConfig: &types.RaftConfig{
					TickInterval:         "100ms",
					ElectionTicks:        10,
					HeartbeatTicks:       1,
					MaxInflightBlocks:    50,
					SnapshotIntervalSize: 1000000,
					MaxRaftId:            2,
				},
			},
			expectedClusterAdminsAfter: []*types.User{
				constructAdminEntryForTest("admin1"),
				constructAdminEntryForTest("admin2"),
			},
			expectedNodesAfter: []*types.NodeConfig{
				constructNodeEntryForTest(1),
				constructNodeEntryForTest(2),
			},
			expectedConsensusAfter: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					constructPeerEntryForTest(1),
					constructPeerEntryForTest(2),
				},
				Observers: nil,
				RaftConfig: &types.RaftConfig{
					TickInterval:         "100ms",
					ElectionTicks:        10,
					HeartbeatTicks:       1,
					MaxInflightBlocks:    50,
					SnapshotIntervalSize: 1000000,
					MaxRaftId:            2,
				},
			},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
		{
			name:                      "add and delete admins and nodes",
			adminsInCommittedConfigTx: []string{"admin1", "admin2", "admin3"},
			adminsInNewConfigTx:       []string{"admin3", "admin4", "admin5"},
			nodesInCommittedConfigTx:  []uint64{1, 2, 3},
			nodesInNewConfigTx:        []uint64{3, 4, 5},
			expectedClusterAdminsBefore: []*types.User{
				constructAdminEntryForTest("admin1"),
				constructAdminEntryForTest("admin2"),
				constructAdminEntryForTest("admin3"),
			},
			expectedNodesBefore: []*types.NodeConfig{
				constructNodeEntryForTest(1),
				constructNodeEntryForTest(2),
				constructNodeEntryForTest(3),
			},
			expectedConsensusBefore: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					constructPeerEntryForTest(1),
					constructPeerEntryForTest(2),
					constructPeerEntryForTest(3),
				},
				Observers: nil,
				RaftConfig: &types.RaftConfig{
					TickInterval:         "100ms",
					ElectionTicks:        10,
					HeartbeatTicks:       1,
					MaxInflightBlocks:    50,
					SnapshotIntervalSize: 1000000,
					MaxRaftId:            3,
				},
			},
			expectedClusterAdminsAfter: []*types.User{
				constructAdminEntryForTest("admin3"),
				constructAdminEntryForTest("admin4"),
				constructAdminEntryForTest("admin5"),
			},
			expectedNodesAfter: []*types.NodeConfig{
				constructNodeEntryForTest(3),
				constructNodeEntryForTest(4),
				constructNodeEntryForTest(5),
			},
			expectedConsensusAfter: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					constructPeerEntryForTest(3),
					constructPeerEntryForTest(4),
					constructPeerEntryForTest(5),
				},
				Observers: nil,
				RaftConfig: &types.RaftConfig{
					TickInterval:         "100ms",
					ElectionTicks:        10,
					HeartbeatTicks:       1,
					MaxInflightBlocks:    50,
					SnapshotIntervalSize: 1000000,
					MaxRaftId:            5,
				},
			},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
		{
			name:                      "deleting nodes does not change MaxRaftID",
			adminsInCommittedConfigTx: []string{"admin1"},
			adminsInNewConfigTx:       []string{"admin1"},
			nodesInCommittedConfigTx:  []uint64{1, 2, 8},
			nodesInNewConfigTx:        []uint64{1, 2},
			expectedClusterAdminsBefore: []*types.User{
				constructAdminEntryForTest("admin1"),
			},
			expectedNodesBefore: []*types.NodeConfig{
				constructNodeEntryForTest(1),
				constructNodeEntryForTest(2),
				constructNodeEntryForTest(8),
			},
			expectedConsensusBefore: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					constructPeerEntryForTest(1),
					constructPeerEntryForTest(2),
					constructPeerEntryForTest(8),
				},
				Observers: nil,
				RaftConfig: &types.RaftConfig{
					TickInterval:         "100ms",
					ElectionTicks:        10,
					HeartbeatTicks:       1,
					MaxInflightBlocks:    50,
					SnapshotIntervalSize: 1000000,
					MaxRaftId:            8,
				},
			},
			expectedClusterAdminsAfter: []*types.User{
				constructAdminEntryForTest("admin1"),
			},
			expectedNodesAfter: []*types.NodeConfig{
				constructNodeEntryForTest(1),
				constructNodeEntryForTest(2),
			},
			expectedConsensusAfter: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					constructPeerEntryForTest(1),
					constructPeerEntryForTest(2),
				},
				Observers: nil,
				RaftConfig: &types.RaftConfig{
					TickInterval:         "100ms",
					ElectionTicks:        10,
					HeartbeatTicks:       1,
					MaxInflightBlocks:    50,
					SnapshotIntervalSize: 1000000,
					MaxRaftId:            8,
				},
			},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
		{
			name:                      "tx is marked invalid",
			adminsInCommittedConfigTx: []string{"admin1", "admin2"},
			adminsInNewConfigTx:       []string{"admin1", "admin2", "admin3"},
			nodesInCommittedConfigTx:  []uint64{1, 2, 3},
			nodesInNewConfigTx:        []uint64{1, 2, 3, 4},
			expectedClusterAdminsBefore: []*types.User{
				constructAdminEntryForTest("admin1"),
				constructAdminEntryForTest("admin2"),
			},
			expectedNodesBefore: []*types.NodeConfig{
				constructNodeEntryForTest(1),
				constructNodeEntryForTest(2),
				constructNodeEntryForTest(3),
			},
			expectedConsensusBefore: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					constructPeerEntryForTest(1),
					constructPeerEntryForTest(2),
					constructPeerEntryForTest(3),
				},
				Observers: nil,
				RaftConfig: &types.RaftConfig{
					TickInterval:         "100ms",
					ElectionTicks:        10,
					HeartbeatTicks:       1,
					MaxInflightBlocks:    50,
					SnapshotIntervalSize: 1000000,
					MaxRaftId:            3,
				},
			},
			expectedClusterAdminsAfter: []*types.User{
				constructAdminEntryForTest("admin1"),
				constructAdminEntryForTest("admin2"),
			},
			expectedNodesAfter: []*types.NodeConfig{
				constructNodeEntryForTest(1),
				constructNodeEntryForTest(2),
				constructNodeEntryForTest(3),
			},
			expectedConsensusAfter: &types.ConsensusConfig{
				Algorithm: "raft",
				Members: []*types.PeerConfig{
					constructPeerEntryForTest(1),
					constructPeerEntryForTest(2),
					constructPeerEntryForTest(3),
				},
				Observers: nil,
				RaftConfig: &types.RaftConfig{
					TickInterval:         "100ms",
					ElectionTicks:        10,
					HeartbeatTicks:       1,
					MaxInflightBlocks:    50,
					SnapshotIntervalSize: 1000000,
					MaxRaftId:            3,
				},
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
			// t.Parallel()

			env := newCommitterTestEnv(t)
			defer env.cleanup()

			var blockNumber uint64

			validationInfo := []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			}
			blockNumber = 1
			configBlock := generateSampleConfigBlock(blockNumber, tt.adminsInCommittedConfigTx, tt.nodesInCommittedConfigTx, validationInfo)
			err := env.committer.commitBlock(configBlock)
			require.NoError(t, err)
			assertExpectedUsers(t, env.identityQuerier, tt.expectedClusterAdminsBefore)
			assertExpectedNodes(t, env.identityQuerier, tt.expectedNodesBefore)
			assertExpectedConsensus(t, env.db, tt.expectedConsensusBefore)

			blockNumber++
			configBlock = generateSampleConfigBlock(blockNumber, tt.adminsInNewConfigTx, tt.nodesInNewConfigTx, tt.valInfo)
			err = env.committer.commitBlock(configBlock)
			require.NoError(t, err)
			assertExpectedUsers(t, env.identityQuerier, tt.expectedClusterAdminsAfter)
			assertExpectedNodes(t, env.identityQuerier, tt.expectedNodesAfter)
			assertExpectedConsensus(t, env.db, tt.expectedConsensusAfter)
		})
	}
}

func TestProvenanceStoreCommitterForDataBlockWithValidTxs(t *testing.T) {
	t.Parallel()

	setup := func(env *committerTestEnv) {
		createDB := map[string]*worldstate.DBUpdates{
			worldstate.DatabasesDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key: "db1",
					},
				},
			},
		}
		require.NoError(t, env.db.Commit(createDB, 1))

		data := map[string]*worldstate.DBUpdates{
			worldstate.DefaultDBName: {
				Writes: []*worldstate.KVWithMetadata{
					constructDataEntryForTest("key0", []byte("value0"), &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					}),
				},
			},
			"db1": {
				Writes: []*worldstate.KVWithMetadata{
					constructDataEntryForTest("key0", []byte("value0"), &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					}),
				},
			},
		}
		require.NoError(t, env.committer.db.Commit(data, 1))

		txsData := []*provenance.TxDataForProvenance{
			{
				IsValid: true,
				DBName:  worldstate.DefaultDBName,
				UserID:  "user1",
				TxID:    "tx0",
				Writes: []*types.KVWithMetadata{
					{
						Key:   "key0",
						Value: []byte("value0"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    0,
							},
						},
					},
				},
			},
			{
				IsValid: true,
				DBName:  "db1",
				UserID:  "user1",
				TxID:    "tx0",
				Writes: []*types.KVWithMetadata{
					{
						Key:   "key0",
						Value: []byte("value0"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    0,
							},
						},
					},
				},
			},
		}
		require.NoError(t, env.committer.provenanceStore.Commit(1, txsData))
	}

	tests := []struct {
		name         string
		txs          []*types.DataTxEnvelope
		valInfo      []*types.ValidationInfo
		query        func(s *provenance.Store, dbName string) ([]*types.ValueWithMetadata, error)
		expectedData []*types.ValueWithMetadata
	}{
		{
			name: "previous value link with the already committed value",
			txs: []*types.DataTxEnvelope{
				{
					Payload: &types.DataTx{
						MustSignUserIds: []string{"user1"},
						TxId:            "tx1",
						DbOperations: []*types.DBOperation{
							{
								DbName: worldstate.DefaultDBName,
								DataWrites: []*types.DataWrite{
									{
										Key:   "key0",
										Value: []byte("value1"),
									},
								},
							},
							{
								DbName: "db1",
								DataWrites: []*types.DataWrite{
									{
										Key:   "key0",
										Value: []byte("value1"),
									},
								},
							},
						},
					},
				},
			},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
			query: func(s *provenance.Store, dbName string) ([]*types.ValueWithMetadata, error) {
				return s.GetPreviousValues(
					dbName,
					"key0",
					&types.Version{
						BlockNum: 2,
						TxNum:    0,
					},
					-1,
				)
			},
			expectedData: []*types.ValueWithMetadata{
				{
					Value: []byte("value0"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name: "tx with read set",
			txs: []*types.DataTxEnvelope{
				{
					Payload: &types.DataTx{
						MustSignUserIds: []string{"user1"},
						TxId:            "tx1",
						DbOperations: []*types.DBOperation{
							{
								DbName: worldstate.DefaultDBName,
								DataReads: []*types.DataRead{
									{
										Key: "key0",
										Version: &types.Version{
											BlockNum: 1,
											TxNum:    0,
										},
									},
								},
								DataWrites: []*types.DataWrite{
									{
										Key:   "key0",
										Value: []byte("value1"),
									},
								},
							},
						},
					},
				},
			},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
			query: func(s *provenance.Store, _ string) ([]*types.ValueWithMetadata, error) {
				dbskvs, err := s.GetValuesReadByUser("user1")
				if err != nil {
					return nil, err
				}

				var values []*types.ValueWithMetadata
				for _, kvs := range dbskvs {
					for _, kv := range kvs.KVs {
						values = append(
							values,
							&types.ValueWithMetadata{
								Value:    kv.Value,
								Metadata: kv.Metadata,
							},
						)
					}
				}

				return values, nil
			},
			expectedData: []*types.ValueWithMetadata{
				{
					Value: []byte("value0"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name: "delete key0",
			txs: []*types.DataTxEnvelope{
				{
					Payload: &types.DataTx{
						MustSignUserIds: []string{"user1"},
						TxId:            "tx1",
						DbOperations: []*types.DBOperation{
							{
								DbName: worldstate.DefaultDBName,
								DataDeletes: []*types.DataDelete{
									{
										Key: "key0",
									},
								},
							},
							{
								DbName: "db1",
								DataDeletes: []*types.DataDelete{
									{
										Key: "key0",
									},
								},
							},
						},
					},
				},
			},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
			query: func(s *provenance.Store, dbName string) ([]*types.ValueWithMetadata, error) {
				return s.GetDeletedValues(
					dbName,
					"key0",
				)
			},
			expectedData: []*types.ValueWithMetadata{
				{
					Value: []byte("value0"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name: "invalid tx",
			txs: []*types.DataTxEnvelope{
				{
					Payload: &types.DataTx{
						MustSignUserIds: []string{"user1"},
						TxId:            "tx1",
						DbOperations: []*types.DBOperation{
							{
								DbName: worldstate.DefaultDBName,
								DataReads: []*types.DataRead{
									{
										Key: "key0",
										Version: &types.Version{
											BlockNum: 1,
											TxNum:    0,
										},
									},
								},
							},
						},
					},
				},
			},
			valInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_INVALID_INCORRECT_ENTRIES,
				},
			},
			query: func(s *provenance.Store, _ string) ([]*types.ValueWithMetadata, error) {
				dbskvs, err := s.GetValuesReadByUser("user1")
				if err != nil {
					return nil, err
				}

				var values []*types.ValueWithMetadata
				for _, kvs := range dbskvs {
					for _, kv := range kvs.KVs {
						values = append(
							values,
							&types.ValueWithMetadata{
								Value:    kv.Value,
								Metadata: kv.Metadata,
							},
						)
					}
				}

				return values, nil
			},
			expectedData: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCommitterTestEnv(t)
			defer env.cleanup()
			setup(env)

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

			_, provenanceData, err := env.committer.constructDBAndProvenanceEntries(block)
			require.NoError(t, err)
			require.NoError(t, env.committer.commitToProvenanceStore(2, provenanceData))

			for _, dbName := range []string{worldstate.DefaultDBName, "db1"} {
				actualData, err := tt.query(env.committer.provenanceStore, dbName)
				require.NoError(t, err)
				require.ElementsMatch(t, tt.expectedData, actualData)
			}
		})
	}
}

func TestProvenanceStoreCommitterForUserBlockWithValidTxs(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 1,
		TxNum:    0,
	}
	user1 := constructUserForTest(t, "user1", sampleVersion)
	rawUser1New := &types.User{
		Id:          "user1",
		Certificate: []byte("rawcert-user1-new"),
	}

	setup := func(env *committerTestEnv) {
		data := map[string]*worldstate.DBUpdates{
			worldstate.UsersDBName: {
				Writes: []*worldstate.KVWithMetadata{
					user1,
				},
			},
		}
		require.NoError(t, env.committer.db.Commit(data, 1))

		txsData := []*provenance.TxDataForProvenance{
			{
				IsValid: true,
				DBName:  worldstate.UsersDBName,
				UserID:  "user1",
				TxID:    "tx0",
				Writes: []*types.KVWithMetadata{
					{
						Key:      "user1",
						Value:    user1.Value,
						Metadata: user1.Metadata,
					},
				},
			},
		}
		require.NoError(t, env.committer.provenanceStore.Commit(1, txsData))
	}

	tests := []struct {
		name         string
		tx           *types.UserAdministrationTxEnvelope
		valInfo      *types.ValidationInfo
		query        func(s *provenance.Store) ([]*types.ValueWithMetadata, error)
		expectedData []*types.ValueWithMetadata
	}{
		{
			name: "previous link with the already committed value of user1",
			valInfo: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
			tx: &types.UserAdministrationTxEnvelope{
				Payload: &types.UserAdministrationTx{
					UserId: "user1",
					TxId:   "tx1",
					UserWrites: []*types.UserWrite{
						{
							User: rawUser1New,
						},
					},
				},
				Signature: nil,
			},
			query: func(s *provenance.Store) ([]*types.ValueWithMetadata, error) {
				return s.GetPreviousValues(
					worldstate.UsersDBName,
					"user1",
					&types.Version{
						BlockNum: 2,
						TxNum:    0,
					},
					-1,
				)
			},
			expectedData: []*types.ValueWithMetadata{
				{
					Value:    user1.Value,
					Metadata: user1.Metadata,
				},
			},
		},
		{
			name: "delete user1",
			tx: &types.UserAdministrationTxEnvelope{
				Payload: &types.UserAdministrationTx{
					UserId: "user1",
					TxId:   "tx1",
					UserDeletes: []*types.UserDelete{
						{
							UserId: "user1",
						},
					},
				},
				Signature: nil,
			},
			valInfo: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
			query: func(s *provenance.Store) ([]*types.ValueWithMetadata, error) {
				return s.GetDeletedValues(
					worldstate.UsersDBName,
					"user1",
				)
			},
			expectedData: []*types.ValueWithMetadata{
				{
					Value:    user1.Value,
					Metadata: user1.Metadata,
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
			setup(env)

			block := &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
					ValidationInfo: []*types.ValidationInfo{
						tt.valInfo,
					},
				},
				Payload: &types.Block_UserAdministrationTxEnvelope{
					UserAdministrationTxEnvelope: tt.tx,
				},
			}

			_, provenanceData, err := env.committer.constructDBAndProvenanceEntries(block)
			require.NoError(t, err)
			require.NoError(t, env.committer.commitToProvenanceStore(2, provenanceData))

			actualData, err := tt.query(env.committer.provenanceStore)
			require.NoError(t, err)
			require.Len(t, actualData, len(tt.expectedData))
			for i, expected := range tt.expectedData {
				require.True(t, proto.Equal(expected, actualData[i]))
			}
		})
	}
}

func TestProvenanceStoreCommitterForConfigBlockWithValidTxs(t *testing.T) {
	t.Parallel()

	clusterConfigWithOneNode := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			constructNodeEntryForTest(1),
		},
		Admins: []*types.Admin{
			{
				Id:          "admin1",
				Certificate: []byte("cert-admin1"),
			},
		},
		CertAuthConfig: &types.CAConfig{
			Roots: [][]byte{[]byte("root-ca")},
		},
		ConsensusConfig: &types.ConsensusConfig{
			Algorithm: "raft",
			Members: []*types.PeerConfig{
				constructPeerEntryForTest(1),
			},
			RaftConfig: &types.RaftConfig{
				TickInterval:         "100ms",
				ElectionTicks:        10,
				HeartbeatTicks:       1,
				MaxInflightBlocks:    50,
				SnapshotIntervalSize: 1000000,
				MaxRaftId:            1,
			},
		},
	}

	clusterConfigWithTwoNodes := proto.Clone(clusterConfigWithOneNode).(*types.ClusterConfig)
	clusterConfigWithTwoNodes.Nodes = append(clusterConfigWithTwoNodes.Nodes, constructNodeEntryForTest(2))
	clusterConfigWithTwoNodes.ConsensusConfig.Members = append(clusterConfigWithTwoNodes.ConsensusConfig.Members, constructPeerEntryForTest(2))
	clusterConfigWithTwoNodes.ConsensusConfig.RaftConfig.MaxRaftId = 2
	clusterConfigWithTwoNodes.Admins[0].Certificate = []byte("cert-new-admin1")
	clusterConfigWithTwoNodesSerialized, err := proto.Marshal(clusterConfigWithTwoNodes)
	require.NoError(t, err)

	admin1Serialized, err := proto.Marshal(
		&types.User{
			Id:          "admin1",
			Certificate: []byte("cert-admin1"),
			Privilege: &types.Privilege{
				Admin: true,
			},
		},
	)
	require.NoError(t, err)
	admin1NewSerialized, err := proto.Marshal(
		&types.User{
			Id:          "admin1",
			Certificate: []byte("cert-new-admin1"),
			Privilege: &types.Privilege{
				Admin: true,
			},
		},
	)
	require.NoError(t, err)

	node1Serialized, err := proto.Marshal(constructNodeEntryForTest(1))
	require.NoError(t, err)
	node2Serialized, err := proto.Marshal(constructNodeEntryForTest(2))
	require.NoError(t, err)

	setup := func(env *committerTestEnv) {
		sampleMetadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    0,
			},
		}

		configUpdates := map[string]*worldstate.DBUpdates{
			worldstate.ConfigDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      worldstate.ConfigKey,
						Value:    clusterConfigWithTwoNodesSerialized,
						Metadata: sampleMetadata,
					},
					{
						Key:      string(identity.NodeNamespace) + "bdb-node-1",
						Value:    node1Serialized,
						Metadata: sampleMetadata,
					},
					{
						Key:      string(identity.NodeNamespace) + "bdb-node-2",
						Value:    node2Serialized,
						Metadata: sampleMetadata,
					},
				},
			},
			worldstate.UsersDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      string(identity.UserNamespace) + "admin1",
						Value:    admin1NewSerialized,
						Metadata: sampleMetadata,
					},
				},
			},
		}
		require.NoError(t, env.committer.db.Commit(configUpdates, 1))

		provenanceData := []*provenance.TxDataForProvenance{
			{
				IsValid: true,
				DBName:  worldstate.ConfigDBName,
				UserID:  "user1",
				TxID:    "tx1",
				Writes: []*types.KVWithMetadata{
					{
						Key:   worldstate.ConfigKey,
						Value: clusterConfigWithTwoNodesSerialized,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    0,
							},
						},
					},
				},
				Deletes:            make(map[string]*types.Version),
				OldVersionOfWrites: make(map[string]*types.Version),
			},
			{
				IsValid: true,
				DBName:  worldstate.UsersDBName,
				UserID:  "user1",
				TxID:    "tx1",
				Writes: []*types.KVWithMetadata{
					{
						Key:   "admin1",
						Value: admin1NewSerialized,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    0,
							},
						},
					},
				},
				Deletes:            make(map[string]*types.Version),
				OldVersionOfWrites: make(map[string]*types.Version),
			},
			{
				IsValid: true,
				DBName:  worldstate.ConfigDBName,
				UserID:  "user1",
				TxID:    "tx1",
				Writes: []*types.KVWithMetadata{
					{
						Key:   "bdb-node-1",
						Value: node1Serialized,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    0,
							},
						},
					},
					{
						Key:   "bdb-node-2",
						Value: node2Serialized,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    0,
							},
						},
					},
				},
				Deletes:            make(map[string]*types.Version),
				OldVersionOfWrites: make(map[string]*types.Version),
			},
		}
		require.NoError(t, env.committer.provenanceStore.Commit(1, provenanceData))
	}

	tests := []struct {
		name                     string
		tx                       *types.ConfigTx
		valInfo                  *types.ValidationInfo
		queryAdmin               func(s *provenance.Store) ([]*types.ValueWithMetadata, error)
		expectedAdminQueryResult []*types.ValueWithMetadata
		queryNode                func(s *provenance.Store) ([]*types.ValueWithMetadata, error)
		expectedNodeQueryResult  []*types.ValueWithMetadata
	}{
		{
			name: "previous link with the already committed config tx; get deleted value of bdb-node-2 config",
			tx: &types.ConfigTx{
				UserId: "user1",
				TxId:   "tx1",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 1,
					TxNum:    0,
				},
				NewConfig: clusterConfigWithOneNode,
			},
			valInfo: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
			queryAdmin: func(s *provenance.Store) ([]*types.ValueWithMetadata, error) {
				return s.GetPreviousValues(
					worldstate.ConfigDBName,
					worldstate.ConfigKey,
					&types.Version{
						BlockNum: 2,
						TxNum:    0,
					},
					-1,
				)
			},
			expectedAdminQueryResult: []*types.ValueWithMetadata{
				{
					Value: clusterConfigWithTwoNodesSerialized,
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
			},
			queryNode: func(s *provenance.Store) ([]*types.ValueWithMetadata, error) {
				return s.GetDeletedValues(
					worldstate.ConfigDBName,
					"bdb-node-2",
				)
			},
			expectedNodeQueryResult: []*types.ValueWithMetadata{
				{
					Value: node2Serialized,
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
			},
		},
		{
			name: "next link with the admin present in committed config tx; get bdb-node-1 config",
			tx: &types.ConfigTx{
				UserId: "user1",
				TxId:   "tx1",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 1,
					TxNum:    0,
				},
				NewConfig: clusterConfigWithOneNode,
			},
			valInfo: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
			queryAdmin: func(s *provenance.Store) ([]*types.ValueWithMetadata, error) {
				return s.GetNextValues(
					worldstate.UsersDBName,
					"admin1",
					&types.Version{
						BlockNum: 1,
						TxNum:    0,
					},
					-1,
				)
			},
			expectedAdminQueryResult: []*types.ValueWithMetadata{
				{
					Value: admin1Serialized,
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    0,
						},
					},
				},
			},
			queryNode: func(s *provenance.Store) ([]*types.ValueWithMetadata, error) {
				return s.GetValues(
					worldstate.ConfigDBName,
					"bdb-node-1",
				)
			},
			expectedNodeQueryResult: []*types.ValueWithMetadata{
				{
					Value: node1Serialized,
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
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
			setup(env)

			block := &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
					ValidationInfo: []*types.ValidationInfo{
						tt.valInfo,
					},
				},
				Payload: &types.Block_ConfigTxEnvelope{
					ConfigTxEnvelope: &types.ConfigTxEnvelope{
						Payload: tt.tx,
					},
				},
			}

			_, provenanceData, err := env.committer.constructDBAndProvenanceEntries(block)
			require.NoError(t, err)
			require.NoError(t, env.committer.commitToProvenanceStore(2, provenanceData))

			actualData, err := tt.queryAdmin(env.committer.provenanceStore)
			require.NoError(t, err)
			require.Len(t, actualData, len(tt.expectedAdminQueryResult))
			for i, expected := range tt.expectedAdminQueryResult {
				require.True(t, proto.Equal(expected, actualData[i]))
			}

			actualData, err = tt.queryNode(env.committer.provenanceStore)
			require.NoError(t, err)
			require.Len(t, actualData, len(tt.expectedNodeQueryResult))
			for i, expected := range tt.expectedNodeQueryResult {
				require.True(t, proto.Equal(expected, actualData[i]))
			}

			txIDs, err := env.committer.provenanceStore.GetTxIDsSubmittedByUser("user1")
			require.NoError(t, err)
			require.ElementsMatch(t, []string{"tx1"}, txIDs)
		})
	}
}

func TestProvenanceStoreCommitterWithInvalidTxs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		block        *types.Block
		query        func(s *provenance.Store) (*provenance.TxIDLocation, error)
		expectedData *provenance.TxIDLocation
		expectedErr  string
	}{
		{
			name: "invalid data tx",
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
					ValidationInfo: []*types.ValidationInfo{
						{
							Flag: types.Flag_VALID,
						},
						{
							Flag: types.Flag_INVALID_INCORRECT_ENTRIES,
						},
					},
				},
				Payload: &types.Block_DataTxEnvelopes{
					DataTxEnvelopes: &types.DataTxEnvelopes{
						Envelopes: []*types.DataTxEnvelope{
							{
								Payload: &types.DataTx{
									MustSignUserIds: []string{"user1"},
									TxId:            "tx1",
									DbOperations: []*types.DBOperation{
										{
											DbName: worldstate.DefaultDBName,
										},
									},
								},
							},
							{
								Payload: &types.DataTx{
									MustSignUserIds: []string{"user1"},
									TxId:            "tx2",
									DbOperations: []*types.DBOperation{
										{
											DbName: worldstate.DefaultDBName,
										},
									},
								},
							},
						},
					},
				},
			},
			query: func(s *provenance.Store) (*provenance.TxIDLocation, error) {
				return s.GetTxIDLocation("tx2")
			},
			expectedData: &provenance.TxIDLocation{
				BlockNum: 2,
				TxIndex:  1,
			},
		},
		{
			name: "invalid user admin tx",
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
					ValidationInfo: []*types.ValidationInfo{
						{
							Flag: types.Flag_INVALID_INCORRECT_ENTRIES,
						},
					},
				},
				Payload: &types.Block_UserAdministrationTxEnvelope{
					UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
						Payload: &types.UserAdministrationTx{
							UserId: "user1",
							TxId:   "tx1",
						},
					},
				},
			},
			query: func(s *provenance.Store) (*provenance.TxIDLocation, error) {
				return s.GetTxIDLocation("tx1")
			},
			expectedData: &provenance.TxIDLocation{
				BlockNum: 2,
				TxIndex:  0,
			},
		},
		{
			name: "invalid config tx",
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
					ValidationInfo: []*types.ValidationInfo{
						{
							Flag: types.Flag_INVALID_INCORRECT_ENTRIES,
						},
					},
				},
				Payload: &types.Block_ConfigTxEnvelope{
					ConfigTxEnvelope: &types.ConfigTxEnvelope{
						Payload: &types.ConfigTx{
							UserId: "user1",
							TxId:   "tx1",
						},
					},
				},
			},
			query: func(s *provenance.Store) (*provenance.TxIDLocation, error) {
				return s.GetTxIDLocation("tx1")
			},
			expectedData: &provenance.TxIDLocation{
				BlockNum: 2,
				TxIndex:  0,
			},
		},
		{
			name: "non-existing txID",
			block: &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 2,
					},
					ValidationInfo: []*types.ValidationInfo{
						{
							Flag: types.Flag_INVALID_INCORRECT_ENTRIES,
						},
					},
				},
				Payload: &types.Block_DataTxEnvelopes{
					DataTxEnvelopes: &types.DataTxEnvelopes{
						Envelopes: []*types.DataTxEnvelope{
							{
								Payload: &types.DataTx{
									MustSignUserIds: []string{"user1"},
									TxId:            "tx1",
									DbOperations: []*types.DBOperation{
										{
											DbName: worldstate.DefaultDBName,
										},
									},
								},
							},
						},
					},
				},
			},
			query: func(s *provenance.Store) (*provenance.TxIDLocation, error) {
				return s.GetTxIDLocation("tx-not-there")
			},
			expectedErr: "TxID not found: tx-not-there",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newCommitterTestEnv(t)
			defer env.cleanup()

			_, provenanceData, err := env.committer.constructDBAndProvenanceEntries(tt.block)
			require.NoError(t, err)
			require.NoError(t, env.committer.commitToProvenanceStore(2, provenanceData))

			actualData, err := tt.query(env.committer.provenanceStore)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tt.expectedData, actualData)
			} else {
				require.Equal(t, tt.expectedErr, err.Error())
			}
		})
	}
}

func TestProvenanceStoreDisabledCommitter(t *testing.T) {
	t.Parallel()

	env := newCommitterTestEnv(t)
	defer env.cleanup()

	env.committer.provenanceStore = nil

	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number: 2,
			},
			ValidationInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
				{
					Flag: types.Flag_INVALID_INCORRECT_ENTRIES,
				},
			},
		},
		Payload: &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{
				Envelopes: []*types.DataTxEnvelope{
					{
						Payload: &types.DataTx{
							MustSignUserIds: []string{"user1"},
							TxId:            "tx1",
							DbOperations: []*types.DBOperation{
								{
									DbName: worldstate.DefaultDBName,
								},
							},
						},
					},
					{
						Payload: &types.DataTx{
							MustSignUserIds: []string{"user1"},
							TxId:            "tx2",
							DbOperations: []*types.DBOperation{
								{
									DbName: worldstate.DefaultDBName,
								},
							},
						},
					},
				},
			},
		},
	}
	_, provenanceData, err := env.committer.constructDBAndProvenanceEntries(block)
	require.NoError(t, err)
	err = env.committer.commitToProvenanceStore(2, provenanceData)
	require.NoError(t, err)
}

func TestConstructProvenanceEntriesForDataTx(t *testing.T) {
	tests := []struct {
		name                   string
		tx                     *types.DataTx
		version                *types.Version
		setup                  func(db worldstate.DB)
		expectedProvenanceData []*provenance.TxDataForProvenance
	}{
		{
			name: "tx with only reads",
			tx: &types.DataTx{
				MustSignUserIds: []string{"user1"},
				TxId:            "tx1",
				DbOperations: []*types.DBOperation{
					{
						DbName: worldstate.DefaultDBName,
						DataReads: []*types.DataRead{
							{
								Key: "key1",
								Version: &types.Version{
									BlockNum: 5,
									TxNum:    10,
								},
							},
							{
								Key: "key2",
								Version: &types.Version{
									BlockNum: 9,
									TxNum:    1,
								},
							},
						},
					},
					{
						DbName: "db1",
						DataReads: []*types.DataRead{
							{
								Key: "key3",
								Version: &types.Version{
									BlockNum: 5,
									TxNum:    10,
								},
							},
						},
					},
				},
			},
			version: &types.Version{
				BlockNum: 10,
				TxNum:    3,
			},
			setup: func(db worldstate.DB) {},
			expectedProvenanceData: []*provenance.TxDataForProvenance{
				{
					IsValid: true,
					DBName:  worldstate.DefaultDBName,
					UserID:  "user1",
					TxID:    "tx1",
					Reads: []*provenance.KeyWithVersion{
						{
							Key: "key1",
							Version: &types.Version{
								BlockNum: 5,
								TxNum:    10,
							},
						},
						{
							Key: "key2",
							Version: &types.Version{
								BlockNum: 9,
								TxNum:    1,
							},
						},
					},
					Deletes:            make(map[string]*types.Version),
					OldVersionOfWrites: make(map[string]*types.Version),
				},
				{
					IsValid: true,
					DBName:  "db1",
					UserID:  "user1",
					TxID:    "tx1",
					Reads: []*provenance.KeyWithVersion{
						{
							Key: "key3",
							Version: &types.Version{
								BlockNum: 5,
								TxNum:    10,
							},
						},
					},
					Deletes:            make(map[string]*types.Version),
					OldVersionOfWrites: make(map[string]*types.Version),
				},
			},
		},
		{
			name: "tx with writes and previous version",
			tx: &types.DataTx{
				MustSignUserIds: []string{"user2"},
				TxId:            "tx2",
				DbOperations: []*types.DBOperation{
					{
						DbName: worldstate.DefaultDBName,
						DataWrites: []*types.DataWrite{
							{
								Key:   "key1",
								Value: []byte("value1"),
							},
							{
								Key:   "key2",
								Value: []byte("value2"),
							},
							{
								Key:   "key3",
								Value: []byte("value3"),
							},
						},
					},
					{
						DbName: "db1",
						DataWrites: []*types.DataWrite{
							{
								Key:   "key1",
								Value: []byte("value1"),
							},
							{
								Key:   "key2",
								Value: []byte("value2"),
							},
							{
								Key:   "key3",
								Value: []byte("value3"),
							},
						},
					},
				},
			},
			version: &types.Version{
				BlockNum: 10,
				TxNum:    3,
			},
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

				writes := []*worldstate.KVWithMetadata{
					{
						Key:   "key1",
						Value: []byte("value1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 3,
								TxNum:    3,
							},
						},
					},
					{
						Key:   "key2",
						Value: []byte("value2"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 5,
								TxNum:    5,
							},
						},
					},
				}

				update := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
						Writes: writes,
					},
					"db1": {
						Writes: writes,
					},
				}
				require.NoError(t, db.Commit(update, 1))
			},
			expectedProvenanceData: []*provenance.TxDataForProvenance{
				{
					IsValid: true,
					DBName:  worldstate.DefaultDBName,
					UserID:  "user2",
					TxID:    "tx2",
					Writes: []*types.KVWithMetadata{
						{
							Key:   "key1",
							Value: []byte("value1"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 10,
									TxNum:    3,
								},
							},
						},
						{
							Key:   "key2",
							Value: []byte("value2"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 10,
									TxNum:    3,
								},
							},
						},
						{
							Key:   "key3",
							Value: []byte("value3"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 10,
									TxNum:    3,
								},
							},
						},
					},
					Deletes: make(map[string]*types.Version),
					OldVersionOfWrites: map[string]*types.Version{
						"key1": {
							BlockNum: 3,
							TxNum:    3,
						},
						"key2": {
							BlockNum: 5,
							TxNum:    5,
						},
					},
				},
				{
					IsValid: true,
					DBName:  "db1",
					UserID:  "user2",
					TxID:    "tx2",
					Writes: []*types.KVWithMetadata{
						{
							Key:   "key1",
							Value: []byte("value1"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 10,
									TxNum:    3,
								},
							},
						},
						{
							Key:   "key2",
							Value: []byte("value2"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 10,
									TxNum:    3,
								},
							},
						},
						{
							Key:   "key3",
							Value: []byte("value3"),
							Metadata: &types.Metadata{
								Version: &types.Version{
									BlockNum: 10,
									TxNum:    3,
								},
							},
						},
					},
					Deletes: make(map[string]*types.Version),
					OldVersionOfWrites: map[string]*types.Version{
						"key1": {
							BlockNum: 3,
							TxNum:    3,
						},
						"key2": {
							BlockNum: 5,
							TxNum:    5,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			env := newCommitterTestEnv(t)
			defer env.cleanup()
			tt.setup(env.db)

			provenanceData, err := constructProvenanceEntriesForDataTx(env.db, tt.tx, tt.version)
			require.NoError(t, err)
			require.Equal(t, tt.expectedProvenanceData, provenanceData)
		})
	}
}

func TestConstructProvenanceEntriesForConfigTx(t *testing.T) {
	clusterConfig := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				Id:          "bdb-node-1",
				Certificate: []byte("node-cert"),
				Address:     "127.0.0.1",
				Port:        0,
			},
		},
		Admins: []*types.Admin{
			{
				Id:          "admin1",
				Certificate: []byte("cert-admin1"),
			},
		},
		CertAuthConfig: &types.CAConfig{
			Roots: [][]byte{[]byte("root-ca")},
		},
	}
	clusterConfigSerialized, err := proto.Marshal(clusterConfig)
	require.NoError(t, err)

	tests := []struct {
		name                   string
		tx                     *types.ConfigTx
		version                *types.Version
		expectedProvenanceData *provenance.TxDataForProvenance
	}{
		{
			name: "first cluster config tx",
			tx: &types.ConfigTx{
				UserId:    "user1",
				TxId:      "tx1",
				NewConfig: clusterConfig,
			},
			version: &types.Version{
				BlockNum: 1,
				TxNum:    0,
			},
			expectedProvenanceData: &provenance.TxDataForProvenance{
				IsValid: true,
				DBName:  worldstate.ConfigDBName,
				UserID:  "user1",
				TxID:    "tx1",
				Writes: []*types.KVWithMetadata{
					{
						Key:   worldstate.ConfigKey,
						Value: clusterConfigSerialized,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    0,
							},
						},
					},
				},
				OldVersionOfWrites: make(map[string]*types.Version),
			},
		},
		{
			name: "updating existing cluster config",
			tx: &types.ConfigTx{
				UserId: "user1",
				TxId:   "tx1",
				ReadOldConfigVersion: &types.Version{
					BlockNum: 1,
					TxNum:    0,
				},
				NewConfig: clusterConfig,
			},
			version: &types.Version{
				BlockNum: 2,
				TxNum:    0,
			},
			expectedProvenanceData: &provenance.TxDataForProvenance{
				IsValid: true,
				DBName:  worldstate.ConfigDBName,
				UserID:  "user1",
				TxID:    "tx1",
				Writes: []*types.KVWithMetadata{
					{
						Key:   worldstate.ConfigKey,
						Value: clusterConfigSerialized,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    0,
							},
						},
					},
				},
				OldVersionOfWrites: map[string]*types.Version{
					worldstate.ConfigKey: {
						BlockNum: 1,
						TxNum:    0,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			provenanceData, err := constructProvenanceEntriesForConfigTx(tt.tx, tt.version, &dbEntriesForConfigTx{}, nil)
			require.NoError(t, err)
			require.Equal(t, tt.expectedProvenanceData, provenanceData[0])
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
		Id:          userID,
		Certificate: []byte("certificate~" + userID),
		Privilege: &types.Privilege{
			Admin: true,
		},
	}
}

func constructNodeEntryForTest(id uint64) *types.NodeConfig {
	return &types.NodeConfig{
		Id:          fmt.Sprintf("bdb-node-%d", id),
		Address:     "192.168.0.5",
		Port:        uint32(10000 + id),
		Certificate: []byte(fmt.Sprintf("certificate~%d", id)),
	}
}

func constructPeerEntryForTest(id uint64) *types.PeerConfig {
	return &types.PeerConfig{
		NodeId:   fmt.Sprintf("bdb-node-%d", id),
		RaftId:   id,
		PeerHost: "192.168.0.6",
		PeerPort: uint32(20000 + id),
	}
}

func constructUserForTest(t *testing.T, userID string, version *types.Version) *worldstate.KVWithMetadata {
	user := &types.User{
		Id: userID,
	}
	userSerialized, err := proto.Marshal(user)
	require.NoError(t, err)

	userEntry := &worldstate.KVWithMetadata{
		Key:   string(identity.UserNamespace) + userID,
		Value: userSerialized,
		Metadata: &types.Metadata{
			Version: version,
		},
	}

	return userEntry
}
