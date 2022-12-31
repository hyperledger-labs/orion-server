// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/stateindex"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

type worldstateQueryProcessorTestEnv struct {
	db      *leveldb.LevelDB
	q       *worldstateQueryProcessor
	cleanup func(t *testing.T)
}

func newWorldstateQueryProcessorTestEnv(t *testing.T) *worldstateQueryProcessorTestEnv {
	nodeID := "test-node-id1"

	path := t.TempDir()

	c := &logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: path,
			Logger:    logger,
		},
	)
	if err != nil {
		t.Fatalf("failed to create a new leveldb instance, %v", err)
	}

	cleanup := func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close leveldb: %v", err)
		}
	}

	qProcConfig := &worldstateQueryProcessorConfig{
		nodeID:              nodeID,
		db:                  db,
		queryProcessingConf: &config.QueryProcessingConf{},
		blockStore:          nil,
		identityQuerier:     identity.NewQuerier(db),
		logger:              logger,
	}

	qProc := newWorldstateQueryProcessor(qProcConfig)
	return &worldstateQueryProcessorTestEnv{
		db:      db,
		q:       qProc,
		cleanup: cleanup,
	}
}

func TestGetDBStatus(t *testing.T) {
	t.Run("getDBStatus-Returns-Status", func(t *testing.T) {

		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		createDB := map[string]*worldstate.DBUpdates{
			worldstate.DatabasesDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key: "test-db",
					},
				},
			},
		}
		require.NoError(t, env.db.Commit(createDB, 1))

		testCases := []struct {
			dbName  string
			isExist bool
		}{
			{
				dbName:  "test-db",
				isExist: true,
			},
			{
				dbName:  "random",
				isExist: false,
			},
		}

		for _, testCase := range testCases {
			status, err := env.q.getDBStatus(testCase.dbName)
			require.NoError(t, err)
			require.NotNil(t, status)
			require.Equal(t, testCase.isExist, status.Exist)
		}
	})
}

func TestGetDBIndex(t *testing.T) {
	index := &types.DBIndex{
		AttributeAndType: map[string]types.IndexAttributeType{"field1": types.IndexAttributeType_STRING, "field2": types.IndexAttributeType_BOOLEAN},
	}
	indexValue, err := json.Marshal(index.GetAttributeAndType())
	require.NoError(t, err)

	setup := func(db worldstate.DB, userID, dbName string, index []byte) {
		user := &types.User{
			Id: userID,
			Privilege: &types.Privilege{
				DbPermission: map[string]types.Privilege_Access{
					dbName: types.Privilege_ReadWrite,
				},
			},
		}

		u, err := proto.Marshal(user)
		require.NoError(t, err)

		createUser := map[string]*worldstate.DBUpdates{
			worldstate.UsersDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + userID,
						Value: u,
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
		require.NoError(t, db.Commit(createUser, 2))

		createDB := map[string]*worldstate.DBUpdates{
			worldstate.DatabasesDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   dbName,
						Value: index,
					},
				},
			},
		}
		require.NoError(t, db.Commit(createDB, 2))
	}

	t.Run("getDBIndex-returns-index", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		setup(env.q.db, "user1", "test-db1", indexValue)
		setup(env.q.db, "user2", "test-db2", nil)

		testCases := []struct {
			dbName        string
			user          string
			expectedIndex string
		}{
			{
				dbName:        "test-db1",
				user:          "user1",
				expectedIndex: `{"field1":1,"field2":2}`,
			},
			{
				dbName:        "test-db2",
				user:          "user2",
				expectedIndex: "",
			},
		}

		for _, testCase := range testCases {
			index, err := env.q.getDBIndex(testCase.dbName, testCase.user)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedIndex, index.GetIndex())
		}
	})

	t.Run("getDBIndex-returns-error", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		setup(env.q.db, "user1", "test-db1", indexValue)
		setup(env.q.db, "user2", "test-db2", nil)

		testCases := []struct {
			dbName      string
			user        string
			expectedErr string
		}{
			{
				dbName:      worldstate.DatabasesDBName,
				user:        "user1",
				expectedErr: "no index for the system database [_dbs]",
			},
			{
				dbName:      "test-db1",
				user:        "user2",
				expectedErr: "the user [user2] has no permission to read from database [test-db1]",
			},
		}

		for _, testCase := range testCases {
			index, err := env.q.getDBIndex(testCase.dbName, testCase.user)
			require.Nil(t, index)
			require.EqualError(t, err, testCase.expectedErr)
		}
	})
}

func TestGetDataRange(t *testing.T) {
	aliceKVs := []*worldstate.KVWithMetadata{
		{
			Key:   "key1",
			Value: []byte("value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 3,
					TxNum:    1,
				},
				AccessControl: &types.AccessControl{
					ReadUsers: map[string]bool{
						"alice": true,
					},
				},
			},
		},
		{
			Key:   "key2",
			Value: []byte("value2"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 5,
					TxNum:    1,
				},
				AccessControl: &types.AccessControl{
					ReadUsers: map[string]bool{
						"alice": true,
					},
				},
			},
		},
		{
			Key:   "key3",
			Value: []byte("value3"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 6,
					TxNum:    1,
				},
				AccessControl: &types.AccessControl{
					ReadUsers: map[string]bool{
						"alice": true,
					},
				},
			},
		},
		{
			Key:   "key5",
			Value: []byte("value5"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 8,
					TxNum:    54,
				},
				AccessControl: &types.AccessControl{
					ReadUsers: map[string]bool{
						"alice": true,
					},
				},
			},
		},
		{
			Key:   "key8",
			Value: []byte("value8"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 8,
					TxNum:    55,
				},
				AccessControl: &types.AccessControl{
					ReadUsers: map[string]bool{
						"alice": true,
					},
				},
			},
		},
	}

	bobKVs := []*worldstate.KVWithMetadata{
		{
			Key:   "key4",
			Value: []byte("value4"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 7,
					TxNum:    1,
				},
				AccessControl: &types.AccessControl{
					ReadUsers: map[string]bool{
						"bob": true,
					},
				},
			},
		},
		{
			Key:   "key6",
			Value: []byte("value6"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 10,
					TxNum:    11,
				},
				AccessControl: &types.AccessControl{
					ReadUsers: map[string]bool{
						"bob": true,
					},
				},
			},
		},
		{
			Key:   "key7",
			Value: []byte("value7"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 13,
					TxNum:    5,
				},
				AccessControl: &types.AccessControl{
					ReadUsers: map[string]bool{
						"bob": true,
					},
				},
			},
		},
	}

	setup := func(db worldstate.DB, userID, dbName string) {
		user := &types.User{
			Id: userID,
			Privilege: &types.Privilege{
				DbPermission: map[string]types.Privilege_Access{
					dbName: types.Privilege_ReadWrite,
				},
			},
		}

		u, err := proto.Marshal(user)
		require.NoError(t, err)

		createUser := map[string]*worldstate.DBUpdates{
			worldstate.UsersDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + userID,
						Value: u,
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
		require.NoError(t, db.Commit(createUser, 2))

		createDB := map[string]*worldstate.DBUpdates{
			worldstate.DatabasesDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key: dbName,
					},
				},
			},
		}
		require.NoError(t, db.Commit(createDB, 2))

		kvs := aliceKVs
		kvs = append(kvs, bobKVs...)
		dbsUpdates := map[string]*worldstate.DBUpdates{
			"test-db": {
				Writes: kvs,
			},
		}
		require.NoError(t, db.Commit(dbsUpdates, 10))
	}

	t.Run("getDataRange", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)
		env.q.queryProcessingConf.ResponseSizeLimitInBytes = 50

		setup(env.db, "alice", "test-db")
		setup(env.db, "bob", "test-db")

		testCases := []struct {
			startKey       string
			endKey         string
			limit          uint64
			user           string
			expectedResult []*types.KVWithMetadata
		}{
			{
				startKey: "key1",
				endKey:   "key9",
				limit:    100,
				user:     "alice",
				expectedResult: []*types.KVWithMetadata{
					{
						Key:   "key1",
						Value: []byte("value1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 3,
								TxNum:    1,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
					{
						Key:   "key2",
						Value: []byte("value2"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 5,
								TxNum:    1,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
					{
						Key:   "key3",
						Value: []byte("value3"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 6,
								TxNum:    1,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
					{
						Key:   "key5",
						Value: []byte("value5"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 8,
								TxNum:    54,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
					{
						Key:   "key8",
						Value: []byte("value8"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 8,
								TxNum:    55,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
				},
			},
			{
				startKey: "key1",
				endKey:   "key4",
				limit:    1,
				user:     "alice",
				expectedResult: []*types.KVWithMetadata{
					{
						Key:   "key1",
						Value: []byte("value1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 3,
								TxNum:    1,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
					{
						Key:   "key2",
						Value: []byte("value2"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 5,
								TxNum:    1,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
					{
						Key:   "key3",
						Value: []byte("value3"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 6,
								TxNum:    1,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
				},
			},
			{
				startKey: "",
				endKey:   "key4",
				limit:    0,
				user:     "alice",
				expectedResult: []*types.KVWithMetadata{
					{
						Key:   "key1",
						Value: []byte("value1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 3,
								TxNum:    1,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
					{
						Key:   "key2",
						Value: []byte("value2"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 5,
								TxNum:    1,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
					{
						Key:   "key3",
						Value: []byte("value3"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 6,
								TxNum:    1,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
				},
			},
			{
				startKey: "",
				endKey:   "",
				user:     "bob",
				expectedResult: []*types.KVWithMetadata{
					{
						Key:   "key4",
						Value: []byte("value4"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 7,
								TxNum:    1,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"bob": true,
								},
							},
						},
					},
					{
						Key:   "key6",
						Value: []byte("value6"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 10,
								TxNum:    11,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"bob": true,
								},
							},
						},
					},
					{
						Key:   "key7",
						Value: []byte("value7"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 13,
								TxNum:    5,
							},
							AccessControl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"bob": true,
								},
							},
						},
					},
				},
			},
			{
				startKey:       "key9",
				endKey:         "",
				limit:          1,
				user:           "bob",
				expectedResult: nil,
			},
			{
				startKey:       "2",
				endKey:         "1",
				limit:          0,
				user:           "alice",
				expectedResult: nil,
			},
		}

		for _, testCase := range testCases {
			var actualKVs []*types.KVWithMetadata
			for {
				payload, err := env.q.getDataRange("test-db", testCase.user, testCase.startKey, testCase.endKey, testCase.limit)
				require.NoError(t, err)
				require.NotNil(t, payload)
				actualKVs = append(actualKVs, payload.GetKVs()...)
				if !payload.PendingResult {
					break
				}
				testCase.startKey = payload.NextStartKey
			}
			require.Equal(t, testCase.expectedResult, actualKVs)
		}
	})

	t.Run("getDataRange returns user does not have permission to read from the database", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)
		env.q.queryProcessingConf.ResponseSizeLimitInBytes = 30

		setup(env.db, "alice", "test-db")
		setup(env.db, "bob", "another-test-db")

		actualVal, err := env.q.getDataRange("another-test-db", "alice", "", "", 0)
		require.EqualError(t, err, "the user [alice] has no permission to read from database [another-test-db]")
		require.Nil(t, actualVal)
	})

	t.Run("getDataRange returns user server restriction error", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)
		env.q.queryProcessingConf.ResponseSizeLimitInBytes = 10

		setup(env.db, "alice", "test-db")

		actualVal, err := env.q.getDataRange("test-db", "alice", "key1", "key2", 10)
		require.EqualError(t, err, "response size limit for queries is configured as 10 bytes but a single record size itself is 33 bytes. Increase the query response size limit at the server")
		require.Nil(t, actualVal)
	})

	t.Run("getData returns permission error due to directly accessing system database", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)
		env.q.queryProcessingConf.ResponseSizeLimitInBytes = 50

		setup(env.db, "testUser", "test-db")

		tests := []struct {
			name     string
			dbName   string
			user     string
			startKey string
			endKey   string
		}{
			{
				name:     "accessing config db",
				dbName:   worldstate.ConfigDBName,
				user:     "alice",
				startKey: "c",
				endKey:   "",
			},
			{
				name:     "accessing users db",
				dbName:   worldstate.UsersDBName,
				user:     "alice",
				startKey: "user1",
				endKey:   "user10",
			},
			{
				name:     "accessing databases db",
				dbName:   worldstate.DatabasesDBName,
				user:     "alice",
				startKey: "db1",
				endKey:   "db10",
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				actualVal, err := env.q.getDataRange(tt.dbName, tt.user, tt.startKey, tt.endKey, 0)
				require.EqualError(t, err, "no user can directly read from a system database ["+tt.dbName+"]. "+
					"To read from a system database, use /config, /user, /db rest endpoints instead of /data")
				require.Nil(t, actualVal)
			})
		}
	})
}

func TestGetData(t *testing.T) {
	setup := func(db worldstate.DB, userID, dbName string) {
		user := &types.User{
			Id: userID,
			Privilege: &types.Privilege{
				DbPermission: map[string]types.Privilege_Access{
					dbName: types.Privilege_ReadWrite,
				},
			},
		}

		u, err := proto.Marshal(user)
		require.NoError(t, err)

		createUser := map[string]*worldstate.DBUpdates{
			worldstate.UsersDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + userID,
						Value: u,
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
		require.NoError(t, db.Commit(createUser, 2))

		createDB := map[string]*worldstate.DBUpdates{
			worldstate.DatabasesDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key: dbName,
					},
				},
			},
		}
		require.NoError(t, db.Commit(createDB, 2))
	}

	t.Run("getData returns data", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		setup(env.db, "testUser", "test-db")

		val := []byte("value1")
		metadata1 := &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			AccessControl: &types.AccessControl{
				ReadUsers: map[string]bool{
					"testUser": true,
				},
			},
		}

		metadata2 := &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
		}

		dbsUpdates := map[string]*worldstate.DBUpdates{
			"test-db": {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      "key1",
						Value:    val,
						Metadata: metadata1,
					},
					{
						Key:      "key2",
						Value:    val,
						Metadata: metadata2,
					},
				},
			},
		}
		require.NoError(t, env.db.Commit(dbsUpdates, 2))

		testCases := []struct {
			key              string
			expectedValue    []byte
			expectedMetadata *types.Metadata
		}{
			{
				key:              "key1",
				expectedValue:    val,
				expectedMetadata: metadata1,
			},
			{
				key:              "key2",
				expectedValue:    val,
				expectedMetadata: metadata2,
			},
			{
				key:              "not-present",
				expectedValue:    nil,
				expectedMetadata: nil,
			},
		}

		for _, testCase := range testCases {
			payload, err := env.q.getData("test-db", "testUser", testCase.key)
			require.NoError(t, err)
			require.NotNil(t, payload)
			require.Equal(t, testCase.expectedValue, payload.Value)
			require.True(t, proto.Equal(testCase.expectedMetadata, payload.Metadata))
		}
	})

	t.Run("getData returns permission error due to ACL", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		setup(env.db, "testUser", "test-db")

		val := []byte("value1")
		metadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			AccessControl: &types.AccessControl{
				ReadUsers: map[string]bool{
					"user5": true,
				},
				ReadWriteUsers: map[string]bool{
					"user6": true,
				},
			},
		}

		dbsUpdates := map[string]*worldstate.DBUpdates{
			"test-db": {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      "key1",
						Value:    val,
						Metadata: metadata,
					},
				},
			},
		}
		require.NoError(t, env.db.Commit(dbsUpdates, 2))

		actualVal, err := env.q.getData("test-db", "testUser", "key1")
		require.EqualError(t, err, "the user [testUser] has no permission to read key [key1] from database [test-db]")
		require.Nil(t, actualVal)
	})

	t.Run("getData returns permission error due to directly accessing system database", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		setup(env.db, "testUser", "test-db")

		tests := []struct {
			name   string
			dbName string
			user   string
			key    string
		}{
			{
				name:   "accessing config db",
				dbName: worldstate.ConfigDBName,
				user:   "testUser",
				key:    worldstate.ConfigDBName,
			},
			{
				name:   "accessing users db",
				dbName: worldstate.UsersDBName,
				user:   "testUser",
				key:    "testUser",
			},
			{
				name:   "accessing databases db",
				dbName: worldstate.DatabasesDBName,
				user:   "testUser",
				key:    "bdb",
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				actualVal, err := env.q.getData(tt.dbName, tt.user, tt.key)
				require.EqualError(t, err, "no user can directly read from a system database ["+tt.dbName+"]. "+
					"To read from a system database, use /config, /user, /db rest endpoints instead of /data")
				require.Nil(t, actualVal)
			})
		}
	})
}

func TestExecuteJSONQuery(t *testing.T) {
	m := &types.Metadata{
		Version: &types.Version{
			BlockNum: 3,
			TxNum:    0,
		},
		AccessControl: &types.AccessControl{
			ReadUsers: map[string]bool{
				"user1": true,
			},
		},
	}
	db1 := "db1"

	setup := func(db worldstate.DB, userID string) {
		user := &types.User{
			Id: userID,
			Privilege: &types.Privilege{
				DbPermission: map[string]types.Privilege_Access{
					db1: types.Privilege_ReadWrite,
				},
			},
		}

		u, err := proto.Marshal(user)
		require.NoError(t, err)

		createUser := map[string]*worldstate.DBUpdates{
			worldstate.UsersDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + userID,
						Value: u,
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
		require.NoError(t, db.Commit(createUser, 2))

		indexDef := map[string]types.IndexAttributeType{
			"attr1": types.IndexAttributeType_STRING,
			"attr2": types.IndexAttributeType_BOOLEAN,
			"attr3": types.IndexAttributeType_STRING,
		}
		marshaledIndexDef, err := json.Marshal(indexDef)
		require.NoError(t, err)

		indexDBName := stateindex.IndexDB(db1)

		createDB := map[string]*worldstate.DBUpdates{
			worldstate.DatabasesDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   db1,
						Value: marshaledIndexDef,
					},
					{
						Key: "db2",
					},
					{
						Key: indexDBName,
					},
				},
			},
		}
		require.NoError(t, db.Commit(createDB, 2))

		dbsUpdates := map[string]*worldstate.DBUpdates{
			db1: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      "key1",
						Value:    []byte(`{"attr1":"a","attr2":false,"attr3":"z","attr4":100}`),
						Metadata: m,
					},
					{
						Key:      "key2",
						Value:    []byte(`{"attr1":"b","attr2":false,"attr3":"y","attr4":101}`),
						Metadata: m,
					},
					{
						Key:      "key3",
						Value:    []byte(`{"attr1":"c","attr2":false,"attr3":"x","attr4":102}`),
						Metadata: m,
					},
					{
						Key:      "key4",
						Value:    []byte(`{"attr1":"f","attr2":true,"attr3":"m","attr4":-100}`),
						Metadata: m,
					},
					{
						Key:      "key5",
						Value:    []byte(`{"attr1":"g","attr2":true,"attr3":"n","attr4":-101}`),
						Metadata: m,
					},
					{
						Key:      "key6",
						Value:    []byte(`{"attr1":"h","attr2":true,"attr3":"o","attr4":-102}`),
						Metadata: m,
					},
				},
			},
		}

		indexUpdates, err := stateindex.ConstructIndexEntries(dbsUpdates, db)
		require.NoError(t, err)
		for indexDB, updates := range indexUpdates {
			dbsUpdates[indexDB] = updates
		}
		require.NoError(t, db.Commit(dbsUpdates, 3))
	}

	tests := []struct {
		name                string
		dbName              string
		userID              string
		query               []byte
		useCancelledContext bool
		expectedKVs         map[string]*types.KVWithMetadata
		expectedErr         string
	}{
		{
			name:   "fetch records based on boolean matching",
			dbName: "db1",
			userID: "user1",
			query: []byte(
				`{
					"selector": {
						"attr2": {
							"$eq": true
						}
					}
				}`,
			),
			useCancelledContext: false,
			expectedKVs: map[string]*types.KVWithMetadata{
				"key4": {
					Key:      "key4",
					Value:    []byte(`{"attr1":"f","attr2":true,"attr3":"m","attr4":-100}`),
					Metadata: m,
				},
				"key5": {
					Key:      "key5",
					Value:    []byte(`{"attr1":"g","attr2":true,"attr3":"n","attr4":-101}`),
					Metadata: m,
				},
				"key6": {
					Key:      "key6",
					Value:    []byte(`{"attr1":"h","attr2":true,"attr3":"o","attr4":-102}`),
					Metadata: m,
				},
			},
		},
		{
			name:   "fetch records based on string",
			dbName: "db1",
			userID: "user1",
			query: []byte(
				`{
					"selector": {
						"attr1": {
							"$gt": "",
							"$lte": "d"
						}
					}
				}`,
			),
			useCancelledContext: false,
			expectedKVs: map[string]*types.KVWithMetadata{
				"key1": {
					Key:      "key1",
					Value:    []byte(`{"attr1":"a","attr2":false,"attr3":"z","attr4":100}`),
					Metadata: m,
				},
				"key2": {
					Key:      "key2",
					Value:    []byte(`{"attr1":"b","attr2":false,"attr3":"y","attr4":101}`),
					Metadata: m,
				},
				"key3": {
					Key:      "key3",
					Value:    []byte(`{"attr1":"c","attr2":false,"attr3":"x","attr4":102}`),
					Metadata: m,
				},
			},
		},
		{
			name:   "empty result due to cancelled context",
			dbName: "db1",
			userID: "user1",
			query: []byte(
				`{
					"attr1": {
						"$gt": "",
						"$lte": "d"
					}
				}`,
			),
			useCancelledContext: true,
			expectedKVs:         nil,
		},
		{
			name:   "empty result due to acl",
			dbName: "db1",
			userID: "user2",
			query: []byte(
				`{
					"selector": {
						"attr2": {
							"$eq": true
						}
					}
				}`,
			),
			useCancelledContext: false,
		},
		{
			name:   "user cannot read from system database",
			dbName: worldstate.ConfigDBName,
			userID: "user1",
			query: []byte(
				`{
					"selector": {
						"attr1": {
							"$gt": "",
							"$lte": "d"
						}
					}
				}`,
			),
			useCancelledContext: false,
			expectedErr:         "no user can directly read from a system database [" + worldstate.ConfigDBName + "]",
		},
		{
			name:   "user does not have read permission",
			dbName: "db2",
			userID: "user1",
			query: []byte(
				`{
					"selector": {
						"attr1": {
							"$gt": "",
							"$lte": "d"
						}
					}
				}`,
			),
			useCancelledContext: false,
			expectedErr:         "the user [user1] has no permission to read from database [db2]",
		},
		{
			name:   "query syntax error",
			dbName: "db1",
			userID: "user1",
			query: []byte(
				`{
					"selector": {
						"attr1": {
							"$gt": "",
							"$lte": "d",
						}
					}
				}`,
			),
			useCancelledContext: false,
			expectedErr:         "error decoding the query",
		},
		{
			name:   "query syntax error",
			dbName: "db1",
			userID: "user1",
			query: []byte(
				`{
					"attr1": {
						"$lte": "d"
					}
				}`,
			),
			expectedErr: "selector field is missing in the query",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			env := newWorldstateQueryProcessorTestEnv(t)
			defer env.cleanup(t)

			setup(env.db, tt.userID)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.useCancelledContext {
				cancel()
			}
			result, err := env.q.executeJSONQuery(ctx, tt.dbName, tt.userID, tt.query)
			if tt.expectedErr == "" {
				require.NoError(t, err)
				if tt.useCancelledContext {
					require.Nil(t, result)
					return
				}

				require.Equal(t, len(tt.expectedKVs), len(result.KVs))
				for _, kv := range result.KVs {
					require.True(t, proto.Equal(kv, tt.expectedKVs[kv.Key]))
				}
			} else {
				require.Nil(t, result)
				require.NotNil(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}

func TestGetUser(t *testing.T) {
	querierUser := &types.User{
		Id: "querierUser",
	}
	querierUserSerialized, err := proto.Marshal(querierUser)
	require.NoError(t, err)

	querierAdminUser := &types.User{
		Id: "querierUser",
		Privilege: &types.Privilege{
			Admin: true,
		},
	}
	querierAdminUserSerialized, err := proto.Marshal(querierAdminUser)
	require.NoError(t, err)

	targetUser := &types.User{
		Id: "targetUser",
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				"db1": types.Privilege_ReadWrite,
			},
		},
	}
	targetUserSerialized, err := proto.Marshal(targetUser)
	require.NoError(t, err)

	targetUserMetadataReadPerm := &types.Metadata{
		Version: &types.Version{
			BlockNum: 1,
			TxNum:    1,
		},
		AccessControl: &types.AccessControl{
			ReadUsers: map[string]bool{
				"querierUser": true,
			},
		},
	}

	targetUserMetadataNoReadPerm := &types.Metadata{
		Version: &types.Version{
			BlockNum: 1,
			TxNum:    1,
		},
		AccessControl: &types.AccessControl{
			ReadUsers: map[string]bool{
				"user1": true,
			},
		},
	}

	targetUserMetadataNoACL := &types.Metadata{
		Version: &types.Version{
			BlockNum: 1,
			TxNum:    1,
		},
	}

	t.Run("query existing user", func(t *testing.T) {
		tests := []struct {
			name            string
			setup           func(db worldstate.DB)
			querierUserID   string
			targetUserID    string
			expectedRespose *types.GetUserResponse
		}{
			{
				name: "querierUser has read permission on targetUser",
				setup: func(db worldstate.DB) {
					addUser := map[string]*worldstate.DBUpdates{
						worldstate.UsersDBName: {
							Writes: []*worldstate.KVWithMetadata{
								{
									Key:   string(identity.UserNamespace) + "querierUser",
									Value: querierUserSerialized,
								},
								{
									Key:      string(identity.UserNamespace) + "targetUser",
									Value:    targetUserSerialized,
									Metadata: targetUserMetadataReadPerm,
								},
							},
						},
					}

					require.NoError(t, db.Commit(addUser, 1))
				},
				querierUserID: "querierUser",
				targetUserID:  "targetUser",
				expectedRespose: &types.GetUserResponse{
					User:     targetUser,
					Metadata: targetUserMetadataReadPerm,
				},
			},
			{
				name: "admin always has read permission on targetUser",
				setup: func(db worldstate.DB) {
					addUser := map[string]*worldstate.DBUpdates{
						worldstate.UsersDBName: {
							Writes: []*worldstate.KVWithMetadata{
								{
									Key:   string(identity.UserNamespace) + "querierUser",
									Value: querierAdminUserSerialized,
								},
								{
									Key:      string(identity.UserNamespace) + "targetUser",
									Value:    targetUserSerialized,
									Metadata: targetUserMetadataReadPerm,
								},
							},
						},
					}

					require.NoError(t, db.Commit(addUser, 1))
				},
				querierUserID: "querierUser",
				targetUserID:  "targetUser",
				expectedRespose: &types.GetUserResponse{
					User:     targetUser,
					Metadata: targetUserMetadataReadPerm,
				},
			},
			{
				name: "target user has no ACL and querier is an admin",
				setup: func(db worldstate.DB) {
					addUser := map[string]*worldstate.DBUpdates{
						worldstate.UsersDBName: {
							Writes: []*worldstate.KVWithMetadata{
								{
									Key:   string(identity.UserNamespace) + "querierUser",
									Value: querierAdminUserSerialized,
								},
								{
									Key:      string(identity.UserNamespace) + "targetUser",
									Value:    targetUserSerialized,
									Metadata: targetUserMetadataNoACL,
								},
							},
						},
					}

					require.NoError(t, db.Commit(addUser, 1))
				},
				querierUserID: "querierUser",
				targetUserID:  "targetUser",
				expectedRespose: &types.GetUserResponse{
					User:     targetUser,
					Metadata: targetUserMetadataNoACL,
				},
			},
			{
				name: "target user does not exist",
				setup: func(db worldstate.DB) {
					addUser := map[string]*worldstate.DBUpdates{
						worldstate.UsersDBName: {
							Writes: []*worldstate.KVWithMetadata{
								{
									Key:   string(identity.UserNamespace) + "querierUser",
									Value: querierUserSerialized,
								},
							},
						},
					}

					require.NoError(t, db.Commit(addUser, 1))
				},
				querierUserID: "querierUser",
				targetUserID:  "targetUser",
				expectedRespose: &types.GetUserResponse{
					User:     nil,
					Metadata: nil,
				},
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				env := newWorldstateQueryProcessorTestEnv(t)
				defer env.cleanup(t)

				tt.setup(env.db)

				payload, err := env.q.getUser(tt.querierUserID, tt.targetUserID)
				require.NoError(t, err)
				require.True(t, proto.Equal(tt.expectedRespose, payload))
			})
		}
	})

	t.Run("error expected", func(t *testing.T) {
		tests := []struct {
			name          string
			setup         func(db worldstate.DB)
			querierUserID string
			targetUserID  string
			expectedError string
		}{
			{
				name: "querierUser has no read permission on the target user",
				setup: func(db worldstate.DB) {
					addUser := map[string]*worldstate.DBUpdates{
						worldstate.UsersDBName: {
							Writes: []*worldstate.KVWithMetadata{
								{
									Key:   string(identity.UserNamespace) + "querierUser",
									Value: querierUserSerialized,
								},
								{
									Key:      string(identity.UserNamespace) + "targetUser",
									Value:    targetUserSerialized,
									Metadata: targetUserMetadataNoReadPerm,
								},
							},
						},
					}

					require.NoError(t, db.Commit(addUser, 1))
				},
				querierUserID: "querierUser",
				targetUserID:  "targetUser",
				expectedError: "the user [querierUser] has no permission to read info of user [targetUser]",
			},
			{
				name: "target user has no ACL and querier is not an admin",
				setup: func(db worldstate.DB) {
					addUser := map[string]*worldstate.DBUpdates{
						worldstate.UsersDBName: {
							Writes: []*worldstate.KVWithMetadata{
								{
									Key:   string(identity.UserNamespace) + "querierUser",
									Value: querierUserSerialized,
								},
								{
									Key:      string(identity.UserNamespace) + "targetUser",
									Value:    targetUserSerialized,
									Metadata: targetUserMetadataNoACL,
								},
							},
						},
					}

					require.NoError(t, db.Commit(addUser, 1))
				},
				querierUserID: "querierUser",
				targetUserID:  "targetUser",
				expectedError: "the user [querierUser] has no permission to read info of user [targetUser]",
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				env := newWorldstateQueryProcessorTestEnv(t)
				defer env.cleanup(t)

				tt.setup(env.db)

				response, err := env.q.getUser(tt.querierUserID, tt.targetUserID)
				require.EqualError(t, err, tt.expectedError)
				require.Nil(t, response)
			})
		}
	})
}

func TestGetConfig(t *testing.T) {
	clusterConfig := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				Id:          "node1",
				Address:     "127.0.0.1",
				Port:        1234,
				Certificate: []byte("cert"),
			},
			{
				Id:          "node2",
				Address:     "127.0.0.1",
				Port:        2345,
				Certificate: []byte("cert"),
			},
		},
		Admins: []*types.Admin{
			{
				Id:          "admin",
				Certificate: []byte("admin-cert"),
			},
		},
		CertAuthConfig: &types.CAConfig{
			Roots: [][]byte{[]byte("cert")},
		},
	}

	adminUpdates, err := identity.ConstructDBEntriesForClusterAdmins(
		nil,
		clusterConfig.Admins,
		&types.Version{
			BlockNum: 1,
			TxNum:    5,
		},
	)
	require.NoError(t, err)

	t.Run("getConfig returns config", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		config, err := proto.Marshal(clusterConfig)
		require.NoError(t, err)

		metadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    5,
			},
		}

		dbUpdates := map[string]*worldstate.DBUpdates{
			worldstate.ConfigDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      worldstate.ConfigKey,
						Value:    config,
						Metadata: metadata,
					},
				},
			},
			worldstate.UsersDBName: {
				Writes: adminUpdates.Writes,
			},
		}
		dbUpdate, err := identity.ConstructDBEntriesForNodes(
			nil,
			clusterConfig.Nodes,
			&types.Version{
				BlockNum: 1,
				TxNum:    5,
			},
		)
		require.NoError(t, err)
		dbUpdates[worldstate.ConfigDBName].Writes = append(dbUpdates[worldstate.ConfigDBName].Writes, dbUpdate.Writes...)
		dbUpdates[worldstate.ConfigDBName].Deletes = append(dbUpdates[worldstate.ConfigDBName].Deletes, dbUpdate.Deletes...)
		require.NoError(t, env.db.Commit(dbUpdates, 1))

		configEnvelope, err := env.q.getConfig("admin")
		require.NoError(t, err)

		expectedConfig := &types.GetConfigResponse{
			Config:   clusterConfig,
			Metadata: metadata,
		}
		require.True(t, proto.Equal(expectedConfig, configEnvelope))
	})

	t.Run("getConfig returns err", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		metadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    5,
			},
		}

		dbUpdates := map[string]*worldstate.DBUpdates{
			worldstate.ConfigDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      worldstate.ConfigKey,
						Value:    []byte("config"),
						Metadata: metadata,
					},
				},
			},
			worldstate.UsersDBName: {
				Writes: adminUpdates.Writes,
			},
		}
		require.NoError(t, env.db.Commit(dbUpdates, 1))

		configEnvelope, err := env.q.getConfig("admin")
		require.Error(t, err)
		require.Contains(t, err.Error(), "error while unmarshaling committed cluster configuration")
		require.Nil(t, configEnvelope)
	})

	t.Run("getConfig returns user does not exist err", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		metadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    5,
			},
		}

		dbUpdates := map[string]*worldstate.DBUpdates{
			worldstate.ConfigDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      worldstate.ConfigKey,
						Value:    []byte("config"),
						Metadata: metadata,
					},
				},
			},
			worldstate.UsersDBName: {
				Writes: adminUpdates.Writes,
			},
		}
		require.NoError(t, env.db.Commit(dbUpdates, 1))

		configEnvelope, err := env.q.getConfig("user")
		require.Error(t, err)
		require.Contains(t, err.Error(), "the user [user] does not exist")
		require.Nil(t, configEnvelope)
	})

	t.Run("getConfig returns permission err", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		metadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    5,
			},
		}

		dbUpdates := map[string]*worldstate.DBUpdates{
			worldstate.ConfigDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      worldstate.ConfigKey,
						Value:    []byte("config"),
						Metadata: metadata,
					},
				},
			},
			worldstate.UsersDBName: {
				Writes: adminUpdates.Writes,
			},
		}

		user := &types.User{
			Id:          "someone",
			Certificate: []byte("someone-cert"),
			Privilege: &types.Privilege{
				Admin: false,
			},
		}
		value, err := proto.Marshal(user)
		require.NoError(t, err)

		kvWriteUser := &worldstate.KVWithMetadata{
			Key:      string(identity.UserNamespace) + user.Id,
			Value:    value,
			Metadata: metadata,
		}
		dbUpdates[worldstate.UsersDBName].Writes = append(dbUpdates[worldstate.UsersDBName].Writes, kvWriteUser)
		require.NoError(t, env.db.Commit(dbUpdates, 1))

		configEnvelope, err := env.q.getConfig("someone")
		require.Error(t, err)
		require.Contains(t, err.Error(), "the user [someone] has no permission to read a config object")
		require.Nil(t, configEnvelope)
	})

	t.Run("getNodeConfig returns single node and multiple nodes config", func(t *testing.T) {
		env := newWorldstateQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		config, err := proto.Marshal(clusterConfig)
		require.NoError(t, err)

		metadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    5,
			},
		}

		dbUpdates := map[string]*worldstate.DBUpdates{
			worldstate.ConfigDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      worldstate.ConfigKey,
						Value:    config,
						Metadata: metadata,
					},
				},
			},
			worldstate.UsersDBName: {
				Writes: adminUpdates.Writes,
			},
		}
		dbUpdate, err := identity.ConstructDBEntriesForNodes(
			nil,
			clusterConfig.Nodes,
			&types.Version{
				BlockNum: 1,
				TxNum:    5,
			},
		)
		dbUpdates[worldstate.ConfigDBName].Writes = append(dbUpdates[worldstate.ConfigDBName].Writes, dbUpdate.Writes...)
		dbUpdates[worldstate.ConfigDBName].Deletes = append(dbUpdates[worldstate.ConfigDBName].Deletes, dbUpdate.Deletes...)
		require.NoError(t, env.db.Commit(dbUpdates, 1))

		singleNodeConfigEnvelope, err := env.q.getNodeConfig("node1")
		require.NoError(t, err)

		expectedSingleNodeConfig := &types.GetNodeConfigResponse{
			NodeConfig: clusterConfig.Nodes[0],
		}
		require.True(t, proto.Equal(expectedSingleNodeConfig, singleNodeConfigEnvelope))

		singleNodeConfigEnvelope, err = env.q.getNodeConfig("node3")
		require.NoError(t, err)

		expectedSingleNodeConfig = &types.GetNodeConfigResponse{
			NodeConfig: nil,
		}
		require.True(t, proto.Equal(expectedSingleNodeConfig, singleNodeConfigEnvelope))
	})
}
