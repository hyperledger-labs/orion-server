package backend

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/internal/worldstate/leveldb"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type queryProcessorTestEnv struct {
	db      *leveldb.LevelDB
	q       *queryProcessor
	cleanup func(t *testing.T)
}

func newQueryProcessorTestEnv(t *testing.T) *queryProcessorTestEnv {
	path, err := ioutil.TempDir("/tmp", "queryProcessor")
	require.NoError(t, err)

	c := &logger.Config{
		Level:         "debug",
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
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove %s due to %v", path, err)
		}

		t.Fatalf("failed to create a new leveldb instance, %v", err)
	}

	cleanup := func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close leveldb: %v", err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Fatalf("failed to remove %s due to %v", path, err)
		}
	}

	qProcConfig := &queryProcessorConfig{
		nodeID:          "test-node-id1",
		db:              db,
		identityQuerier: identity.NewQuerier(db),
		logger:          logger,
	}

	return &queryProcessorTestEnv{
		db:      db,
		q:       newQueryProcessor(qProcConfig),
		cleanup: cleanup,
	}
}

func TestGetDBStatus(t *testing.T) {
	t.Parallel()

	t.Run("getDBStatus-Returns-Status", func(t *testing.T) {
		t.Parallel()
		env := newQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		createDB := []*worldstate.DBUpdates{
			{
				DBName: worldstate.DatabasesDBName,
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
			require.NotNil(t, status.Payload)
			require.Equal(t, testCase.isExist, status.Payload.Exist)
			require.NotNil(t, status.Payload.Header)
			require.Equal(t, "test-node-id1", string(status.Payload.Header.NodeID))
		}
	})
}

func TestGetData(t *testing.T) {
	t.Parallel()

	setup := func(db worldstate.DB, userID, dbName string) {
		user := &types.User{
			ID: userID,
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					dbName: types.Privilege_ReadWrite,
				},
			},
		}

		u, err := proto.Marshal(user)
		require.NoError(t, err)

		createUser := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
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

		createDB := []*worldstate.DBUpdates{
			{
				DBName: worldstate.DatabasesDBName,
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
		t.Parallel()
		env := newQueryProcessorTestEnv(t)
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

		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "test-db",
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
			val, err := env.q.getData("test-db", "testUser", testCase.key)
			require.NoError(t, err)
			require.NotNil(t, val.Payload)
			require.Equal(t, testCase.expectedValue, val.Payload.Value)
			require.True(t, proto.Equal(testCase.expectedMetadata, val.Payload.Metadata))
			require.NotNil(t, val.Payload.Header)
			require.Equal(t, "test-node-id1", string(val.Payload.Header.NodeID))
		}
	})

	t.Run("getData returns permission error due to ACL", func(t *testing.T) {
		t.Parallel()
		env := newQueryProcessorTestEnv(t)
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

		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "test-db",
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
		t.Parallel()
		env := newQueryProcessorTestEnv(t)
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

func TestGetUser(t *testing.T) {
	t.Parallel()

	t.Run("query existing user", func(t *testing.T) {
		querierUser := &types.User{
			ID: "querierUser",
		}
		querierUserSerialized, err := proto.Marshal(querierUser)
		require.NoError(t, err)

		targetUser := &types.User{
			ID: "targetUser",
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					"db1": types.Privilege_ReadWrite,
				},
				DBAdministration: true,
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

		targetUserMetadataReadWritePerm := &types.Metadata{
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

		targetUserMetadataNoACL := &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    1,
			},
		}

		tests := []struct {
			name            string
			setup           func(db worldstate.DB)
			querierUserID   string
			targetUserID    string
			expectedRespose *types.GetUserResponseEnvelope
		}{
			{
				name: "querierUser has read permission on targetUser",
				setup: func(db worldstate.DB) {
					addUser := []*worldstate.DBUpdates{
						{
							DBName: worldstate.UsersDBName,
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
				expectedRespose: &types.GetUserResponseEnvelope{
					Payload: &types.GetUserResponse{
						Header: &types.ResponseHeader{
							NodeID: "test-node-id1",
						},
						User:     targetUser,
						Metadata: targetUserMetadataReadPerm,
					},
				},
			},
			{
				name: "querierUser has read-write permission on targetUser",
				setup: func(db worldstate.DB) {
					addUser := []*worldstate.DBUpdates{
						{
							DBName: worldstate.UsersDBName,
							Writes: []*worldstate.KVWithMetadata{
								{
									Key:   string(identity.UserNamespace) + "querierUser",
									Value: querierUserSerialized,
								},
								{
									Key:      string(identity.UserNamespace) + "targetUser",
									Value:    targetUserSerialized,
									Metadata: targetUserMetadataReadWritePerm,
								},
							},
						},
					}

					require.NoError(t, db.Commit(addUser, 1))
				},
				querierUserID: "querierUser",
				targetUserID:  "targetUser",
				expectedRespose: &types.GetUserResponseEnvelope{
					Payload: &types.GetUserResponse{
						Header: &types.ResponseHeader{
							NodeID: "test-node-id1",
						},
						User:     targetUser,
						Metadata: targetUserMetadataReadWritePerm,
					},
				},
			},
			{
				name: "target user has no ACL",
				setup: func(db worldstate.DB) {
					addUser := []*worldstate.DBUpdates{
						{
							DBName: worldstate.UsersDBName,
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
				expectedRespose: &types.GetUserResponseEnvelope{
					Payload: &types.GetUserResponse{
						Header: &types.ResponseHeader{
							NodeID: "test-node-id1",
						},
						User:     targetUser,
						Metadata: targetUserMetadataNoACL,
					},
				},
			},
			{
				name: "target user does not exist",
				setup: func(db worldstate.DB) {
					addUser := []*worldstate.DBUpdates{
						{
							DBName: worldstate.UsersDBName,
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
				expectedRespose: &types.GetUserResponseEnvelope{
					Payload: &types.GetUserResponse{
						Header: &types.ResponseHeader{
							NodeID: "test-node-id1",
						},
						User:     nil,
						Metadata: nil,
					},
				},
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				env := newQueryProcessorTestEnv(t)
				defer env.cleanup(t)

				tt.setup(env.db)

				response, err := env.q.getUser(tt.querierUserID, tt.targetUserID)
				require.NoError(t, err)
				require.True(t, proto.Equal(tt.expectedRespose, response))
			})
		}
	})

	t.Run("error expected", func(t *testing.T) {
		t.Parallel()

		querierUser := &types.User{
			ID: "querierUser",
		}
		querierUserSerialized, err := proto.Marshal(querierUser)
		require.NoError(t, err)

		targetUser := &types.User{
			ID: "targetUser",
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					"db1": types.Privilege_ReadWrite,
				},
				DBAdministration: true,
			},
		}
		targetUserSerialized, err := proto.Marshal(targetUser)
		require.NoError(t, err)

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
					addUser := []*worldstate.DBUpdates{
						{
							DBName: worldstate.UsersDBName,
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
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				env := newQueryProcessorTestEnv(t)
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
	t.Parallel()

	t.Run("getConfig returns config", func(t *testing.T) {
		env := newQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		clusterConfig := &types.ClusterConfig{
			Nodes: []*types.NodeConfig{
				{
					ID:          "node1",
					Address:     "127.0.0.1",
					Port:        1234,
					Certificate: []byte("cert"),
				},
			},
			Admins: []*types.Admin{
				{
					ID:          "admin",
					Certificate: []byte("cert"),
				},
			},
			RootCACertificate: []byte("cert"),
		}

		config, err := proto.Marshal(clusterConfig)
		require.NoError(t, err)

		metadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    5,
			},
		}

		dbUpdates := []*worldstate.DBUpdates{
			{
				DBName: worldstate.ConfigDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      worldstate.ConfigKey,
						Value:    config,
						Metadata: metadata,
					},
				},
			},
		}
		require.NoError(t, env.db.Commit(dbUpdates, 1))

		configEnvelope, err := env.q.getConfig()
		require.NoError(t, err)

		expectedConfigEnvelope := &types.GetConfigResponseEnvelope{
			Payload: &types.GetConfigResponse{
				Header: &types.ResponseHeader{
					NodeID: env.q.nodeID,
				},
				Config:   clusterConfig,
				Metadata: metadata,
			},
			Signature: nil,
		}
		require.True(t, proto.Equal(expectedConfigEnvelope, configEnvelope))
	})

	t.Run("getConfig returns err", func(t *testing.T) {
		env := newQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		metadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    5,
			},
		}

		dbUpdates := []*worldstate.DBUpdates{
			{
				DBName: worldstate.ConfigDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      worldstate.ConfigKey,
						Value:    []byte("config"),
						Metadata: metadata,
					},
				},
			},
		}
		require.NoError(t, env.db.Commit(dbUpdates, 1))

		configEnvelope, err := env.q.getConfig()
		require.Contains(t, err.Error(), "error while unmarshaling committed cluster configuration")
		require.Nil(t, configEnvelope)
	})
}
