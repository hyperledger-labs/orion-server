package server

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
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
		nodeID: []byte("test-node-id1"),
		db:     db,
		logger: logger,
	}

	return &queryProcessorTestEnv{
		db:      db,
		q:       newQueryProcessor(qProcConfig),
		cleanup: cleanup,
	}
}

func TestGetStatus(t *testing.T) {
	t.Parallel()

	t.Run("GetStatus-Returns-Status", func(t *testing.T) {
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
		require.NoError(t, env.db.Commit(createDB))

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
			req := &types.GetStatusQueryEnvelope{
				Payload: &types.GetStatusQuery{
					UserID: "testUser",
					DBName: testCase.dbName,
				},
				Signature: []byte("signature"),
			}
			status, err := env.q.getStatus(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, status.Payload)
			require.Equal(t, testCase.isExist, status.Payload.Exist)
			require.NotNil(t, status.Payload.Header)
			require.Equal(t, "test-node-id1", string(status.Payload.Header.NodeID))
		}
	})

	t.Run("GetStatus-Returns-Error", func(t *testing.T) {
		t.Parallel()
		env := newQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		testCases := []struct {
			request       *types.GetStatusQueryEnvelope
			expectedError string
		}{
			{
				request:       nil,
				expectedError: "`GetStatusQueryEnvelope` is nil",
			},
			{
				request: &types.GetStatusQueryEnvelope{
					Payload: nil,
				},
				expectedError: "`Payload` in `GetStatusQueryEnvelope` is nil",
			},
			{
				request: &types.GetStatusQueryEnvelope{
					Payload: &types.GetStatusQuery{
						UserID: "",
					},
				},
				expectedError: "`UserID` is not set in `Payload`",
			},
		}

		for _, testCase := range testCases {
			status, err := env.q.getStatus(context.Background(), testCase.request)
			require.Contains(t, err.Error(), testCase.expectedError)
			require.Nil(t, status)
		}
	})
}

func TestGetState(t *testing.T) {
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
		require.NoError(t, db.Commit(createUser))

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
		require.NoError(t, db.Commit(createDB))
	}

	t.Run("GetState-Returns-State", func(t *testing.T) {
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
		require.NoError(t, env.db.Commit(dbsUpdates))

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
			req := &types.GetStateQueryEnvelope{
				Payload: &types.GetStateQuery{
					UserID: "testUser",
					DBName: "test-db",
					Key:    testCase.key,
				},
				Signature: []byte("signature"),
			}

			val, err := env.q.getState(context.Background(), req)
			require.NoError(t, err)
			require.NotNil(t, val.Payload)
			require.Equal(t, testCase.expectedValue, val.Payload.Value)
			require.True(t, proto.Equal(testCase.expectedMetadata, val.Payload.Metadata))
			require.NotNil(t, val.Payload.Header)
			require.Equal(t, "test-node-id1", string(val.Payload.Header.NodeID))
		}
	})

	t.Run("GetState-Returns-Error", func(t *testing.T) {
		t.Parallel()
		env := newQueryProcessorTestEnv(t)
		defer env.cleanup(t)

		setup(env.db, "testUser1", "test-db")
		setup(env.db, "testUser2", "")

		val := []byte("value1")
		metadata := &types.Metadata{
			Version: &types.Version{
				BlockNum: 2,
				TxNum:    1,
			},
			AccessControl: &types.AccessControl{
				ReadUsers: map[string]bool{
					"testUser2": true,
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
		require.NoError(t, env.db.Commit(dbsUpdates))

		testCases := []struct {
			name          string
			request       *types.GetStateQueryEnvelope
			expectedError string
		}{
			{
				name:          "query envelope is nil",
				request:       nil,
				expectedError: "`GetStateQueryEnvelope` is nil",
			},
			{
				name: "payload is nil",
				request: &types.GetStateQueryEnvelope{
					Payload: nil,
				},
				expectedError: "`Payload` in `GetStateQueryEnvelope` is nil",
			},
			{
				name: "userID not set",
				request: &types.GetStateQueryEnvelope{
					Payload: &types.GetStateQuery{
						UserID: "",
					},
				},
				expectedError: "`UserID` is not set in `Payload`",
			},
			{
				name: "user has no permission to read the key",
				request: &types.GetStateQueryEnvelope{
					Payload: &types.GetStateQuery{
						UserID: "testUser1",
						DBName: "test-db",
						Key:    "key1",
					},
					Signature: []byte("signature"),
				},
				expectedError: "the user [testUser1] has no permission to read key [key1] from database [test-db]",
			},
			{
				name: "user has no permission to read from the database",
				request: &types.GetStateQueryEnvelope{
					Payload: &types.GetStateQuery{
						UserID: "testUser2",
						DBName: "test-db",
						Key:    "key1",
					},
					Signature: []byte("signature"),
				},
				expectedError: "the user [testUser2] has no permission to read from database [test-db]",
			},
			{
				name: "user has no permission to read from the database",
				request: &types.GetStateQueryEnvelope{
					Payload: &types.GetStateQuery{
						UserID: "testUser2",
						DBName: "_config",
						Key:    "config",
					},
					Signature: []byte("signature"),
				},
				expectedError: "the user [testUser2] has no permission to read from database [_config]",
			},
		}

		for _, testCase := range testCases {
			state, err := env.q.getState(context.Background(), testCase.request)
			require.Contains(t, err.Error(), testCase.expectedError)
			require.Nil(t, state)
		}
	})
}
