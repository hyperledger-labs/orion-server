package leveldb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/fileops"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type testEnv struct {
	l       *LevelDB
	path    string
	cleanup func()
}

func newTestEnv(t *testing.T) *testEnv {
	dir, err := ioutil.TempDir("/tmp", "ledger")
	require.NoError(t, err)

	path := filepath.Join(dir, "leveldb")
	l, err := Open(path)
	if err != nil {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove %s, %v", dir, err)
		}
		t.Fatalf("failed to create leveldb with path %s", path)
	}

	cleanup := func() {
		if err := l.Close(); err != nil {
			t.Errorf("failed to close the database instance, %v", err)
		}

		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove %s, %v", dir, err)
		}
	}

	require.Equal(t, path, l.dbRootDir)
	require.Len(t, l.dbs, len(systemDBs))

	for _, dbName := range systemDBs {
		require.NotNil(t, l.dbs[dbName])
	}

	return &testEnv{
		l:       l,
		path:    path,
		cleanup: cleanup,
	}
}

func TestCreateDB(t *testing.T) {
	t.Parallel()

	t.Run("create new database", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		dbName := "db1"

		verifyDBExistance(t, l, dbName, false)

		require.NoError(t, l.create(dbName))
		verifyDBExistance(t, l, dbName, true)
	})

	t.Run("create already existing database -- no-op", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		dbName := "db1"

		require.NoError(t, l.create(dbName))
		verifyDBExistance(t, l, dbName, true)

		db := l.dbs[dbName]
		db.file.Put([]byte("key1"), []byte("value1"), &opt.WriteOptions{Sync: true})

		require.NoError(t, l.create(dbName))

		actualVal, err := db.file.Get([]byte("key1"), nil)
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), actualVal)

		verifyDBExistance(t, l, dbName, true)
	})
}

func TestDeleteDB(t *testing.T) {
	t.Parallel()

	t.Run("deleting an existing database", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		dbName := "db1"

		require.NoError(t, l.create(dbName))
		verifyDBExistance(t, l, dbName, true)

		require.NoError(t, l.delete(dbName))
		verifyDBExistance(t, l, dbName, false)
	})

	t.Run("deleting a non-existing database -- no-op", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		dbName := "db1"

		verifyDBExistance(t, l, dbName, false)
		require.NoError(t, l.delete(dbName))
	})
}

func verifyDBExistance(t *testing.T, l *LevelDB, dbName string, expected bool) {
	require.Equal(t, expected, l.Exist(dbName))
	exist, err := fileops.Exists(filepath.Join(l.dbRootDir, dbName))
	require.NoError(t, err)
	require.Equal(t, expected, exist)
}

func TestListDBsAndExist(t *testing.T) {
	t.Parallel()

	t.Run("list all user databases", func(t *testing.T) {
		env := newTestEnv(t)
		l := env.l
		defer env.cleanup()

		dbs := []string{"db1", "db2", "db3"}
		for _, name := range dbs {
			require.NoError(t, l.create(name))
		}

		require.ElementsMatch(t, dbs, l.ListDBs())
	})

	t.Run("check for database existance", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup()

		for _, dbName := range systemDBs {
			require.True(t, env.l.Exist(dbName))
		}

		for _, dbName := range []string{"no-db1", "no-db2", "no-db3"} {
			require.False(t, env.l.Exist(dbName))
		}
	})
}

func TestCommitAndGet(t *testing.T) {
	t.Parallel()

	setupWithNoData := func(l *LevelDB) {
		require.NoError(t, l.create("db1"))
		require.NoError(t, l.create("db2"))
	}

	setupWithData := func(l *LevelDB) (map[string]*ValueAndMetadata, map[string]*ValueAndMetadata) {
		db1valAndMetadata1 := &ValueAndMetadata{
			Value: []byte("db1-value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
			},
		}
		db1valAndMetadata2 := &ValueAndMetadata{
			Value: []byte("db1-value2"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    2,
				},
			},
		}
		db2valAndMetadata1 := &ValueAndMetadata{
			Value: []byte("db2-value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    3,
				},
			},
		}
		db2valAndMetadata2 := &ValueAndMetadata{
			Value: []byte("db2-value2"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    4,
				},
			},
		}
		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "db1",
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      "db1-key1",
						Value:    db1valAndMetadata1.Value,
						Metadata: db1valAndMetadata1.Metadata,
					},
					{
						Key:      "db1-key2",
						Value:    db1valAndMetadata2.Value,
						Metadata: db1valAndMetadata2.Metadata,
					},
				},
			},
			{
				DBName: "db2",
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      "db2-key1",
						Value:    db2valAndMetadata1.Value,
						Metadata: db2valAndMetadata1.Metadata,
					},
					{
						Key:      "db2-key2",
						Value:    db2valAndMetadata2.Value,
						Metadata: db2valAndMetadata2.Metadata,
					},
				},
			},
		}

		require.NoError(t, l.create("db1"))
		require.NoError(t, l.create("db2"))
		require.NoError(t, l.Commit(dbsUpdates))

		db1KVs := map[string]*ValueAndMetadata{
			"db1-key1": db1valAndMetadata1,
			"db1-key2": db1valAndMetadata2,
		}
		db2KVs := map[string]*ValueAndMetadata{
			"db2-key1": db2valAndMetadata1,
			"db2-key2": db2valAndMetadata2,
		}
		return db1KVs, db2KVs
	}

	t.Run("Get() and GetVersion() on empty databases", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		setupWithNoData(l)

		val, metadata, err := l.Get("db1", "db1-key1")
		require.NoError(t, err)
		require.Nil(t, val)
		require.Nil(t, metadata)

		val, metadata, err = l.Get("db2", "db2-key1")
		require.NoError(t, err)
		require.Nil(t, val)
		require.Nil(t, metadata)

		ver, err := l.GetVersion("db1", "db1-key1")
		require.NoError(t, err)
		require.Nil(t, ver)

		ver, err = l.GetVersion("db2", "db2-key1")
		require.NoError(t, err)
		require.Nil(t, ver)
	})

	// Scenario-2: For both databases (db1, db2), create
	// two keys per database (db1-key1, db1-key2 for db1) and
	// (db2-key1, db2-key2 for db2) and commit them.
	t.Run("Get() and GetVersion() on non-empty databases", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		db1KVs, db2KVs := setupWithData(l)

		for key, expectedValAndMetadata := range db1KVs {
			val, metadata, err := l.Get("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(
				expectedValAndMetadata,
				&ValueAndMetadata{Value: val, Metadata: metadata},
			))

			ver, err := l.GetVersion("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata().GetVersion(), ver))
		}

		for key, expectedValAndMetadata := range db2KVs {
			val, metadata, err := l.Get("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(
				expectedValAndMetadata,
				&ValueAndMetadata{Value: val, Metadata: metadata},
			))

			ver, err := l.GetVersion("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata().GetVersion(), ver))
		}
	})

	// Scenario-2: For both databases (db1, db2), update
	// the first key and delete the second key.
	// (db1-key1 is updated, db1-key2 is deleted for db1) and
	// (db2-key1 is updated, db2-key2 is deleted for db2) and
	// commit them.
	t.Run("Commit() and Get() on non-empty databases", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		db1KVs, db2KVs := setupWithData(l)

		db1valAndMetadata1New := &ValueAndMetadata{
			Value: []byte("db1-value1-new"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 2,
					TxNum:    1,
				},
			},
		}
		db2valAndMetadata1New := &ValueAndMetadata{
			Value: []byte("db2-value1-new"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 2,
					TxNum:    2,
				},
			},
		}
		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "db1",
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      "db1-key1",
						Value:    db1valAndMetadata1New.Value,
						Metadata: db1valAndMetadata1New.Metadata,
					},
				},
				Deletes: []string{"db1-key2"},
			},
			{
				DBName: "db2",
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      "db2-key1",
						Value:    db2valAndMetadata1New.Value,
						Metadata: db2valAndMetadata1New.Metadata,
					},
				},
				Deletes: []string{"db2-key2"},
			},
		}

		require.NoError(t, l.Commit(dbsUpdates))

		db1KVs["db1-key1"] = db1valAndMetadata1New
		db1KVs["db1-key2"] = nil
		for key, expectedValAndMetadata := range db1KVs {
			val, metadata, err := l.Get("db1", key)
			require.NoError(t, err)
			require.Equal(t, expectedValAndMetadata.GetValue(), val)
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata(), metadata))

			ver, err := l.GetVersion("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata().GetVersion(), ver))
		}

		db2KVs["db2-key1"] = db2valAndMetadata1New
		db2KVs["db2-key2"] = nil
		for key, expectedValAndMetadata := range db2KVs {
			val, metadata, err := l.Get("db2", key)
			require.NoError(t, err)
			require.Equal(t, expectedValAndMetadata.GetValue(), val)
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata(), metadata))

			ver, err := l.GetVersion("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata().GetVersion(), ver))
		}
	})
}

func TestCommitWithDBManagement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		preCreateDBs           []string
		updates                *worldstate.DBUpdates
		expectedDBsAfterCommit []string
	}{
		{
			name:         "only create DBs",
			preCreateDBs: nil,
			updates: &worldstate.DBUpdates{
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
			expectedDBsAfterCommit: []string{"db1", "db2"},
		},
		{
			name:         "only delete DBs",
			preCreateDBs: []string{"db1", "db2"},
			updates: &worldstate.DBUpdates{
				DBName:  worldstate.DatabasesDBName,
				Deletes: []string{"db1", "db2"},
			},
			expectedDBsAfterCommit: nil,
		},
		{
			name:         "create and delete DBs",
			preCreateDBs: []string{"db3", "db4"},
			updates: &worldstate.DBUpdates{
				DBName: worldstate.DatabasesDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key: "db1",
					},
					{
						Key: "db2",
					},
				},
				Deletes: []string{"db3", "db4"},
			},
			expectedDBsAfterCommit: []string{"db1", "db2"},
		},
		{
			name:         "create already existing DBs and delete non-existing DBs -- applicable during node failure and reply of block",
			preCreateDBs: []string{"db1", "db3"},
			updates: &worldstate.DBUpdates{
				DBName: worldstate.DatabasesDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key: "db1",
					},
					{
						Key: "db2",
					},
				},
				Deletes: []string{"db3", "db4"},
			},
			expectedDBsAfterCommit: []string{"db1", "db2"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			defer env.cleanup()
			l := env.l

			for _, name := range tt.preCreateDBs {
				require.NoError(t, l.create(name))
			}

			require.NoError(
				t,
				l.Commit(
					[]*worldstate.DBUpdates{
						tt.updates,
					},
				),
			)

			require.ElementsMatch(t, tt.expectedDBsAfterCommit, l.ListDBs())
		})
	}
}
