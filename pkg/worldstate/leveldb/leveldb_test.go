package leveldb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type testEnv struct {
	l       *LevelDB
	path    string
	cleanup func()
}

func (env *testEnv) init(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "ledger")
	require.NoError(t, err)
	env.path = filepath.Join(dir, "leveldb")

	env.cleanup = func() {
		require.NoError(t, os.RemoveAll(dir))
		require.NoError(t, os.RemoveAll(env.path))
	}

	l, err := NewLevelDB(env.path)
	if err != nil {
		env.cleanup()
		t.Fatalf("failed to create leveldb with path %s", env.path)
	}
	require.Equal(t, env.path, l.dirPath)
	require.Empty(t, l.dbs)

	env.l = l
}

func TestCreateAndOpenDB(t *testing.T) {
	t.Run("opening an non-existing database", func(t *testing.T) {
		t.Parallel()
		env := &testEnv{}
		env.init(t)
		defer env.cleanup()
		l := env.l
		require.Contains(t, l.Open("db1").Error(), "database db1 does not exist")
		exists, err := fileExists(filepath.Join(env.path, "db1"))
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("creating and opening a database", func(t *testing.T) {
		t.Parallel()
		env := &testEnv{}
		env.init(t)
		defer env.cleanup()
		l := env.l
		require.NoError(t, l.Create("db1"))
		require.DirExists(t, filepath.Join(env.path, "db1"))
		require.NotEmpty(t, l.dbs)
		require.NotNil(t, l.dbs["db1"])
		require.NoError(t, l.Open("db1"))
	})
}

func TestCommitAndGet(t *testing.T) {
	setupWithNoData := func(l *LevelDB) {
		require.NoError(t, l.Create("db1"))
		require.NoError(t, l.Create("db2"))
	}

	setupWithData := func(l *LevelDB) (map[string]*api.Value, map[string]*api.Value) {
		db1val1 := &api.Value{
			Value: []byte("db1-value1"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 1,
					TxNum:    1,
				},
			},
		}
		db1val2 := &api.Value{
			Value: []byte("db1-value2"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 1,
					TxNum:    2,
				},
			},
		}
		db2val1 := &api.Value{
			Value: []byte("db2-value1"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 1,
					TxNum:    3,
				},
			},
		}
		db2val2 := &api.Value{
			Value: []byte("db2-value2"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 1,
					TxNum:    4,
				},
			},
		}
		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "db1",
				Writes: []*worldstate.KV{
					{
						Key:   "db1-key1",
						Value: db1val1,
					},
					{
						Key:   "db1-key2",
						Value: db1val2,
					},
				},
			},
			{
				DBName: "db2",
				Writes: []*worldstate.KV{
					{
						Key:   "db2-key1",
						Value: db2val1,
					},
					{
						Key:   "db2-key2",
						Value: db2val2,
					},
				},
			},
		}

		require.NoError(t, l.Create("db1"))
		require.NoError(t, l.Create("db2"))
		require.NoError(t, l.Commit(dbsUpdates))

		db1KVs := map[string]*api.Value{
			"db1-key1": db1val1,
			"db1-key2": db1val2,
		}
		db2KVs := map[string]*api.Value{
			"db2-key1": db2val1,
			"db2-key2": db2val2,
		}
		return db1KVs, db2KVs
	}

	t.Run("Get() and GetVersion() on empty databases", func(t *testing.T) {
		t.Parallel()
		env := &testEnv{}
		env.init(t)
		defer env.cleanup()
		l := env.l
		setupWithNoData(l)

		val, err := l.Get("db1", "db1-key1")
		require.NoError(t, err)
		require.Nil(t, val)

		val, err = l.Get("db2", "db2-key1")
		require.NoError(t, err)
		require.Nil(t, val)

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
		env := &testEnv{}
		env.init(t)
		defer env.cleanup()
		l := env.l
		db1KVs, db2KVs := setupWithData(l)

		for key, expectedVal := range db1KVs {
			val, err := l.Get("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal, val))

			ver, err := l.GetVersion("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal.GetMetadata().GetVersion(), ver))
		}

		for key, expectedVal := range db2KVs {
			val, err := l.Get("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal, val))

			ver, err := l.GetVersion("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal.GetMetadata().GetVersion(), ver))
		}
	})

	// Scenario-2: For both databases (db1, db2), update
	// the first key and delete the second key.
	// (db1-key1 is updated, db1-key2 is deleted for db1) and
	// (db2-key1 is updated, db2-key2 is deleted for db2) and
	// commit them.
	t.Run("Commit() and Get() on non-empty databases", func(t *testing.T) {
		t.Parallel()
		env := &testEnv{}
		env.init(t)
		defer env.cleanup()
		l := env.l
		db1KVs, db2KVs := setupWithData(l)

		db1val1new := &api.Value{
			Value: []byte("db1-value1-new"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 2,
					TxNum:    1,
				},
			},
		}
		db2val1new := &api.Value{
			Value: []byte("db2-value1-new"),
			Metadata: &api.Metadata{
				Version: &api.Version{
					BlockNum: 2,
					TxNum:    2,
				},
			},
		}
		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: "db1",
				Writes: []*worldstate.KV{
					{
						Key:   "db1-key1",
						Value: db1val1new,
					},
				},
				Deletes: []string{"db1-key2"},
			},
			{
				DBName: "db2",
				Writes: []*worldstate.KV{
					{
						Key:   "db2-key1",
						Value: db2val1new,
					},
				},
				Deletes: []string{"db2-key2"},
			},
		}

		require.NoError(t, l.Commit(dbsUpdates))

		db1KVs["db1-key1"] = db1val1new
		db1KVs["db1-key2"] = nil
		for key, expectedVal := range db1KVs {
			val, err := l.Get("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal, val))

			ver, err := l.GetVersion("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal.GetMetadata().GetVersion(), ver))
		}

		db2KVs["db2-key1"] = db2val1new
		db2KVs["db2-key2"] = nil
		for key, expectedVal := range db2KVs {
			val, err := l.Get("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal, val))

			ver, err := l.GetVersion("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedVal.GetMetadata().GetVersion(), ver))
		}
	})
}

func TestNewLevelDB(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "ledger")
	require.NoError(t, err)
	levelPath := filepath.Join(dir, "leveldb")
	defer func() {
		require.NoError(t, os.RemoveAll(dir))
		require.NoError(t, os.RemoveAll(levelPath))
	}()

	l, err := NewLevelDB(levelPath)
	require.NoError(t, err)
	require.NoError(t, l.Create("db1"))
	require.NoError(t, l.Create("db2"))
	require.NoError(t, l.Create("db3"))
	require.NoError(t, l.Create("db4"))

	require.Len(t, l.dbs, 4)
	require.NoError(t, l.Open("db1"))
	require.NoError(t, l.Open("db2"))
	require.NoError(t, l.Open("db3"))
	require.NoError(t, l.Open("db4"))

	closeLevelDB(t, l)
	l, err = NewLevelDB(levelPath)
	require.NoError(t, err)
	require.Len(t, l.dbs, 4)
	require.NoError(t, l.Open("db1"))
	require.NoError(t, l.Open("db2"))
	require.NoError(t, l.Open("db3"))
	require.NoError(t, l.Open("db4"))
}

func closeLevelDB(t *testing.T, l *LevelDB) {
	for _, db := range l.dbs {
		require.NoError(t, db.file.Close())
	}
}
