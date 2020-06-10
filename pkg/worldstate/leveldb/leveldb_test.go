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
		defer os.RemoveAll(dir)
		defer os.RemoveAll(env.path)
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

var env testEnv

func TestCreateAndOpenDB(t *testing.T) {
	env.init(t)
	defer env.cleanup()
	l := env.l

	require.Contains(t, l.Open("db1").Error(), "database db1 does not exist")
	exists, err := fileExists(filepath.Join(env.path, "db1"))
	require.NoError(t, err)
	require.False(t, exists)

	require.NoError(t, l.Create("db1"))
	require.DirExists(t, filepath.Join(env.path, "db1"))
	require.NotEmpty(t, l.dbs)
	require.NotNil(t, l.dbs["db1"])
	require.NoError(t, l.Open("db1"))
}

func TestCommitAndGet(t *testing.T) {
	env.init(t)
	defer env.cleanup()
	l := env.l

	require.NoError(t, l.Create("db1"))
	require.NoError(t, l.Create("db2"))

	// Scenario-1: DB is empty. Hence, all Get should
	// return a nil value and no error
	val, err := l.Get("db1", "db1-key1")
	require.NoError(t, err)
	require.Nil(t, val)

	val, err = l.Get("db1", "db1-key2")
	require.NoError(t, err)
	require.Nil(t, val)

	val, err = l.Get("db1", "db2-key1")
	require.NoError(t, err)
	require.Nil(t, val)

	val, err = l.Get("db1", "db2-key1")
	require.NoError(t, err)
	require.Nil(t, val)

	// Scenario-2: For both databases (db1, db2), create
	// two keys per database (db1-key1, db1-key2 for db1) and
	// (db2-key1, db2-key2 for db2) and commit them.
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

	require.NoError(t, l.Commit(dbsUpdates))

	val, err = l.Get("db1", "db1-key1")
	require.NoError(t, err)
	require.True(t, proto.Equal(db1val1, val))

	val, err = l.Get("db1", "db1-key2")
	require.NoError(t, err)
	require.True(t, proto.Equal(db1val2, val))

	val, err = l.Get("db2", "db2-key1")
	require.NoError(t, err)
	require.True(t, proto.Equal(db2val1, val))

	val, err = l.Get("db2", "db2-key2")
	require.NoError(t, err)
	require.True(t, proto.Equal(db2val2, val))

	// Scenario-2: For both databases (db1, db2), update
	// the first key and delete the second key.
	// (db1-key1 is updated, db1-key2 is deleted for db1) and
	// (db2-key1 is updated, db2-key2 is deleted for db2) and
	// commit them.
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
	dbsUpdates = []*worldstate.DBUpdates{
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

	val, err = l.Get("db1", "db1-key1")
	require.NoError(t, err)
	require.True(t, proto.Equal(db1val1new, val))

	val, err = l.Get("db1", "db1-key2")
	require.NoError(t, err)
	require.Nil(t, val)

	val, err = l.Get("db2", "db2-key1")
	require.NoError(t, err)
	require.True(t, proto.Equal(db2val1new, val))

	val, err = l.Get("db2", "db2-key2")
	require.NoError(t, err)
	require.Nil(t, val)

	// test GetVersion
	ver, err := l.GetVersion("db1", "db1-key1")
	require.NoError(t, err)
	require.True(t, proto.Equal(db1val1new.Metadata.Version, ver))

	ver, err = l.GetVersion("db1", "db1-key2")
	require.NoError(t, err)
	require.Nil(t, ver)

	ver, err = l.GetVersion("db2", "db2-key1")
	require.NoError(t, err)
	require.True(t, proto.Equal(db2val1new.Metadata.Version, ver))

	ver, err = l.GetVersion("db2", "db2-key2")
	require.NoError(t, err)
	require.Nil(t, ver)
}

func TestNewLevelDB(t *testing.T) {
	dir, err := ioutil.TempDir("/tmp", "ledger")
	require.NoError(t, err)
	levelPath := filepath.Join(dir, "leveldb")
	defer func() {
		os.RemoveAll(dir)
		os.RemoveAll(levelPath)
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
