// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package leveldb

import (
	"path/filepath"
	"testing"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

type testEnv struct {
	l       *LevelDB
	path    string
	cleanup func()
}

func newTestEnv(t *testing.T) *testEnv {
	dir := t.TempDir()

	path := filepath.Join(dir, "leveldb")

	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	conf := &Config{
		DBRootDir: path,
		Logger:    logger,
	}
	l, err := Open(conf)
	if err != nil {
		t.Fatalf("failed to create leveldb with path %s", path)
	}

	cleanup := func() {
		if err := l.Close(); err != nil {
			t.Errorf("failed to close the database instance, %v", err)
		}
	}

	require.Equal(t, path, l.dbRootDir)
	require.Equal(t, l.size(), len(preCreateDBs))

	for _, dbName := range preCreateDBs {
		db, ok := l.getDB(dbName)
		require.True(t, ok)
		require.NotNil(t, db)
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

		db, ok := l.getDB(dbName)
		require.True(t, ok)
		err := db.file.Put([]byte("key1"), []byte("value1"), &opt.WriteOptions{Sync: true})
		require.NoError(t, err)

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

		for _, dbName := range preCreateDBs {
			require.True(t, env.l.Exist(dbName))
		}

		for _, dbName := range []string{"no-db1", "no-db2", "no-db3"} {
			require.False(t, env.l.Exist(dbName))
		}
	})
}

func TestCommitAndQuery(t *testing.T) {
	t.Parallel()

	setupWithNoData := func(l *LevelDB) {
		require.NoError(t, l.create("db1"))
		require.NoError(t, l.create("db2"))
	}

	setupWithData := func(l *LevelDB) (map[string]*types.ValueWithMetadata, map[string]*types.ValueWithMetadata) {
		db1valAndMetadata1 := &types.ValueWithMetadata{
			Value: []byte("db1-value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
				AccessControl: &types.AccessControl{
					ReadUsers: map[string]bool{
						"user1": true,
					},
					ReadWriteUsers: map[string]bool{
						"user2": true,
					},
				},
			},
		}
		db1valAndMetadata2 := &types.ValueWithMetadata{
			Value: []byte("db1-value2"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    2,
				},
			},
		}
		db2valAndMetadata1 := &types.ValueWithMetadata{
			Value: []byte("db2-value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    3,
				},
				AccessControl: &types.AccessControl{
					ReadUsers: map[string]bool{
						"user1": true,
					},
					ReadWriteUsers: map[string]bool{
						"user2": true,
					},
				},
			},
		}
		db2valAndMetadata2 := &types.ValueWithMetadata{
			Value: []byte("db2-value2"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    4,
				},
			},
		}
		dbsUpdates := map[string]*worldstate.DBUpdates{
			"db1": {
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
			"db2": {
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
		require.NoError(t, l.Commit(dbsUpdates, 1))

		db1KVs := map[string]*types.ValueWithMetadata{
			"db1-key1": db1valAndMetadata1,
			"db1-key2": db1valAndMetadata2,
		}
		db2KVs := map[string]*types.ValueWithMetadata{
			"db2-key1": db2valAndMetadata1,
			"db2-key2": db2valAndMetadata2,
		}

		testCacheEntriesCount(t, l.cache, 0)
		return db1KVs, db2KVs
	}

	t.Run("Get(), GetVersion(), Has(), and GetIterator() on empty databases", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		setupWithNoData(l)

		testCacheEntriesCount(t, l.cache, 0)

		for _, db := range []string{"db1", "db2"} {
			for _, key := range []string{"key1", "key2"} {
				val, metadata, err := l.Get(db, db+"-"+key)
				require.NoError(t, err)
				require.Nil(t, val)
				require.Nil(t, metadata)

				ver, err := l.GetVersion(db, db+"-"+key)
				require.NoError(t, err)
				require.Nil(t, ver)

				exist, err := l.Has(db, db+"-"+key)
				require.NoError(t, err)
				require.False(t, exist)
			}

			iter, err := l.GetIterator(db, "key0", "key3")
			defer iter.Release()
			require.NoError(t, err)
			require.False(t, iter.Next())
			require.NoError(t, iter.Error())
		}

		// Even when database is empty, cache is filled with reads of non-existing
		// keys with a nil value/metadata. This does not waste the cache memory and
		// instead, it would help the mvcc validation to avoid reads from the disk
		testCacheEntriesCount(t, l.cache, 4)

		for _, db := range []string{"db1", "db2"} {
			for _, key := range []string{"key1", "key2"} {
				valWithMetadata, inCache := unmarshaler(t)(l.cache.getState(db, db+"-"+key))
				require.True(t, inCache)
				require.NotNil(t, valWithMetadata)
				require.Nil(t, valWithMetadata.Value)
				require.Nil(t, valWithMetadata.Metadata)
			}
		}
	})

	// Scenario-2: For both databases (db1, db2), create
	// two keys per database (db1-key1, db1-key2 for db1) and
	// (db2-key1, db2-key2 for db2) and commit them.
	t.Run("Get(), GetVersion(), and Has() on non-empty databases", func(t *testing.T) {
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
				&types.ValueWithMetadata{Value: val, Metadata: metadata},
			))

			ver, err := l.GetVersion("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata().GetVersion(), ver))

			acl, err := l.GetACL("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata().GetAccessControl(), acl))

			exist, err := l.Has("db1", key)
			require.NoError(t, err)
			require.True(t, exist)
		}

		// db1-key1, db1-key2 should exist in the cache
		testCacheEntriesCount(t, l.cache, 2)
		for key, expectedValAndMetadata := range db1KVs {
			valWithMetadata, inCache := unmarshaler(t)(l.cache.getState("db1", key))
			require.True(t, inCache)
			require.True(t, proto.Equal(expectedValAndMetadata, valWithMetadata))
		}

		for key, expectedValAndMetadata := range db2KVs {
			val, metadata, err := l.Get("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(
				expectedValAndMetadata,
				&types.ValueWithMetadata{Value: val, Metadata: metadata},
			))

			ver, err := l.GetVersion("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata().GetVersion(), ver))

			acl, err := l.GetACL("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata().GetAccessControl(), acl))

			exist, err := l.Has("db2", key)
			require.NoError(t, err)
			require.True(t, exist)
		}

		// db2-key1, db2-key2 should exist in the cache
		testCacheEntriesCount(t, l.cache, 4)
		for key, expectedValAndMetadata := range db1KVs {
			valWithMetadata, inCache := unmarshaler(t)(l.cache.getState("db1", key))
			require.True(t, inCache)
			require.True(t, proto.Equal(expectedValAndMetadata, valWithMetadata))
		}
		for key, expectedValAndMetadata := range db2KVs {
			valWithMetadata, inCache := unmarshaler(t)(l.cache.getState("db2", key))
			require.True(t, inCache)
			require.True(t, proto.Equal(expectedValAndMetadata, valWithMetadata))
		}
	})

	t.Run("GetIterator() on non-empty databases", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		defer env.cleanup()
		l := env.l
		db1KVs, db2KVs := setupWithData(l)

		tests := []struct {
			name                 string
			dbName               string
			startKey             string
			endKey               string
			expectedValueAndMeta map[string]*types.ValueWithMetadata
		}{
			{
				name:     "end key is exclusive and both keys exist",
				dbName:   "db1",
				startKey: "db1-key1",
				endKey:   "db1-key2",
				expectedValueAndMeta: map[string]*types.ValueWithMetadata{
					"db1-key1": db1KVs["db1-key1"],
				},
			},
			{
				name:                 "end key does not exist",
				dbName:               "db1",
				startKey:             "db1-key1",
				endKey:               "db1-key3",
				expectedValueAndMeta: db1KVs,
			},
			{
				name:                 "start key does not exist",
				dbName:               "db1",
				startKey:             "db1-key0",
				endKey:               "db1-key3",
				expectedValueAndMeta: db1KVs,
			},
			{
				name:                 "start key is beyond the range",
				dbName:               "db1",
				startKey:             "db1-key3",
				endKey:               "",
				expectedValueAndMeta: nil,
			},
			{
				name:                 "end key is before the beginning",
				dbName:               "db1",
				startKey:             "",
				endKey:               "db1-key0",
				expectedValueAndMeta: nil,
			},
			{
				name:                 "[begin:]",
				dbName:               "db2",
				startKey:             "db2-key1",
				endKey:               "",
				expectedValueAndMeta: db2KVs,
			},
			{
				name:     "[:end]",
				dbName:   "db2",
				startKey: "",
				endKey:   "db2-key2",
				expectedValueAndMeta: map[string]*types.ValueWithMetadata{
					"db2-key1": db2KVs["db2-key1"],
				},
			},
			{
				name:                 "[:]",
				dbName:               "db2",
				startKey:             "",
				endKey:               "",
				expectedValueAndMeta: db2KVs,
			},
		}

		for _, tt := range tests {
			iter1, err := l.GetIterator(tt.dbName, tt.startKey, tt.endKey)
			defer iter1.Release()
			require.NoError(t, err)
			kvs := make(map[string]*types.ValueWithMetadata)

			for iter1.Next() {
				require.NoError(t, err)
				k := string(iter1.Key())
				v := &types.ValueWithMetadata{}
				require.NoError(t, proto.Unmarshal(iter1.Value(), v))
				kvs[k] = v
			}

			require.Equal(t, len(tt.expectedValueAndMeta), len(kvs))
			for expectedKey, expectedValueAndMeta := range tt.expectedValueAndMeta {
				valueAndMeta := kvs[expectedKey]
				require.Equal(t, expectedValueAndMeta.Value, valueAndMeta.Value)
				require.True(t, proto.Equal(expectedValueAndMeta.Metadata, valueAndMeta.Metadata))
			}
		}

		iter2, err := l.GetIterator("db3", "", "")
		require.Nil(t, iter2)
		require.EqualError(t, err, "database db3 does not exist")

		iter3, err := l.GetIterator("db2", "", "")
		iter3.Release()
		iter3.Next()
		require.EqualError(t, iter3.Error(), "leveldb: iterator released")

		iter4, err := l.GetIterator("db2", "", "")
		defer iter4.Release()
		require.NoError(t, err)
		// let's skip "db1-key1" by seeking to "db2-key10"
		// as the next greater value to "db2-key10" is "db2-key2",
		// seek would return true
		require.True(t, iter4.Seek([]byte("db2-key2")))
		require.Equal(t, []byte("db2-key2"), iter4.Key())
		require.False(t, iter4.Next())

		iter5, err := l.GetIterator("db2", "", "")
		defer iter5.Release()
		require.NoError(t, err)
		// let's skip to the end. As there is no value equal to or
		// greater than "db2-key3", seek would return a false
		require.False(t, iter4.Seek([]byte("db2-key3")))
		require.False(t, iter4.Next())

		testCacheEntriesCount(t, l.cache, 0)
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

		for key, expectedValAndMetadata := range db1KVs {
			val, metadata, err := l.Get("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(
				expectedValAndMetadata,
				&types.ValueWithMetadata{Value: val, Metadata: metadata},
			))
		}

		testCacheEntriesCount(t, l.cache, 2)
		for key, expectedValAndMetadata := range db1KVs {
			valWithMetadata, inCache := unmarshaler(t)(l.cache.getState("db1", key))
			require.True(t, inCache)
			require.True(t, proto.Equal(expectedValAndMetadata, valWithMetadata))
		}

		for key, expectedValAndMetadata := range db2KVs {
			val, metadata, err := l.Get("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(
				expectedValAndMetadata,
				&types.ValueWithMetadata{Value: val, Metadata: metadata},
			))
		}

		testCacheEntriesCount(t, l.cache, 4)
		for key, expectedValAndMetadata := range db1KVs {
			valWithMetadata, inCache := unmarshaler(t)(l.cache.getState("db1", key))
			require.True(t, inCache)
			require.True(t, proto.Equal(expectedValAndMetadata, valWithMetadata))
		}
		for key, expectedValAndMetadata := range db2KVs {
			valWithMetadata, inCache := unmarshaler(t)(l.cache.getState("db2", key))
			require.True(t, inCache)
			require.True(t, proto.Equal(expectedValAndMetadata, valWithMetadata))
		}

		db1valAndMetadata1New := &types.ValueWithMetadata{
			Value: []byte("db1-value1-new"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 2,
					TxNum:    1,
				},
				AccessControl: &types.AccessControl{
					ReadUsers: map[string]bool{
						"user3": true,
					},
					ReadWriteUsers: map[string]bool{
						"user4": true,
					},
				},
			},
		}
		db2valAndMetadata1New := &types.ValueWithMetadata{
			Value:    []byte("db2-value1-new"),
			Metadata: &types.Metadata{Version: &types.Version{BlockNum: 2, TxNum: 2}},
		}
		dbsUpdates := map[string]*worldstate.DBUpdates{
			"db1": {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      "db1-key1",
						Value:    db1valAndMetadata1New.Value,
						Metadata: db1valAndMetadata1New.Metadata,
					},
				},
				Deletes: []string{"db1-key2"},
			},
			"db2": {
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

		require.NoError(t, l.Commit(dbsUpdates, 2))

		testCacheEntriesCount(t, l.cache, 2)
		valWithMetadata, inCache := unmarshaler(t)(l.cache.getState("db1", "db1-key1"))
		require.True(t, inCache)
		require.True(t, proto.Equal(db1valAndMetadata1New, valWithMetadata))

		valWithMetadata, inCache = unmarshaler(t)(l.cache.getState("db1", "db1-key2"))
		require.False(t, inCache)
		require.Nil(t, valWithMetadata)

		valWithMetadata, inCache = unmarshaler(t)(l.cache.getState("db2", "db2-key1"))
		require.True(t, inCache)
		require.True(t, proto.Equal(db2valAndMetadata1New, valWithMetadata))

		valWithMetadata, inCache = unmarshaler(t)(l.cache.getState("db2", "db2-key2"))
		require.False(t, inCache)
		require.Nil(t, valWithMetadata)

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

			acl, err := l.GetACL("db1", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata().GetAccessControl(), acl))
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

			acl, err := l.GetACL("db2", key)
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata().GetAccessControl(), acl))
		}

		testCacheEntriesCount(t, l.cache, 4)
		for key, expectedValAndMetadata := range db1KVs {
			valWithMetadata, inCache := unmarshaler(t)(l.cache.getState("db1", key))
			require.True(t, inCache)
			require.Equal(t, expectedValAndMetadata.GetValue(), valWithMetadata.GetValue())
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata(), valWithMetadata.GetMetadata()))
		}
		for key, expectedValAndMetadata := range db1KVs {
			valWithMetadata, inCache := unmarshaler(t)(l.cache.getState("db1", key))
			require.True(t, inCache)
			require.Equal(t, expectedValAndMetadata.GetValue(), valWithMetadata.GetValue())
			require.True(t, proto.Equal(expectedValAndMetadata.GetMetadata(), valWithMetadata.GetMetadata()))
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
		expectedIndex          map[string][]byte
	}{
		{
			name:         "only create DBs",
			preCreateDBs: nil,
			updates: &worldstate.DBUpdates{
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
				Deletes: []string{"db1", "db2"},
			},
			expectedDBsAfterCommit: nil,
		},
		{
			name:         "create and delete DBs",
			preCreateDBs: []string{"db3", "db4"},
			updates: &worldstate.DBUpdates{
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   "db1",
						Value: []byte("index-db1"),
					},
					{
						Key:   "db2",
						Value: []byte("index-db2"),
					},
				},
				Deletes: []string{"db3", "db4"},
			},
			expectedDBsAfterCommit: []string{"db1", "db2"},
			expectedIndex: map[string][]byte{
				"db1": []byte("index-db1"),
				"db2": []byte("index-db2"),
			},
		},
		{
			name:         "create already existing DBs and delete non-existing DBs -- applicable during node failure and reply of block",
			preCreateDBs: []string{"db1", "db3"},
			updates: &worldstate.DBUpdates{
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   "db1",
						Value: []byte("index-db1"),
					},
					{
						Key:   "db2",
						Value: []byte("index-db2"),
					},
				},
				Deletes: []string{"db3", "db4"},
			},
			expectedDBsAfterCommit: []string{"db1", "db2"},
			expectedIndex: map[string][]byte{
				"db1": []byte("index-db1"),
				"db2": []byte("index-db2"),
			},
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
					map[string]*worldstate.DBUpdates{
						worldstate.DatabasesDBName: tt.updates,
					},
					1,
				),
			)

			require.ElementsMatch(t, tt.expectedDBsAfterCommit, l.ListDBs())

			for dbName, expectedIndex := range tt.expectedIndex {
				index, _, err := l.GetIndexDefinition(dbName)
				require.NoError(t, err)
				require.Equal(t, expectedIndex, index)
			}
		})
	}
}

func TestGetConfig(t *testing.T) {
	t.Parallel()

	t.Run("commit and query cluster config", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup()

		clusterConfig := &types.ClusterConfig{
			Nodes: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "127.0.0.1",
					Port:        1234,
					Certificate: []byte("cert"),
				},
			},
			Admins: []*types.Admin{
				{
					Id:          "admin",
					Certificate: []byte("cert"),
				},
			},
			CertAuthConfig: &types.CAConfig{
				Roots: [][]byte{[]byte("cert")},
			},
		}

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
		}
		require.NoError(t, env.l.Commit(dbUpdates, 1))

		actualConfig, actualMetadata, err := env.l.GetConfig()
		require.NoError(t, err)
		require.True(t, proto.Equal(clusterConfig, actualConfig))
		require.True(t, proto.Equal(metadata, actualMetadata))
	})

	t.Run("querying config returns error", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup()

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
		}
		require.NoError(t, env.l.Commit(dbUpdates, 1))

		actualConfig, actualMetadata, err := env.l.GetConfig()
		require.Contains(t, err.Error(), "error while unmarshaling committed cluster configuration")
		require.Nil(t, actualConfig)
		require.Nil(t, actualMetadata)
	})
}

func TestHeight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		blockNumber    uint64
		dbsUpdates     map[string]*worldstate.DBUpdates
		expectedHeight uint64
	}{
		{
			name:           "block 1 with empty updates",
			blockNumber:    1,
			dbsUpdates:     nil,
			expectedHeight: 1,
		},
		{
			name:        "block 10 with non-empty updates",
			blockNumber: 10,
			dbsUpdates: map[string]*worldstate.DBUpdates{
				worldstate.DefaultDBName: {
					Writes:  []*worldstate.KVWithMetadata{},
					Deletes: []string{"key1"},
				},
			},
			expectedHeight: 10,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			defer env.cleanup()

			height, err := env.l.Height()
			require.NoError(t, err)
			require.Equal(t, uint64(0), height)

			require.NoError(t, env.l.Commit(tt.dbsUpdates, tt.blockNumber))

			height, err = env.l.Height()
			require.NoError(t, err)
			require.Equal(t, tt.expectedHeight, height)

			require.NoError(t, env.l.Commit(nil, tt.blockNumber+1))

			height, err = env.l.Height()
			require.NoError(t, err)
			require.Equal(t, tt.expectedHeight+1, height)
		})
	}
}

func testCacheEntriesCount(t *testing.T, c *cache, expectedEntriesCount uint64) {
	s := &fastcache.Stats{}
	c.dataCache.UpdateStats(s)
	require.Equal(t, expectedEntriesCount, s.EntriesCount)
}
