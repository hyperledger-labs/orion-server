package leveldb

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestSnapshots(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	db1 := "db1"
	db2 := worldstate.DatabasesDBName
	require.NoError(t, env.l.create(db1))
	require.NoError(t, env.l.create(db2))

	s0, err := env.l.GetDBsSnapshot([]string{db1, db2})
	require.NoError(t, err)
	defer s0.Release()

	// as db is empty, both snapshot and real db should
	// return no kv pairs
	verifyEmptiness(t, env.l)
	verifyEmptiness(t, s0)

	// write some kv pairs to the real db. As a result,
	// the real db should return some kv pairs while the
	// snapshot should not
	writeForSnapshotTest(t, env.l)

	verifyNonEmptiness(t, env.l)
	verifyEmptiness(t, s0)

	// create a new snapshot on db which is not empty.
	// as a result, the new snapshot s1 should return
	// some kv pairs while the old snapshot s0 should
	// not return any kv pairs
	s1, err := env.l.GetDBsSnapshot([]string{db1, db2})
	require.NoError(t, err)
	defer s1.Release()

	verifyNonEmptiness(t, s1)
	verifyEmptiness(t, s0)

	// remove all kv pairs from the real db. As a result,
	// the real db should not return any kv pairs while
	// the snapshot s1 should return the deleted kv pairs
	deleteForSnapshotTest(t, env.l)

	verifyEmptiness(t, env.l)
	verifyNonEmptiness(t, s1)
	verifyEmptiness(t, s0)

	// acquire a new snapshot s2. The snapshot s2 should
	// not return any kv pairs while s1 should
	s2, err := env.l.GetDBsSnapshot([]string{db1, db2})
	require.NoError(t, err)
	defer s2.Release()

	verifyEmptiness(t, s2)
	verifyNonEmptiness(t, s1)
	verifyEmptiness(t, s0)
}

func verifyEmptiness(t *testing.T, db snapshotTestAPIs) {
	v, m, err := db.Get("db1", "key1")
	require.NoError(t, err)
	require.Nil(t, v)
	require.Nil(t, m)

	v, m, err = db.GetIndexDefinition("db1")
	require.NoError(t, err)
	require.Nil(t, v)
	require.Nil(t, m)

	itr, err := db.GetIterator("db1", "key1", "key4")
	require.NoError(t, err)
	defer itr.Release()
	require.False(t, itr.Next())
	require.NoError(t, itr.Error())
}

func verifyNonEmptiness(t *testing.T, db snapshotTestAPIs) {
	expectedMeta := &types.Metadata{
		Version: &types.Version{
			BlockNum: 1,
			TxNum:    1,
		},
	}

	v, m, err := db.Get("db1", "key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), v)
	require.True(t, proto.Equal(expectedMeta, m))

	v, m, err = db.GetIndexDefinition("db1")
	require.NoError(t, err)
	require.Equal(t, []byte("index def"), v)
	require.True(t, proto.Equal(expectedMeta, m))

	itr, err := db.GetIterator("db1", "key1", "key4")
	require.NoError(t, err)
	defer itr.Release()

	expectedKeys := []string{"key1", "key2", "key3"}
	keys := []string{}
	for itr.Next() {
		keys = append(keys, string(itr.Key()))
	}
	require.NoError(t, itr.Error())
	require.Equal(t, expectedKeys, keys)
}

type snapshotTestAPIs interface {
	Get(dbName, key string) ([]byte, *types.Metadata, error)
	GetIndexDefinition(dbName string) ([]byte, *types.Metadata, error)
	GetIterator(dbName, startKey, endKey string) (worldstate.Iterator, error)
}

func writeForSnapshotTest(t *testing.T, l *LevelDB) {
	u := map[string]*worldstate.DBUpdates{
		"db1": {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   "key1",
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
				{
					Key:   "key2",
					Value: []byte("value2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
				{
					Key:   "key3",
					Value: []byte("value3"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
			},
		},
		worldstate.DatabasesDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   "db1",
					Value: []byte("index def"),
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
	require.NoError(t, l.Commit(u, 1))
}

func deleteForSnapshotTest(t *testing.T, l *LevelDB) {
	u := map[string]*worldstate.DBUpdates{
		"db1": {
			Deletes: []string{"key1", "key2", "key3"},
		},
	}
	require.NoError(t, l.Commit(u, 2))

	wdb, ok := l.getDB(worldstate.DatabasesDBName)
	require.True(t, ok)
	require.NoError(t, wdb.file.Delete([]byte("db1"), &opt.WriteOptions{Sync: true}))
	l.cache.delState(worldstate.DatabasesDBName, "db1")
}
