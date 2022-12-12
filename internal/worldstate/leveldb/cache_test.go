package leveldb

import (
	"testing"

	"github.com/VictoriaMetrics/fastcache"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestCache(t *testing.T) {
	cache := newCache(10)

	db1Key1Value2 := &types.ValueWithMetadata{
		Value: []byte("value2"),
		Metadata: &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    1,
			},
		},
	}

	db2Key1Value1 := &types.ValueWithMetadata{
		Value: []byte("value1"),
		Metadata: &types.Metadata{
			Version: &types.Version{
				BlockNum: 1,
				TxNum:    1,
			},
		},
	}

	t.Run("check basic storage and retrieval", func(t *testing.T) {
		// db1, key1 does not exist
		v, err := cache.getState("db1", "key1")
		require.NoError(t, err)
		require.Nil(t, v)

		s := &fastcache.Stats{}
		cache.dataCache.UpdateStats(s)
		require.Equal(t, uint64(0), s.EntriesCount)

		// store db1, key1
		db1Key1Value1 := &types.ValueWithMetadata{
			Value: []byte("value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
			},
		}
		valBytes, err := proto.Marshal(db1Key1Value1)
		require.NoError(t, err)
		cache.putState("db1", "key1", valBytes)

		// db1, key1 should exists
		actualKey1Value1, err := cache.getState("db1", "key1")
		require.NoError(t, err)
		require.True(t, proto.Equal(db1Key1Value1, actualKey1Value1))

		cache.dataCache.UpdateStats(s)
		require.Equal(t, uint64(1), s.EntriesCount)
	})

	t.Run("check whether update works", func(t *testing.T) {
		// update key1's value
		valBytes, err := proto.Marshal(db1Key1Value2)
		require.NoError(t, err)
		cache.putState("db1", "key1", valBytes)

		// db1, key1 should have the updated value
		actualKey1Value2, err := cache.getState("db1", "key1")
		require.NoError(t, err)
		require.True(t, proto.Equal(db1Key1Value2, actualKey1Value2))

		s := &fastcache.Stats{}
		cache.dataCache.UpdateStats(s)
		require.Equal(t, uint64(1), s.EntriesCount)
	})

	t.Run("check updates of existing key only", func(t *testing.T) {
		// store db2, key1 if exists already (but it does not)
		valBytes, err := proto.Marshal(db2Key1Value1)
		require.NoError(t, err)
		cache.putStateIfExist("db2", "key1", valBytes)

		v, err := cache.getState("db2", "key1")
		require.NoError(t, err)
		require.Nil(t, v)

		s := &fastcache.Stats{}
		cache.dataCache.UpdateStats(s)
		require.Equal(t, uint64(1), s.EntriesCount)
	})

	t.Run("store same key in two different databases", func(t *testing.T) {
		valBytes, err := proto.Marshal(db2Key1Value1)
		require.NoError(t, err)
		cache.putState("db2", "key1", valBytes)

		// both db1, key1 and db2, key1 should exist
		actualDB1Key1Value2, err := cache.getState("db1", "key1")
		require.NoError(t, err)
		require.True(t, proto.Equal(db1Key1Value2, actualDB1Key1Value2))

		actualDB2Key1Value1, err := cache.getState("db2", "key1")
		require.NoError(t, err)
		require.True(t, proto.Equal(db2Key1Value1, actualDB2Key1Value1))

		s := &fastcache.Stats{}
		cache.dataCache.UpdateStats(s)
		require.Equal(t, uint64(2), s.EntriesCount)
	})

	t.Run("ensure delete works", func(t *testing.T) {
		cache.delState("db1", "key1")

		s := &fastcache.Stats{}
		cache.dataCache.UpdateStats(s)
		require.Equal(t, uint64(1), s.EntriesCount)

		cache.delState("db2", "key1")
		s = &fastcache.Stats{}
		cache.dataCache.UpdateStats(s)
		require.Equal(t, uint64(0), s.EntriesCount)
	})
}
