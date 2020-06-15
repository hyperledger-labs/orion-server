package server

import (
	"context"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

func TestQueryService(t *testing.T) {
	qs, err := newQueryServer()
	require.NoError(t, err)
	require.NotNil(t, qs)
	require.NoError(t, qs.db.Create("test-db"))
	dbConf := config.Database()
	defer os.RemoveAll(dbConf.LedgerDirectory)

	val1 := &api.Value{
		Value: []byte("value1"),
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 1,
				TxNum:    1,
			},
		},
	}
	val2 := &api.Value{
		Value: []byte("value2"),
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 1,
				TxNum:    2,
			},
		},
	}
	dbsUpdates := []*worldstate.DBUpdates{
		{
			DBName: "test-db",
			Writes: []*worldstate.KV{
				{
					Key:   "key1",
					Value: val1,
				},
				{
					Key:   "key2",
					Value: val2,
				},
			},
		},
	}
	require.NoError(t, qs.db.Commit(dbsUpdates))

	t.Run("GetStatus", func(t *testing.T) {
		t.Parallel()
		req := &api.DB{
			Name: "test-db",
		}
		status, err := qs.GetStatus(context.TODO(), req)
		require.NoError(t, err)
		expectedStatus := &api.DBStatus{
			Exist: true,
		}
		require.True(t, proto.Equal(expectedStatus, status))

		req.Name = ""
		status, err = qs.GetStatus(context.TODO(), req)
		require.NoError(t, err)
		expectedStatus.Exist = false
		require.True(t, proto.Equal(expectedStatus, status))

		status, err = qs.GetStatus(context.TODO(), nil)
		require.EqualError(t, err, "db request is nil")
		require.Nil(t, status)
	})

	t.Run("GetState", func(t *testing.T) {
		t.Parallel()
		req := &api.DataQuery{
			Header: &api.QueryHeader{
				DBName: "test-db",
			},
			Key: "key1",
		}
		val, err := qs.GetState(context.TODO(), req)
		require.NoError(t, err)
		require.True(t, proto.Equal(val1, val))

		req.Key = "key3"
		val, err = qs.GetState(context.TODO(), req)
		require.NoError(t, err)
		require.Nil(t, val)

		req.Header = nil
		val, err = qs.GetState(context.TODO(), req)
		require.EqualError(t, err, "header in DataQuery is nil [key:\"key3\" ]")
		require.Nil(t, val)

		val, err = qs.GetState(context.TODO(), nil)
		require.EqualError(t, err, "dataQuery request is nil")
		require.Nil(t, val)
	})
}
