package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/server/mock"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

var httpServer *http.Server

func TestCreateRESTService(t *testing.T) {
	rs, err := NewDBServer()
	require.NoError(t, err)
	require.NotNil(t, rs)
	s = &http.Server{
		Addr:    fmt.Sprintf("localhost:%d", 6001),
		Handler: rs.router,
	}
	go s.ListenAndServe()
	time.Sleep(time.Millisecond * 10)
	defer s.Close()
	rc, _ := mock.NewRESTClient("http://localhost:6001")
	req := &api.GetStatusQueryEnvelope{
		Payload: &api.GetStatusQuery{
			UserID: "testUser",
			DBName: "db1",
		},
		Signature: []byte("signature"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := rc.GetStatus(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.False(t, resp.Payload.Exist)
}

func Test_handleStatusQuery(t *testing.T) {
	rs, err := NewDBServer()
	require.NoError(t, err)
	require.NotNil(t, rs)
	s = &http.Server{
		Addr:    fmt.Sprintf("localhost:%d", 6001),
		Handler: rs.router,
	}
	go s.ListenAndServe()
	time.Sleep(time.Millisecond * 10)
	defer s.Close()
	rs.qs.db.Create("db1")
	dbConf := config.Database()
	defer os.RemoveAll(dbConf.LedgerDirectory)
	rc, _ := mock.NewRESTClient("http://localhost:6001")
	req := &api.GetStatusQueryEnvelope{
		Payload: &api.GetStatusQuery{
			UserID: "testUser",
			DBName: "db1",
		},
		Signature: []byte("signature"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := rc.GetStatus(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, resp.Payload.Exist)

	req.Signature = nil
	ctx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	resp, err = rc.GetStatus(ctx, req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty signature")

	req.Payload.UserID = ""
	req.Signature = []byte("signature")
	ctx, cancel3 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel3()
	resp, err = rc.GetStatus(ctx, req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty user")
}

func Test_handleStateQuery(t *testing.T) {
	rs, err := NewDBServer()
	require.NoError(t, err)
	require.NotNil(t, rs)
	s = &http.Server{
		Addr:    fmt.Sprintf("localhost:%d", 6001),
		Handler: rs.router,
	}
	go s.ListenAndServe()
	time.Sleep(time.Millisecond * 10)
	defer s.Close()
	rs.qs.db.Create("db1")
	dbConf := config.Database()
	defer os.RemoveAll(dbConf.LedgerDirectory)

	val1 := &api.Value{
		Value: []byte("Value1"),
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 1,
				TxNum:    1,
			},
		},
	}
	val2 := &api.Value{
		Value: []byte("Value2"),
		Metadata: &api.Metadata{
			Version: &api.Version{
				BlockNum: 1,
				TxNum:    2,
			},
		},
	}
	dbsUpdates := []*worldstate.DBUpdates{
		{
			DBName: "db1",
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
	require.NoError(t, rs.qs.db.Commit(dbsUpdates))
	rc, _ := mock.NewRESTClient("http://localhost:6001")
	req := &api.GetStateQueryEnvelope{
		Payload: &api.GetStateQuery{
			UserID: "testUser",
			DBName: "db1",
			Key:    "key1",
		},
		Signature: []byte("signature"),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := rc.GetState(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, proto.Equal(resp.Payload.Value, val1))

	req.Payload.Key = "key2"
	ctx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	resp, err = rc.GetState(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.True(t, proto.Equal(resp.Payload.Value, val2))

	req.Payload.Key = "key3"
	ctx, cancel3 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel3()
	resp, err = rc.GetState(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Nil(t, resp.Payload.Value)

	req.Payload.UserID = ""
	ctx, cancel4 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel4()
	resp, err = rc.GetState(ctx, req)
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty user")

}
