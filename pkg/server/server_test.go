package server

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/server/mock"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

func TestMain(m *testing.M) {
	path, err := filepath.Abs("../../config")
	if err != nil {
		log.Fatalf("Error while constructing absolute path from the default config, %v", err)
	}
	if err := os.Setenv(config.PathEnv, path); err != nil {
		log.Fatalf(" Error while setting the config path to %s, %v", config.PathEnv, err)
	}

	if err := config.Init(); err != nil {
		log.Fatalf("Error while initializing the configuration, %v", err)
	}
	os.Exit(m.Run())
}

type serverTestEnv struct {
	client  *mock.Client
	cleanup func(t *testing.T)
}

func newServerTestEnv(t *testing.T) *serverTestEnv {
	env := &serverTestEnv{}

	go Start()
	dbConf := config.Database()
	env.cleanup = func(t *testing.T) {
		if err := os.RemoveAll(dbConf.LedgerDirectory); err != nil {
			t.Errorf("Warning: failed to remove %s: %v\n", dbConf.LedgerDirectory, err)
		}
		if err := Stop(); err != nil {
			t.Errorf("Warning: failed to stop the server: %v\n", err)
		}
	}

	var err error
	createClient := func() bool {
		env.client, err = mock.NewRESTClient("http://127.0.0.1:6001")
		if err == nil && env.client != nil {
			return true
		}
		return false
	}
	require.Eventually(t, createClient, time.Second*2, time.Millisecond*100)

	return env
}

func TestStart(t *testing.T) {
	t.Run("server-starts-successfully", func(t *testing.T) {
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		valEnv, err := env.client.GetState(
			ctx,
			&types.GetStateQueryEnvelope{
				Payload: &types.GetStateQuery{
					UserID: "testUser",
					DBName: "db1",
					Key:    "key1",
				},
				Signature: []byte("hello"),
			},
		)
		require.Nil(t, valEnv)
		require.Contains(t, err.Error(), "database db1 does not exist")
	})
}

func TestHandleStatusQuery(t *testing.T) {
	t.Run("GetStatus-Returns-True", func(t *testing.T) {
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		req := &types.GetStatusQueryEnvelope{
			Payload: &types.GetStatusQuery{
				UserID: "testUser",
				DBName: worldstate.DefaultDBName,
			},
			Signature: []byte("signature"),
		}
		resp, err := env.client.GetStatus(context.Background(), req)
		require.NoError(t, err)
		require.True(t, resp.Payload.Exist)
	})

	t.Run("GetStatus-Returns-Error", func(t *testing.T) {
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		testCases := []struct {
			request       *types.GetStatusQueryEnvelope
			expectedError string
		}{
			{
				request: &types.GetStatusQueryEnvelope{
					Payload: &types.GetStatusQuery{
						UserID: "testUser",
						DBName: worldstate.DefaultDBName,
					},
				},
				expectedError: "X-BLockchain-DB-Signature is not set in the http request header",
			},
			{
				request: &types.GetStatusQueryEnvelope{
					Payload: &types.GetStatusQuery{
						UserID: "",
						DBName: worldstate.DefaultDBName,
					},
					Signature: []byte("signature"),
				},
				expectedError: "X-BLockchain-DB-User-ID is not set in the http request header",
			},
		}

		for _, testCase := range testCases {
			resp, err := env.client.GetStatus(context.Background(), testCase.request)
			require.Contains(t, err.Error(), testCase.expectedError)
			require.Nil(t, resp)
		}
	})
}

func TestHandleStateQuery(t *testing.T) {
	t.Run("GetState-Returns-State", func(t *testing.T) {
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		val1 := &types.Value{
			Value: []byte("Value1"),
			Metadata: &types.Metadata{
				Version: &types.Version{
					BlockNum: 1,
					TxNum:    1,
				},
			},
		}
		dbsUpdates := []*worldstate.DBUpdates{
			{
				DBName: worldstate.DefaultDBName,
				Writes: []*worldstate.KV{
					{
						Key:   "key1",
						Value: val1,
					},
				},
			},
		}
		require.NoError(t, s.dbServ.db.Commit(dbsUpdates))

		testCases := []struct {
			key         string
			expectedVal *types.Value
		}{
			{
				key:         "key1",
				expectedVal: val1,
			},
			{
				key:         "key2",
				expectedVal: nil,
			},
		}

		for _, testCase := range testCases {
			req := &types.GetStateQueryEnvelope{
				Payload: &types.GetStateQuery{
					UserID: "testUser",
					DBName: worldstate.DefaultDBName,
					Key:    testCase.key,
				},
				Signature: []byte("signature"),
			}
			resp, err := env.client.GetState(context.Background(), req)
			require.NoError(t, err)
			require.True(t, proto.Equal(resp.Payload.Value, testCase.expectedVal))
		}
	})

	t.Run("GetState-Returns-Error", func(t *testing.T) {
		env := newServerTestEnv(t)
		defer env.cleanup(t)

		testCases := []struct {
			request       *types.GetStateQueryEnvelope
			expectedError string
		}{
			{
				request: &types.GetStateQueryEnvelope{
					Payload: &types.GetStateQuery{
						UserID: "testUser",
						DBName: worldstate.DefaultDBName,
						Key:    "key1",
					},
				},
				expectedError: "X-BLockchain-DB-Signature is not set in the http request header",
			},
			{
				request: &types.GetStateQueryEnvelope{
					Payload: &types.GetStateQuery{
						UserID: "",
						DBName: worldstate.DefaultDBName,
						Key:    "key1",
					},
					Signature: []byte("signature"),
				},
				expectedError: "X-BLockchain-DB-User-ID is not set in the http request header",
			},
		}

		for _, testCase := range testCases {
			resp, err := env.client.GetState(context.Background(), testCase.request)
			require.Contains(t, err.Error(), testCase.expectedError)
			require.Nil(t, resp)
		}
	})
}
