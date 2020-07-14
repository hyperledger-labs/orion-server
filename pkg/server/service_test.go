package server

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/server/mock"
)

func TestStart(t *testing.T) {
	Start()
	time.Sleep(time.Millisecond * 10)
	defer Stop()
	client, err := mock.NewRESTClient("http://localhost:6001")
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	valEnv, err := client.GetState(
		ctx,
		&api.GetStateQueryEnvelope{
			Payload: &api.GetStateQuery{
				UserID: []byte("testUser"),
				DBName: "db1",
				Key:    "key1",
			},
			Signature: []byte("hello"),
		},
	)
	require.Nil(t, valEnv)
	require.Error(t, err)
	require.Contains(t, err.Error(), "database db1 does not exist")
}
