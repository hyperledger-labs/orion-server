package server

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/api"
	"google.golang.org/grpc"
)

func TestStart(t *testing.T) {
	Start()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())
	conn, err := grpc.Dial("localhost:6001", opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	queryClient := api.NewQueryClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	val, err := queryClient.Get(ctx, &api.DataQuery{})
	require.Nil(t, val)
	require.Error(t, err)
	require.Contains(t, err.Error(), "method Get not implemented")
}
