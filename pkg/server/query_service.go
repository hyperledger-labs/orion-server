package server

import (
	"context"

	"github.ibm.com/blockchaindb/server/api"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type queryServer struct {
	api.UnimplementedQueryServer
}

func (qs *queryServer) Get(ctx context.Context, req *api.DataQuery) (*api.Value, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}

func newQueryServer() (*queryServer, error) {
	return &queryServer{}, nil
}
