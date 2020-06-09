package server

import "github.ibm.com/blockchaindb/server/api"

type transactionServer struct {
	api.UnimplementedTransactionSvcServer
}

func NewTransactionServer() (*transactionServer, error) {
	return &transactionServer{}, nil
}
