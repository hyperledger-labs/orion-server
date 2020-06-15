package server

import (
	"fmt"
	"log"
	"net"

	"github.ibm.com/blockchaindb/server/api"
	"google.golang.org/grpc"
)

func Start() {
	qs, err := newQueryServer()
	if err != nil {
		log.Fatalf("Failed to initiate query server %v", err)
	}
	ts, err := newTransactionServer()
	if err != nil {
		log.Fatalf("Failed to initiate transaction server %v", err)
	}

	log.Printf("starting server localhost:%d", 6001)
	listen, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", 6001))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	api.RegisterQueryServer(grpcServer, qs)
	api.RegisterTransactionSvcServer(grpcServer, ts)
	go func() {
		if err := grpcServer.Serve(listen); err != nil {
			log.Fatalf("Failed to start the grpc server")
		}
	}()
}
