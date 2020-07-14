package server

import (
	"fmt"
	"log"
	"net/http"

	"github.ibm.com/blockchaindb/server/config"
)

var s *http.Server

func Start() {
	config.Init()

	restServer, err := NewDBServer()
	if err != nil {
		log.Fatalf("Failed to start rest server: %v", err)
	}

	netConf := config.ServerNetwork()
	go func() {
		s = &http.Server{
			Addr:    fmt.Sprintf("%s:%d", netConf.Address, netConf.Port),
			Handler: restServer.router,
		}

		s.ListenAndServe()
	}()
}

func Stop() {
	s.Close()
}
