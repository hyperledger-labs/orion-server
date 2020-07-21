package server

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/config"
)

var s *http.Server

func Start() error {
	if err := config.Init(); err != nil {
		return errors.WithMessagef(err, "error while starting the server")
	}

	restServer, err := NewDBServer()
	if err != nil {
		log.Fatalf("Failed to start rest server: %v", err)
	}

	netConf := config.ServerNetwork()
	log.Printf("Starting the server on %s:%d\n", netConf.Address, netConf.Port)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		s = &http.Server{
			Addr:    fmt.Sprintf("%s:%d", netConf.Address, netConf.Port),
			Handler: restServer.router,
		}

		s.ListenAndServe()
	}()
	wg.Wait()

	return nil
}

func Stop() {
	s.Close()
}
