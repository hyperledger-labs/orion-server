package server

import (
	"fmt"
	"log"
	"net/http"
)

var s *http.Server
var restServer *DBServer

func Start() {
	var err error
	restServer, err = NewDBServer()
	if err != nil {
		log.Fatalf("failed to start rest server: %v", err)
	}

	go func() {
		s = &http.Server{
			Addr:    fmt.Sprintf("localhost:%d", 6001),
			Handler: restServer.router,
		}

		s.ListenAndServe()
	}()
}

func Stop() {
	s.Close()
}
