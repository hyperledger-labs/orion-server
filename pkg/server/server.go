// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package server

import (
	"fmt"
	"net"
	"net/http"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/bcdb"
	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/httphandler"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/pkg/errors"
)

// BCDBHTTPServer holds the database and http server objects
type BCDBHTTPServer struct {
	db      bcdb.DB
	handler http.Handler
	listen  net.Listener
	server  *http.Server
	conf    *config.Configurations
	logger  *logger.SugarLogger
}

// New creates a object of BCDBHTTPServer
func New(conf *config.Configurations) (*BCDBHTTPServer, error) {
	c := &logger.Config{
		Level:         conf.LocalConfig.Server.LogLevel,
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          conf.LocalConfig.Server.Identity.ID,
	}
	lg, err := logger.New(c)
	if err != nil {
		return nil, err
	}

	db, err := bcdb.NewDB(conf, lg)
	if err != nil {
		return nil, errors.Wrap(err, "error while creating the database object")
	}

	mux := http.NewServeMux()
	mux.Handle(constants.UserEndpoint, httphandler.NewUsersRequestHandler(db, lg))
	mux.Handle(constants.DataEndpoint, httphandler.NewDataRequestHandler(db, lg))
	mux.Handle(constants.DBEndpoint, httphandler.NewDBRequestHandler(db, lg))
	mux.Handle(constants.ConfigEndpoint, httphandler.NewConfigRequestHandler(db, lg))
	mux.Handle(constants.LedgerEndpoint, httphandler.NewLedgerRequestHandler(db, lg))
	mux.Handle(constants.ProvenanceEndpoint, httphandler.NewProvenanceRequestHandler(db, lg))

	netConf := conf.LocalConfig.Server.Network
	addr := fmt.Sprintf("%s:%d", netConf.Address, netConf.Port)

	netListener, err := net.Listen("tcp", addr)
	if err != nil {
		lg.Errorf("Failed to create a tcp listener on: %s, error: %s", addr, err)
		return nil, errors.Wrapf(err, "error while creating a tcp listener on: %s", addr)
	}

	server := &http.Server{Handler: mux}

	return &BCDBHTTPServer{
		db:      db,
		handler: mux,
		listen:  netListener,
		server:  server,
		conf:    conf,
		logger:  lg,
	}, nil
}

// Start starts the server
func (s *BCDBHTTPServer) Start() error {
	if blockHeight, err := s.db.LedgerHeight(); err != nil {
		return err
	} else if blockHeight == 0 {
		return errors.New("ledger height == 0, bootstrap failed")
	} else {
		s.logger.Infof("Server starting at ledger height [%d]", blockHeight)
	}

	go s.serveRequests(s.listen)

	return nil
}

func (s *BCDBHTTPServer) serveRequests(l net.Listener) {
	s.logger.Infof("Starting to serve requests on: %s", s.listen.Addr().String())

	if err := s.server.Serve(l); err != nil {
		if err == http.ErrServerClosed {
			s.logger.Infof("Server stopped: %s", err)
		} else {
			s.logger.Panicf("server stopped unexpectedly, %v", err)
		}
	}

	s.logger.Infof("Finished serving requests on: %s", s.listen.Addr().String())
}

// Stop stops the server
func (s *BCDBHTTPServer) Stop() error {
	if s == nil || s.listen == nil || s.server == nil {
		return nil
	}

	var errR error

	s.logger.Infof("Stopping the server listening on: %s\n", s.listen.Addr().String())
	if err := s.server.Close(); err != nil {
		s.logger.Errorf("Failure while closing the http server: %s", err)
		errR = err
	}

	if err := s.db.Close(); err != nil {
		s.logger.Errorf("Failure while closing the database: %s", err)
		errR = err
	}
	return errR
}

// Port returns port number server allocated to run on
func (s *BCDBHTTPServer) Port() (port string, err error) {
	_, port, err = net.SplitHostPort(s.listen.Addr().String())
	return
}

func (s *BCDBHTTPServer) IsLeader() *ierrors.NotLeaderError {
	return s.db.IsLeader()
}
