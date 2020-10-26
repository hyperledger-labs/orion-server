package server

import (
	"fmt"
	"net"
	"net/http"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
	"github.ibm.com/blockchaindb/server/pkg/server/backend/handlers"
)

// BCDBHTTPServer holds the database and http server objects
type BCDBHTTPServer struct {
	db      backend.DB
	handler http.Handler
	listen  net.Listener
	conf    *config.Configurations
	logger  *logger.SugarLogger
}

// New creates a object of BCDBHTTPServer
func New(conf *config.Configurations) (*BCDBHTTPServer, error) {
	c := &logger.Config{
		Level:         conf.Node.LogLevel,
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	if err != nil {
		return nil, err
	}

	db, err := backend.NewDB(conf, logger)
	if err != nil {
		return nil, errors.Wrap(err, "error while creating the database object")
	}

	mux := http.NewServeMux()
	mux.Handle(constants.UserEndpoint, handlers.NewUsersRequestHandler(db))
	mux.Handle(constants.DataEndpoint, handlers.NewDataRequestHandler(db))
	mux.Handle(constants.DBEndpoint, handlers.NewDBRequestHandler(db))
	mux.Handle(constants.ConfigEndpoint, handlers.NewConfigRequestHandler(db))

	netConf := conf.Node.Network
	addr := fmt.Sprintf("%s:%d", netConf.Address, netConf.Port)
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "error while creating a tcp listener")
	}

	return &BCDBHTTPServer{
		db:      db,
		handler: mux,
		listen:  listen,
		conf:    conf,
		logger:  logger,
	}, nil
}

// Start starts the server
func (s *BCDBHTTPServer) Start() error {
	blockHeight, err := s.db.LedgerHeight()
	if err != nil {
		return err
	}
	if blockHeight == 0 {
		if err := s.db.BootstrapDB(s.conf); err != nil {
			return errors.Wrap(err, "error while preparing and committing config transaction")
		}
	}

	s.logger.Infof("Starting the server on %s", s.listen.Addr().String())

	go func() {
		if err := http.Serve(s.listen, s.handler); err != nil {
			switch err.(type) {
			case *net.OpError:
				s.logger.Warn("network connection is closed")
			default:
				s.logger.Panicf("server stopped unexpectedly, %v", err)
			}
		}
	}()

	return nil
}

// Stop stops the server
func (s *BCDBHTTPServer) Stop() error {
	if s == nil || s.listen == nil {
		return nil
	}

	s.logger.Infof("Stopping the server listening on %s\n", s.listen.Addr().String())
	if err := s.listen.Close(); err != nil {
		return errors.Wrap(err, "error while closing the network listener")
	}

	return s.db.Close()
}
