package server

import (
	"fmt"
	"net"
	"net/http"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/internal/bcdb"
	"github.ibm.com/blockchaindb/server/internal/httphandler"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// BCDBHTTPServer holds the database and http server objects
type BCDBHTTPServer struct {
	db      bcdb.DB
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
		Name:          conf.Node.Identity.ID,
	}
	logger, err := logger.New(c)
	if err != nil {
		return nil, err
	}

	db, err := bcdb.NewDB(conf, logger)
	if err != nil {
		return nil, errors.Wrap(err, "error while creating the database object")
	}

	mux := http.NewServeMux()
	mux.Handle(constants.UserEndpoint, httphandler.NewUsersRequestHandler(db, logger))
	mux.Handle(constants.DataEndpoint, httphandler.NewDataRequestHandler(db, logger))
	mux.Handle(constants.DBEndpoint, httphandler.NewDBRequestHandler(db, logger))
	mux.Handle(constants.ConfigEndpoint, httphandler.NewConfigRequestHandler(db, logger))
	mux.Handle(constants.LedgerEndpoint, httphandler.NewLedgerRequestHandler(db, logger))
	mux.Handle(constants.ProvenanceEndpoint, httphandler.NewProvenanceRequestHandler(db, logger))

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
		s.logger.Infof("Bootstrapping DB for the first time")
		resp, err := s.db.BootstrapDB(s.conf)
		if err != nil {
			return errors.Wrap(err, "error while preparing and committing config transaction")
		}

		txReceipt := resp.GetPayload().GetReceipt()
		valInfo := txReceipt.GetHeader().GetValidationInfo()[txReceipt.TxIndex]
		if valInfo.Flag != types.Flag_VALID {
			return errors.Errorf("config transaction was not committed due to invalidation [" + valInfo.ReasonIfInvalid + "]")
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

// Port returns port number server allocated to run on
func (s *BCDBHTTPServer) Port() (port string, err error) {
	_, port, err = net.SplitHostPort(s.listen.Addr().String())
	return
}
