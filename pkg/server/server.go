// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package server

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/bcdb"
	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/httphandler"
	"github.com/hyperledger-labs/orion-server/pkg/certificateauthority"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// BCDBHTTPServer holds the database and http server objects
type BCDBHTTPServer struct {
	db            bcdb.DB
	handler       http.Handler
	listen        net.Listener
	server        *http.Server
	conf          *config.Configurations
	metricsListen net.Listener
	metricsServer *http.Server
	logger        *logger.SugarLogger
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

	// If metricsRegistry is not initialized (nil), then no metrics will be collected
	var metricsRegistry *prometheus.Registry
	var metricsServer *http.Server
	var metricsListen net.Listener
	if conf.LocalConfig.Prometheus.Enabled {
		metricsRegistry = prometheus.NewRegistry()
		metricsRegistry.MustRegister(
			collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
			collectors.NewGoCollector(),
		)

		metricsNetConf := conf.LocalConfig.Prometheus.Network
		metricsServer = &http.Server{
			Addr: fmt.Sprintf("%s:%d", metricsNetConf.Address, metricsNetConf.Port),
			Handler: promhttp.InstrumentMetricHandler(
				metricsRegistry, promhttp.HandlerFor(metricsRegistry, promhttp.HandlerOpts{}),
			),
		}
		metricsListen, err = net.Listen("tcp", metricsServer.Addr)
		if err != nil {
			lg.Errorf("Failed to create Promeheus tcp listener on: %s, error: %s", metricsServer.Addr, err)
			return nil, errors.Wrapf(err, "error while creating a tcp listener on: %s", metricsServer.Addr)
		}
		metricsServer.TLSConfig, err = makeTlsConfig(&conf.LocalConfig.Prometheus.TLS, conf.SharedConfig)
		if err != nil {
			return nil, err
		}
	}

	db, err := bcdb.NewDB(conf, lg, metricsRegistry)
	if err != nil {
		return nil, errors.Wrap(err, "error while creating the database object")
	}

	mux := http.NewServeMux()
	mux.Handle(constants.UserEndpoint, httphandler.NewUsersRequestHandler(db, lg))
	mux.Handle(constants.DataEndpoint, httphandler.NewDataRequestHandler(db, lg, metricsRegistry))
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

	server := &http.Server{
		Handler: mux,
	}

	server.TLSConfig, err = makeTlsConfig(&conf.LocalConfig.Server.TLS, conf.SharedConfig)
	if err != nil {
		return nil, err
	}

	return &BCDBHTTPServer{
		db:            db,
		handler:       mux,
		listen:        netListener,
		server:        server,
		conf:          conf,
		metricsListen: metricsListen,
		metricsServer: metricsServer,
		logger:        lg,
	}, nil
}

func makeTlsConfig(tlsConf *config.TLSConf, sharedConf *config.SharedConfiguration) (*tls.Config, error) {
	if !tlsConf.Enabled {
		return nil, nil
	}
	// load and check the CA certificates
	caCerts, err := certificateauthority.LoadCAConfig(&sharedConf.CAConfig)
	if err != nil {
		return nil, errors.Wrapf(err, "error while loading CA certificates from local configuration Server.TLS.CaConfig: %+v", &sharedConf.CAConfig)
	}
	caColl, err := certificateauthority.NewCACertCollection(caCerts.GetRoots(), caCerts.GetIntermediates())
	if err != nil {
		return nil, errors.Wrap(err, "error while creating a CA certificate collection")
	}
	if err := caColl.VerifyCollection(); err != nil {
		return nil, errors.Wrap(err, "error while verifying the CA certificate collection")
	}

	// get a x509.CertPool of all the CA certificates for tls.Config
	caCertPool := caColl.GetCertPool()

	// server tls.Config
	serverKeyBytes, err := os.ReadFile(tlsConf.ServerKeyPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read local config Server.TLS.ServerKeyPath")
	}
	serverCertBytes, err := os.ReadFile(tlsConf.ServerCertificatePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read local config Server.TLS.ServerCertificatePath")
	}
	serverKeyPair, err := tls.X509KeyPair(serverCertBytes, serverKeyBytes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create server tls.X509KeyPair")
	}

	tlsServerConfig := &tls.Config{
		Certificates: []tls.Certificate{serverKeyPair},
		RootCAs:      caCertPool,
		ClientCAs:    caCertPool,
		MinVersion:   tls.VersionTLS12,
	}
	if tlsConf.ClientAuthRequired {
		tlsServerConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsServerConfig, nil
}

// Start starts the server
func (s *BCDBHTTPServer) Start() error {
	for {
		if blockHeight, err := s.db.LedgerHeight(); err != nil {
			return err
		} else if blockHeight == 0 {
			// If the server is bootstrapping from the peers, it might still be trying to get the genesis block; We
			// wait for that to happen before we start serving requests.
			s.logger.Infof("Ledger height =0, server boostraping from peers; waiting for height >0; going to sleep for 1s...")
			time.Sleep(time.Second)
		} else {
			s.logger.Infof("Server starting at ledger height [%d]", blockHeight)
			break
		}
	}

	go s.serveRequests(s.listen)
	go s.servePrometheus()

	return nil
}

func (s *BCDBHTTPServer) serveRequests(l net.Listener) {
	s.logger.Infof("Starting to serve requests on: %s", s.listen.Addr().String())

	var err error
	if s.conf.LocalConfig.Server.TLS.Enabled {
		err = s.server.ServeTLS(l, "", "")
	} else {
		err = s.server.Serve(l)
	}

	if err == http.ErrServerClosed {
		s.logger.Infof("Server stopped: %s", err)
	} else {
		s.logger.Panicf("server stopped unexpectedly, %v", err)
	}

	s.logger.Infof("Finished serving requests on: %s", s.listen.Addr().String())
}

func (s *BCDBHTTPServer) servePrometheus() {
	if s.metricsServer == nil {
		return
	}

	s.logger.Infof("Starting Prometheus server on: %s", s.server.Addr)

	var err error
	if s.conf.LocalConfig.Prometheus.TLS.Enabled {
		err = s.metricsServer.ServeTLS(s.metricsListen, "", "")
	} else {
		err = s.metricsServer.Serve(s.metricsListen)
	}

	if err == http.ErrServerClosed {
		s.logger.Infof("Prometheus server stopped: %s", err)
	} else {
		s.logger.Panicf("Prometheus server stopped unexpectedly, %v", err)
	}
	s.logger.Infof("Finished serving prometheus requests on: %s", s.metricsServer.Addr)
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

	if s.metricsServer != nil {
		s.logger.Infof("Stopping the Prometheus server listening on: %s\n", s.metricsServer.Addr)
		if err := s.metricsServer.Close(); err != nil {
			s.logger.Errorf("Failure while closing the http Prometheus server: %s", err)
			errR = err
		}
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
