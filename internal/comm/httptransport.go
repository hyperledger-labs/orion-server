// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package comm

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/certificateauthority"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/transport"
	etcd_types "go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
)

//go:generate counterfeiter -o mocks/consensus_listener.go --fake-name ConsensusListener . ConsensusListener

type ConsensusListener interface {
	rafthttp.Raft
}

type HTTPTransport struct {
	localConf *config.LocalConfiguration

	mutex             sync.Mutex
	consensusListener ConsensusListener
	clusterConfig     *types.ClusterConfig

	raftID uint64

	tlsInfo         transport.TLSInfo //for use as a rafthttp client
	tlsServerConfig *tls.Config       //for use as a server
	tlsClientConfig *tls.Config       //for use as a catchup client
	transport       *rafthttp.Transport
	catchUpClient   *catchUpClient
	catchupHandler  *catchupHandler
	httpServer      *http.Server

	stopCh chan struct{} // signals HTTPTransport to shutdown
	doneCh chan struct{} // signals HTTPTransport shutdown complete

	logger *logger.SugarLogger
}

type Config struct {
	LocalConf    *config.LocalConfiguration
	Logger       *logger.SugarLogger
	LedgerReader LedgerReader
}

func NewHTTPTransport(config *Config) (*HTTPTransport, error) {
	if config.LocalConf.Replication.TLS.Enabled && config.LocalConf.Replication.TLS.ClientAuthRequired {
		return nil, errors.New("TLS Client authentication not supported yet")
	}

	tr := &HTTPTransport{
		logger:         config.Logger,
		localConf:      config.LocalConf,
		catchUpClient:  NewCatchUpClient(config.Logger, nil),
		catchupHandler: NewCatchupHandler(config.Logger, config.LedgerReader, 0), //TODO make max-response-bytes configurable
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}

	if config.LocalConf.Replication.TLS.Enabled {
		// load and check the CA certificates
		caCerts, err := certificateauthority.LoadCAConfig(&tr.localConf.Replication.TLS.CaConfig)
		if err != nil {
			return nil, errors.Wrapf(err, "error while loading CA certificates from local configuration Replication.TLS.CaConfig: %+v", tr.localConf.Replication.TLS.CaConfig)
		}
		caColl, err := certificateauthority.NewCACertCollection(caCerts.GetRoots(), caCerts.GetIntermediates())
		if err != nil {
			return nil, errors.Wrap(err, "error while creating a CA certificate collection")
		}
		if err := caColl.VerifyCollection(); err != nil {
			return nil, errors.Wrap(err, "error while verifying the CA certificate collection")
		}

		// get a x509.CertPool of all the CA crtificates for tls.Config
		caCertPool := caColl.GetCertPool()

		// combine all the root & intermediate CA certificates we have into a single file for Raft TLSInfo
		caBundleFile := path.Join(tr.localConf.Replication.AuxDir, "ca-bundle.pem")
		if err := tr.localConf.Replication.TLS.CaConfig.WriteBundle(caBundleFile); err != nil {
			return nil, errors.Wrapf(err, "failed to create CA bundle file")
		}

		tr.tlsInfo = transport.TLSInfo{
			CertFile:            tr.localConf.Replication.TLS.ClientCertificatePath,
			KeyFile:             tr.localConf.Replication.TLS.ClientKeyPath,
			TrustedCAFile:       caBundleFile,
			ClientCertAuth:      config.LocalConf.Replication.TLS.ClientAuthRequired,
			CRLFile:             "",
			InsecureSkipVerify:  false,
			SkipClientSANVerify: false,
			ServerName:          "",
			HandshakeFailure:    nil,
			CipherSuites:        nil,
			AllowedCN:           "",
			AllowedHostname:     "",
			Logger:              config.Logger.Desugar().Named("tls"),
			EmptyCN:             false,
		}

		// catch-up client tls.Config
		clientKeyBytes, err := os.ReadFile(tr.localConf.Replication.TLS.ClientKeyPath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read local config Replication.TLS.ClientKeyPath")
		}
		clientCertBytes, err := os.ReadFile(tr.localConf.Replication.TLS.ClientCertificatePath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read local config Replication.TLS.ClientCertificatePath")
		}
		clientKeyPair, err := tls.X509KeyPair(clientCertBytes, clientKeyBytes)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create client tls.X509KeyPair")
		}

		tr.tlsClientConfig = &tls.Config{
			Certificates: []tls.Certificate{clientKeyPair},
			RootCAs:      caCertPool,
			ClientCAs:    caCertPool,
			MinVersion:   tls.VersionTLS12,
		}
		tr.catchUpClient = NewCatchUpClient(config.Logger, tr.tlsClientConfig)

		// server tls.Config
		serverKeyBytes, err := os.ReadFile(tr.localConf.Replication.TLS.ServerKeyPath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read local config Replication.TLS.ServerKeyPath")
		}
		serverCertBytes, err := os.ReadFile(tr.localConf.Replication.TLS.ServerCertificatePath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read local config Replication.TLS.ServerCertificatePath")
		}
		serverKeyPair, err := tls.X509KeyPair(serverCertBytes, serverKeyBytes)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create server tls.X509KeyPair")
		}

		tr.tlsServerConfig = &tls.Config{
			Certificates: []tls.Certificate{serverKeyPair},
			RootCAs:      caCertPool,
			ClientCAs:    caCertPool,
			MinVersion:   tls.VersionTLS12,
		}
	}

	return tr, nil
}

func (p *HTTPTransport) SetConsensusListener(l ConsensusListener) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.consensusListener != nil {
		return errors.New("ConsensusListener already set")
	}
	p.consensusListener = l

	return nil
}

//TODO implement dynamic re-config, currently it can only be updated once.
func (p *HTTPTransport) UpdateClusterConfig(clusterConfig *types.ClusterConfig) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.clusterConfig != nil {
		return errors.New("dynamic re-config of http transport is not supported yet")
	}

	raftID, err := MemberRaftID(p.localConf.Server.Identity.ID, clusterConfig)
	if err != nil {
		return err
	}

	p.raftID = raftID
	p.clusterConfig = clusterConfig

	return nil
}

func (p *HTTPTransport) Start() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.consensusListener == nil {
		p.logger.Panic("Must set ConsensusListener before Start()")
	}

	if p.clusterConfig == nil {
		p.logger.Panic("Must update ClusterConfig before Start()")
	}

	netConf := p.localConf.Replication.Network
	addr := fmt.Sprintf("%s:%d", netConf.Address, netConf.Port)
	netListener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrap(err, "error while creating a tcp listener")
	}

	p.transport = &rafthttp.Transport{
		Logger:      p.logger.Desugar(),
		TLSInfo:     p.tlsInfo,
		ID:          etcd_types.ID(p.raftID),
		ClusterID:   0x1000, // TODO compute a ClusterID from the genesis block?
		Raft:        p.consensusListener,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(int(p.raftID))),
		ErrorC:      make(chan error),
	}

	if err = p.transport.Start(); err != nil {
		return errors.Wrapf(err, "failed to start rafthttp transport")
	}

	var membersList []*types.PeerConfig
	for _, peer := range p.clusterConfig.ConsensusConfig.Members {
		if peer.RaftId != p.raftID {
			membersList = append(membersList, peer)
			schema := "http"
			if p.localConf.Replication.TLS.Enabled {
				schema = "https"
			}
			p.transport.AddPeer(
				etcd_types.ID(peer.RaftId),
				[]string{fmt.Sprintf("%s://%s:%d", schema, peer.PeerHost, peer.PeerPort)})
		}
	}
	if err = p.catchUpClient.UpdateMembers(membersList); err != nil {
		return err
	}

	raftHandler := p.transport.Handler()
	mux := http.NewServeMux()
	mux.Handle(rafthttp.RaftPrefix, raftHandler)
	mux.Handle(BCDBPeerEndpoint, p.catchupHandler)

	p.httpServer = &http.Server{
		Handler:   mux,
		TLSConfig: p.tlsServerConfig,
		ErrorLog: log.New(
			&LogAdapter{SugarLogger: p.logger, Debug: false}, //log all errors as Info
			"peer-http-server: ", 0),
	}

	go p.servePeers(netListener)

	return nil
}

func (p *HTTPTransport) servePeers(l net.Listener) {
	p.logger.Infof("http transport starting to serve peers on: %s", l.Addr().String())
	var err error
	if p.localConf.Replication.TLS.Enabled {
		err = p.httpServer.ServeTLS(l, "", "")
	} else {
		err = p.httpServer.Serve(l)
	}

	select {
	case <-p.stopCh:
		p.logger.Info("http transport stopping to server peers")
	default:
		p.logger.Errorf("http transport failed to serve peers (%v)", err)
	}
	close(p.doneCh)
}

func (p *HTTPTransport) Close() {
	p.logger.Info("closing http transport")
	close(p.stopCh)

	p.transport.Stop()

	if err := p.httpServer.Close(); err != nil {
		p.logger.Errorf("http transport failed to close http server: %s", err)
	}

	select {
	case <-p.doneCh:
		p.logger.Info("http transport closed")
	case <-time.After(10 * time.Second):
		p.logger.Info("http transport Close() timed-out waiting for http server to complete shutdown")
	}
}

func (p *HTTPTransport) SendConsensus(msgs []raftpb.Message) error {
	for i, m := range msgs {
		p.logger.Debugf("SendConsensus (%d/%d): Type: %s, From: %d, To: %d", i+1, len(msgs), m.Type, p.raftID, m.To)
	}

	p.transport.Send(msgs)

	return nil
}

func (p *HTTPTransport) ClientTLSConfig() *tls.Config {
	return p.tlsClientConfig
}

// PullBlocks tries to pull as many blocks as possible from startBlock to endBlock (inclusive).
//
// The calling go-routine will block until some blocks are retrieved, depending on the availability of remote peers.
// The underlying implementation will poll the cluster members, starting from the leader hint (if exists), until it can
// retrieve some blocks. The call may return fewer blocks than requested. The `leaderID` is a hint to the leader's
// Raft ID, and can be 0. The call maybe canceled using the context `ctx`.
func (p *HTTPTransport) PullBlocks(ctx context.Context, startBlock, endBlock, leaderID uint64) ([]*types.Block, error) {
	return p.catchUpClient.PullBlocks(ctx, startBlock, endBlock, leaderID)
}

func MemberRaftID(memberID string, clusterConfig *types.ClusterConfig) (uint64, error) {
	for _, member := range clusterConfig.ConsensusConfig.Members {
		if member.NodeId == memberID {
			return member.RaftId, nil
		}
	}

	return 0, errors.Errorf("node ID '%s' is not in Consensus members: %v", memberID, clusterConfig.ConsensusConfig.Members)
}
