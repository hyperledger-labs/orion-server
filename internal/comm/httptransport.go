// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package comm

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/config"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
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

	transport      *rafthttp.Transport
	catchUpClient  *catchUpClient
	catchupHandler *catchupHandler
	httpServer     *http.Server

	stopCh chan struct{} // signals HTTPTransport to shutdown
	doneCh chan struct{} // signals HTTPTransport shutdown complete

	logger *logger.SugarLogger
}

type Config struct {
	LocalConf    *config.LocalConfiguration
	Logger       *logger.SugarLogger
	LedgerReader LedgerReader
}

func NewHTTPTransport(config *Config) *HTTPTransport {
	if config.LocalConf.Replication.TLS.Enabled {
		config.Logger.Panic("TLS not supported yet")
	}

	tr := &HTTPTransport{
		logger:         config.Logger,
		localConf:      config.LocalConf,
		catchUpClient:  NewCatchUpClient(config.Logger),
		catchupHandler: NewCatchupHandler(config.Logger, config.LedgerReader),
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}

	return tr
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
			p.transport.AddPeer(
				etcd_types.ID(peer.RaftId),
				[]string{fmt.Sprintf("http://%s:%d", peer.PeerHost, peer.PeerPort)}) //TODO unsecure for now, add TLS/https later
		}
	}
	if err = p.catchUpClient.UpdateMembers(membersList); err != nil {
		return err
	}

	raftHandler := p.transport.Handler()
	mux := http.NewServeMux()
	mux.Handle(rafthttp.RaftPrefix, raftHandler)
	mux.Handle(BCDBPeerEndpoint, p.catchupHandler)
	p.httpServer = &http.Server{Handler: mux}

	go p.servePeers(netListener)

	return nil
}

func (p *HTTPTransport) servePeers(l net.Listener) {
	p.logger.Infof("http transport starting to serve peers on: %s", l.Addr().String())
	err := p.httpServer.Serve(l)
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

	if err := p.httpServer.Close(); err != nil {
		p.logger.Errorf("http transport failed to close http server: %s", err)
	}

	p.transport.Stop()

	select {
	case <-p.doneCh:
		p.logger.Info("http transport closed")
	case <-time.After(10 * time.Second):
		p.logger.Info("http transport Close() timed-out waiting for http server to complete shutdown")
	}
}

func (p *HTTPTransport) SendConsensus(msgs []raftpb.Message) error {

	p.transport.Send(msgs)

	return nil
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
