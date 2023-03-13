// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package setup

import (
	"crypto/tls"
	"net/http"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/pkg/errors"
)

// Cluster holds bcdb servers present in a blockchainDB cluster
type Cluster struct {
	Servers            []*Server
	Users              []string
	testDirAbsPath     string
	bdbBinaryPath      string
	cmdTimeout         time.Duration
	baseNodePort       uint32
	basePeerPort       uint32
	logger             *logger.SugarLogger
	rootCAPath         string
	rootCAPemCert      []byte
	caPrivKey          []byte
	disableStateMPTrie bool
	mu                 sync.Mutex
}

// Config holds configuration detail needed to instantiate a cluster
type Config struct {
	NumberOfServers          int
	TestDirAbsolutePath      string
	BDBBinaryPath            string
	CmdTimeout               time.Duration
	BaseNodePort             uint32
	BasePeerPort             uint32
	CheckRedirectFunc        func(req *http.Request, via []*http.Request) error // rest client checks redirects
	ClusterTLSEnabled        bool
	BlockCreationOverride    *config.BlockCreationConf
	ServersQueryLimit        uint64
	DisableProvenanceServers []int
	DisableStateMPTrie       bool
	PrometheusEnabled        bool
}

// NewCluster creates a new cluster environment for the blockchain database
func NewCluster(conf *Config) (*Cluster, error) {
	if conf.CmdTimeout < 1*time.Second {
		return nil, errors.New("cmd timeout must be at least 1 second")
	}

	exist, err := fileops.Exists(conf.BDBBinaryPath)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, errors.New(conf.BDBBinaryPath + " executable does not exist")
	}

	if conf.BaseNodePort == 0 || conf.BasePeerPort == 0 {
		return nil, errors.New("set BaseNodePort >0 & BasePeerPort >0")
	}

	l, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	if err != nil {
		return nil, err
	}

	cluster := &Cluster{
		Servers:            make([]*Server, conf.NumberOfServers),
		Users:              []string{"admin", "alice", "bob", "charlie"},
		logger:             l,
		testDirAbsPath:     conf.TestDirAbsolutePath,
		bdbBinaryPath:      conf.BDBBinaryPath,
		cmdTimeout:         conf.CmdTimeout,
		rootCAPath:         path.Join(conf.TestDirAbsolutePath, "ca"),
		baseNodePort:       conf.BaseNodePort,
		basePeerPort:       conf.BasePeerPort,
		disableStateMPTrie: conf.DisableStateMPTrie,
	}

	if err = cluster.createRootCA(); err != nil {
		return nil, err
	}

	for i := 0; i < conf.NumberOfServers; i++ {
		cluster.Servers[i], err = NewServer(uint64(i), conf.TestDirAbsolutePath, conf.BaseNodePort, conf.BasePeerPort, conf.CheckRedirectFunc, l, "genesis", conf.ServersQueryLimit)
		if err != nil {
			return nil, err
		}
	}

	if err := cluster.createCryptoMaterials(); err != nil {
		return nil, err
	}

	localConf := &config.LocalConfiguration{
		Replication: config.ReplicationConf{
			TLS: config.TLSConf{
				Enabled: conf.ClusterTLSEnabled,
			},
		},
		Prometheus: config.PrometheusConf{
			Enabled: conf.PrometheusEnabled,
		},
	}
	if conf.BlockCreationOverride != nil {
		localConf.BlockCreation = *conf.BlockCreationOverride
	}

	if err := cluster.createConfigFile(localConf, conf.DisableProvenanceServers); err != nil {
		return nil, err
	}

	if err := cluster.createBootstrapFile(); err != nil {
		return nil, err
	}

	cluster.createCmdToStartServers()

	return cluster, nil
}

func (c *Cluster) createRootCA() (err error) {
	c.rootCAPemCert, c.caPrivKey, err = testutils.GenerateRootCA("BCDB RootCA", "127.0.0.1")
	if err != nil {
		return err
	}

	if err = fileops.CreateDir(c.rootCAPath); err != nil {
		return err
	}
	serverRootCACertPath := path.Join(c.rootCAPath, "rootCA.pem")
	serverRootCACertFile, err := os.Create(serverRootCACertPath)
	if err != nil {
		return err
	}
	if _, err = serverRootCACertFile.Write(c.rootCAPemCert); err != nil {
		return err
	}
	if err = serverRootCACertFile.Close(); err != nil {
		return err
	}

	serverRootCAKeyPath := path.Join(c.rootCAPath, "rootCA.key")
	serverRootCAKeyFile, err := os.Create(serverRootCAKeyPath)
	if err != nil {
		return err
	}
	if _, err = serverRootCAKeyFile.Write(c.caPrivKey); err != nil {
		return err
	}
	if err = serverRootCAKeyFile.Close(); err != nil {
		return err
	}

	return err
}

// Start starts the cluster by starting all servers in the cluster
func (c *Cluster) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.startWithoutLock()
}

func (c *Cluster) startWithoutLock() error {
	err := make(chan error, len(c.Servers))
	var wg sync.WaitGroup
	wg.Add(len(c.Servers))
	for _, s := range c.Servers {
		go func(s *Server, err chan error) {
			c.logger.Debug("Check whether the server " + s.serverID + " has started")
			defer wg.Done()
			if errR := s.start(c.cmdTimeout); err != nil {
				err <- errR
				return
			}
			c.logger.Debug("Successfully started server " + s.serverID)
		}(s, err)
	}
	wg.Wait()

	select {
	case e := <-err:
		return e
	default:
		return nil
	}
}

// Shutdown shuts the cluster down by shutting down all servers in the cluster
func (c *Cluster) Shutdown() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.shutdownWithoutLock()
}

func (c *Cluster) shutdownWithoutLock() error {
	err := make(chan error, len(c.Servers))
	var wg sync.WaitGroup
	wg.Add(len(c.Servers))
	for _, s := range c.Servers {
		go func(s *Server, err chan error) {
			defer wg.Done()
			if errR := s.shutdown(); err != nil {
				err <- errR
				return
			}
		}(s, err)
	}
	wg.Wait()

	select {
	case e := <-err:
		return e
	default:
		return nil
	}
}

// ShutdownAndCleanup shuts the cluster down by shutting down all servers in the
// cluster and removes all directories
func (c *Cluster) ShutdownAndCleanup() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.shutdownWithoutLock(); err != nil {
		return err
	}

	for _, s := range c.Servers {
		if err := os.RemoveAll(s.configDir); err != nil {
			return err
		}
	}

	return nil
}

// Restart restarts the cluster by shutting down and starting all servers in the cluster
func (c *Cluster) Restart() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.shutdownWithoutLock(); err != nil {
		return err
	}

	c.createCmdToStartServers()

	return c.startWithoutLock()
}

// ShutdownServer shuts a given server present in the cluster down
func (c *Cluster) ShutdownServer(s *Server) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return s.shutdown()
}

// RestartServer restarts a given server present in the cluster by shutting down and restarting the server
func (c *Cluster) RestartServer(s *Server) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := s.shutdown(); err != nil {
		return err
	}

	s.createCmdToStartServers(c.bdbBinaryPath)

	return s.start(c.cmdTimeout)
}

// StartServer stars a given server present in the cluster
func (c *Cluster) StartServer(s *Server) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	s.createCmdToStartServers(c.bdbBinaryPath)

	return s.start(c.cmdTimeout)
}

func (c *Cluster) GetServerByID(serverID string) (*Server, int) {
	if serverID == "" {
		return nil, -1
	}
	for i, srv := range c.Servers {
		if srv.serverID == serverID {
			return srv, i
		}
	}
	return nil, -1
}

func (c *Cluster) AgreedLeader(t *testing.T,
	activeServers ...int) int {
	var leaders []int
	var leader int

	for _, srvVal := range activeServers {
		clusterStatusResEnv, err := c.Servers[srvVal].QueryClusterStatus(t)
		if err == nil && clusterStatusResEnv != nil {
			_, leader = c.GetServerByID(clusterStatusResEnv.GetResponse().GetLeader())
			if leader == -1 {
				return -1
			}
			leaders = append(leaders, leader)
		} else {
			return -1
		}
	}

	if len(activeServers) != len(leaders) {
		return -1
	}

	for _, l := range leaders {
		if l != leader {
			return -1
		}
	}

	return leader
}

func (c *Cluster) AgreedHeight(t *testing.T, expectedBlockHeight uint64, activeServers ...int) bool {
	for _, srvVal := range activeServers {
		blockResEnv, err := c.Servers[srvVal].QueryLastBlockStatus(t)
		if err != nil {
			t.Logf("error: %s", err.Error())
			return false
		}
		if blockResEnv == nil {
			t.Errorf("error: GetBlockResponseEnvelope is nil") // should never happen when no error
			return false
		}
		if blockResEnv.GetResponse().GetBlockHeader().GetBaseHeader().GetNumber() != expectedBlockHeight {
			return false
		}
	}

	return true
}

func (c *Cluster) AddNewServerToCluster(server *Server) {
	c.Servers = append(c.Servers, server)
}

func (c *Cluster) GetUserCertDir() string {
	return path.Join(c.testDirAbsPath, "users")
}

func (c *Cluster) GetUserCertKeyPath(userID string) (string, string) {
	userCertPath := path.Join(c.testDirAbsPath, "users", userID+".pem")
	userKeyPath := path.Join(c.testDirAbsPath, "users", userID+".key")
	return userCertPath, userKeyPath
}

func (c *Cluster) GetSigner(userID string) (crypto.Signer, error) {
	_, keyPath := c.GetUserCertKeyPath(userID)
	return crypto.NewSigner(
		&crypto.SignerOptions{
			Identity:    "",
			KeyFilePath: keyPath,
		},
	)
}

func (c *Cluster) GetX509KeyPair() (tls.Certificate, error) {
	keyPair, err := tls.X509KeyPair(c.rootCAPemCert, c.caPrivKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	return keyPair, nil
}

func (c *Cluster) GetKeyAndCA() ([]byte, []byte) {
	return c.caPrivKey, c.rootCAPemCert
}

func (c *Cluster) createCryptoMaterials() error {
	for _, s := range c.Servers {
		if err := s.CreateCryptoMaterials(c.rootCAPemCert, c.caPrivKey); err != nil {
			return err
		}
	}

	if err := c.createUsersCryptoMaterials(); err != nil {
		return err
	}

	return nil
}

func (c *Cluster) createUsersCryptoMaterials() error {
	if err := fileops.CreateDir(path.Join(c.testDirAbsPath, "users")); err != nil {
		return err
	}

	keyPair, err := c.GetX509KeyPair()
	if err != nil {
		return err
	}

	for _, user := range c.Users {
		c.CreateUserCerts(user, keyPair)
	}

	return nil
}

func (c *Cluster) CreateAdditionalUserCryptoMaterials(user string, certRootCA []byte, caPrivKey []byte) error {
	keyPair, err := tls.X509KeyPair(certRootCA, caPrivKey)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	err = c.CreateUserCerts(user, keyPair)
	if err != nil {
		return err
	}

	return nil
}

func (c *Cluster) CreateUserCerts(user string, keyPair tls.Certificate) error {
	var err error

	pemUserCert, pemUserKey, err := testutils.IssueCertificate("Cluster User: "+user, "127.0.0.1", keyPair)
	if err != nil {
		return err
	}

	userCertPath := path.Join(c.testDirAbsPath, "users", user+".pem")
	if err = os.WriteFile(userCertPath, pemUserCert, 0644); err != nil {
		return err
	}

	userKeyPath := path.Join(c.testDirAbsPath, "users", user+".key")
	if err = os.WriteFile(userKeyPath, pemUserKey, 0644); err != nil {
		return err
	}

	if user == "admin" {
		for _, s := range c.Servers {
			s.adminCertPath = userCertPath
			s.adminKeyPath = userKeyPath
			adminSigner, err := crypto.NewSigner(
				&crypto.SignerOptions{KeyFilePath: userKeyPath},
			)
			if err != nil {
				return err
			}
			s.adminSigner = adminSigner
		}
	}

	return nil
}

func (c *Cluster) GetUser(user string) ([]byte, []byte, error) {
	userCertPath, userKeyPath := c.GetUserCertKeyPath(user)
	keyBytes, err := os.ReadFile(userKeyPath)
	if err != nil {
		return nil, nil, err
	}
	certBytes, err := os.ReadFile(userCertPath)
	if err != nil {
		return nil, nil, err
	}

	return certBytes, keyBytes, err
}

func (c *Cluster) UpdateServersAdmin(newAdmin string, newAdminKeyPath string, newAdminCertPath string) error {
	for _, s := range c.Servers {
		newAdminSigner, err := crypto.NewSigner(
			&crypto.SignerOptions{KeyFilePath: newAdminKeyPath},
		)
		if err != nil {
			return err
		}
		s.SetAdmin(newAdmin, newAdminCertPath, newAdminKeyPath, newAdminSigner)
	}
	return nil
}

func (c *Cluster) GetLogger() *logger.SugarLogger {
	return c.logger
}

func contain(arr []int, s int) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}
	return false
}

func (c *Cluster) createConfigFile(conf *config.LocalConfiguration, DisableProvenanceServers []int) error {
	for i, s := range c.Servers {
		newConf := conf
		if contain(DisableProvenanceServers, i) {
			newConf.Server.Provenance.Disabled = true
		}
		if err := s.CreateConfigFile(newConf); err != nil {
			return err
		}
	}

	return nil
}

func (c *Cluster) createBootstrapFile() error {
	sharedConfig := &config.SharedConfiguration{
		Nodes: nil,
		Consensus: &config.ConsensusConf{
			Algorithm: "raft",
			Members:   nil,
			Observers: nil,
			RaftConfig: &config.RaftConf{
				TickInterval:         "100ms",
				ElectionTicks:        50,
				HeartbeatTicks:       5,
				MaxInflightBlocks:    50,
				SnapshotIntervalSize: 64 * 1024 * 1024,
			},
		},
		CAConfig: config.CAConfiguration{
			RootCACertsPath:         []string{path.Join(c.rootCAPath, "rootCA.pem")},
			IntermediateCACertsPath: nil,
		},
		Admin: config.AdminConf{
			ID:              "admin",
			CertificatePath: path.Join(c.testDirAbsPath, "users", "admin.pem"),
		},
		Ledger: config.LedgerConf{
			StateMerklePatriciaTrieDisabled: c.disableStateMPTrie,
		},
	}

	for _, s := range c.Servers {
		sharedConfig.Consensus.Members = append(
			sharedConfig.Consensus.Members,
			&config.PeerConf{
				NodeId:   s.serverID,
				RaftId:   s.serverNum,
				PeerHost: s.address,
				PeerPort: uint32(s.peerPort),
			},
		)
	}

	for _, s := range c.Servers {
		sharedConfig.Nodes = append(sharedConfig.Nodes, &config.NodeConf{
			NodeID:          s.serverID,
			Host:            s.address,
			Port:            uint32(s.nodePort),
			CertificatePath: s.serverCertPath,
		})
	}

	for _, s := range c.Servers {
		if err := WriteSharedConfig(sharedConfig, s.bootstrapFilePath); err != nil {
			return err
		}
	}

	return nil
}

func (c *Cluster) createCmdToStartServers() {
	for _, s := range c.Servers {
		s.createCmdToStartServers(c.bdbBinaryPath)
	}
}
