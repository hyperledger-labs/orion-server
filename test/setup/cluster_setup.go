package setup

import (
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/internal/fileops"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
)

// Cluster holds bcdb servers present in a blockchainDB cluster
type Cluster struct {
	Servers        []*Server
	testDirAbsPath string
	bdbBinaryPath  string
	cmdTimeout     time.Duration
	logger         *logger.SugarLogger
	rootCAPemCert  []byte
	caPrivKey      []byte
	mu             sync.Mutex
}

// Config holds configuration detail needed to instantiate a cluster
type Config struct {
	NumberOfServers     int
	TestDirAbsolutePath string
	BDBBinaryPath       string
	CmdTimeout          time.Duration
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

	l, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	if err != nil {
		return nil, err
	}

	rootCAPemCert, caPrivKey, err := testutils.GenerateRootCA("BCDB RootCA", "127.0.0.1")
	if err != nil {
		return nil, err
	}

	cluster := &Cluster{
		Servers:        make([]*Server, conf.NumberOfServers),
		logger:         l,
		testDirAbsPath: conf.TestDirAbsolutePath,
		bdbBinaryPath:  conf.BDBBinaryPath,
		cmdTimeout:     conf.CmdTimeout,
		rootCAPemCert:  rootCAPemCert,
		caPrivKey:      caPrivKey,
	}

	for i := 0; i < conf.NumberOfServers; i++ {
		cluster.Servers[i], err = NewServer(i, conf.TestDirAbsolutePath, l)
		if err != nil {
			return nil, err
		}
	}

	if err := cluster.createCryptoMaterials(); err != nil {
		return nil, err
	}

	if err := cluster.createConfigFile(); err != nil {
		return nil, err
	}

	cluster.createCmdToStartServers()

	return cluster, nil
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

func (c *Cluster) createCryptoMaterials() error {
	for _, s := range c.Servers {
		if err := s.createCryptoMaterials(c.rootCAPemCert, c.caPrivKey); err != nil {
			return err
		}
	}

	return nil
}

func (c *Cluster) createConfigFile() error {
	for _, s := range c.Servers {
		if err := s.createConfigFile(); err != nil {
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
