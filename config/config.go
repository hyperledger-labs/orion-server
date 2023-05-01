// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

const (
	defaultLocalConfigFile = "config.yml"
)

// Configurations holds the complete configuration of a database node.
type Configurations struct {
	LocalConfig  *LocalConfiguration
	SharedConfig *SharedConfiguration
	JoinBlock    *types.Block
}

// LocalConfiguration holds the local configuration of the server.
// These definitions may vary from server to server, and are defined independently for each server.
type LocalConfiguration struct {
	Server        ServerConf
	BlockCreation BlockCreationConf
	Replication   ReplicationConf
	Bootstrap     BootstrapConf
	Prometheus    PrometheusConf
}

// ReplicationConf provides local configuration parameters for replication and server to server communication.
type ReplicationConf struct {
	// WALDir defines the directory used to store the WAL of the consensus algorithm.
	WALDir string
	// SnapDir defines the directory used to store snapshots produced by the consensus algorithm.
	SnapDir string
	// AuxDir defines the directory used to store auxiliary and temporary files during replication.
	AuxDir string
	// Network defines the listen address and port used for server to server communication.
	Network NetworkConf
	// TLS defines TLS settings for server to server communication.
	TLS TLSConf
}

// TLSConf holds TLS configuration settings.
type TLSConf struct {
	// Require server-side TLS.
	Enabled bool
	// Require client certificates / mutual TLS for inbound connections.
	ClientAuthRequired bool
	// X.509 certificate used for TLS server
	ServerCertificatePath string
	// Private key for TLS server
	ServerKeyPath string
	// X.509 certificate used for creating TLS client connections.
	ClientCertificatePath string
	// Private key used for creating TLS client connections.
	ClientKeyPath string
	// cluster.tls.caConfig defines the paths to the x509 certificates
	// of the root and intermediate certificate authorities that issued
	// all the certificates used for intra-cluster communication.
	CaConfig CAConfiguration
}

// ServerConf holds the identity information of the local database server, along with network interface, as well as
// internal component configuration parameters.
type ServerConf struct {
	// The identity of the local node.
	Identity IdentityConf
	// The network interface and port used to serve client requests.
	Network NetworkConf
	// The database configuration of the local node.
	Database DatabaseConf
	// The provenance store configuration of the local node.
	Provenance ProvenanceConf
	// The lengths of various queues that buffer between internal components.
	QueueLength QueueLengthConf
	// QueryProcessing holds limits associated with query responses
	QueryProcessing QueryProcessingConf
	// Server logging level.
	LogLevel string
	// Server TLS configuration, for secure communication with clients.
	TLS TLSConf
}

// IdentityConf holds the ID, path to x509 certificate and the private key associated with the database node.
type IdentityConf struct {
	// A unique name that identifies the node within the cluster.
	// This corresponds to NodeConf.NodeID, and links the local server to one of the nodes defined
	// in SharedConfiguration.Nodes.
	ID string
	// Path to the certificate used to authenticate communication with clients,
	// and to verify the server's signature on blocks and request responses.
	CertificatePath string
	// Path to the private key used to authenticate communication with clients,
	// and to sign blocks and request responses.
	KeyPath string
}

// NetworkConf holds the listen address and port of an endpoint.
// See `net.Listen(network, address string)`. The `address` parameter will be the `Address`:`Port` defined below.
type NetworkConf struct {
	Address string
	Port    uint32
}

// DatabaseConf holds the name of the state database and the path where the data is stored.
type DatabaseConf struct {
	Name            string
	LedgerDirectory string
}

// QueueLengthConf holds the queue length of all queues within the node.
type QueueLengthConf struct {
	Transaction               uint32
	ReorderedTransactionBatch uint32
	Block                     uint32
}

// QueryProcessingConf holds the configuration associated with rich and range query processing.
type QueryProcessingConf struct {
	ResponseSizeLimitInBytes uint64
}

// BlockCreationConf holds the block creation parameters.
// TODO consider moving this to shared-config if we want to have it consistent across nodes
type BlockCreationConf struct {
	MaxBlockSize                uint64
	MaxTransactionCountPerBlock uint32
	BlockTimeout                time.Duration
}

// ProvenanceConf holds the provenance configuration parameters.
type ProvenanceConf struct {
	// Disabled disable the provenance store at this node.When disabled:
	// - No data is committed to the provenance store.
	// - Provenance queries return 503 (Service Unavailable).
	// Provenance can be disabled on one server but disabled on another.
	// Restarting a server with provenance switched from on to off will leave the provenance store intact, but no more
	// data will be committed to it, and queries will return 503 (Service Unavailable).
	// Restarting a server with provenance switched from off to on is not supported and will result in an error.
	Disabled bool
}

// BootstrapConf specifies the method of starting a new node with an empty ledger and database.
type BootstrapConf struct {
	// Method specifies how to use the bootstrap file:
	// - 'genesis' means to load it as the initial configuration that will be converted into the ledger's genesis block and
	//   loaded into the database when the server starts with an empty ledger.
	// - 'join' means to load it as a temporary configuration that will be used to connect to existing cluster members
	//   and on-board by fetching the ledger from them, rebuilding the database in the process (not supported yet).
	// - 'none' means the server will not load any bootstrap file. This appropriate for servers that already have a
	//   database with a valid shared configuration in them.
	Method string
	// File contains the path to initial configuration that will be used to bootstrap the node,
	// as specified by the`Method`.
	File string
}

// PrometheusConf specifies the metrics collection for monitoring and performance analysis.
type PrometheusConf struct {
	// Enabled specifies if metrics will be collected
	Enabled bool
	// Network defines the listen address and port used for the Prometheus server.
	Network NetworkConf
	// TLS defines TLS settings for the Prometheus server.
	TLS TLSConf
}

// Read reads configurations from the config file and returns the config
func Read(configFilePath string) (*Configurations, error) {
	if configFilePath == "" {
		return nil, errors.New("path to the configuration file is empty")
	}

	fileInfo, err := os.Stat(configFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read the status of the configuration path: '%s'", configFilePath)
	}

	fileName := configFilePath
	if fileInfo.IsDir() {
		fileName = path.Join(configFilePath, defaultLocalConfigFile)
	}

	conf := &Configurations{}
	conf.LocalConfig, err = readLocalConfig(fileName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read the local configuration from: '%s'", fileName)
	}

	switch conf.LocalConfig.Bootstrap.Method {
	case "genesis":
		if conf.LocalConfig.Bootstrap.File != "" {
			conf.SharedConfig, err = readSharedConfig(conf.LocalConfig.Bootstrap.File)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to read the shared configuration from: '%s'", conf.LocalConfig.Bootstrap.File)
			}
		}
	case "join":
		conf.JoinBlock, err = readJoinBlock(conf.LocalConfig.Bootstrap.File)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read the join block from: '%s'", conf.LocalConfig.Bootstrap.File)
		}

	case "none":
		return conf, nil
	default:
		return nil, errors.Errorf("unsupported bootstrap.method %s", conf.LocalConfig.Bootstrap.Method)
	}

	return conf, nil
}

// readLocalConfig reads the local config from the file and returns it.
func readLocalConfig(localConfigFile string) (*LocalConfiguration, error) {
	if localConfigFile == "" {
		return nil, errors.New("path to the local configuration file is empty")
	}

	v := viper.New()
	v.SetConfigFile(localConfigFile)

	v.SetDefault("server.database.name", "leveldb")
	v.SetDefault("server.database.ledgerDirectory", "./tmp/")
	v.SetDefault("server.queryProcessing.responseSizeLimitInBytes", 1048576)

	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrap(err, "error reading local config file")
	}

	conf := &LocalConfiguration{}
	if err := v.UnmarshalExact(conf); err != nil {
		return nil, errors.Wrapf(err, "unable to unmarshal local config file: '%s' into struct", localConfigFile)
	}
	return conf, nil
}
