// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package config

import (
	"os"
	"path"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

const (
	defaultLocalConfigFile = "config.yml"
)

// Configurations holds the complete configuration
// of a database node
type Configurations struct {
	LocalConfig  *LocalConfiguration
	SharedConfig *SharedConfiguration
}

type LocalConfiguration struct {
	Node          NodeConf
	BlockCreation BlockCreationConf
	Replication   ReplicationConf
	Bootstrap     BootstrapConf
}

type ReplicationConf struct {
	WALDir  string
	SnapDir string
	Network NetworkConf
	TLS     TLSConf
}

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

// NodeConf holds the identity information of the
// database node along with network and underlying
// state database configuration
type NodeConf struct {
	Identity    IdentityConf
	Network     NetworkConf
	Database    DatabaseConf
	Replication ReplicationConf
	QueueLength QueueLengthConf
	LogLevel    string
}

// IdentityConf holds the ID, path to x509 certificate
// and the private key associated with the database node
type IdentityConf struct {
	ID              string
	CertificatePath string
	KeyPath         string
}

// NetworkConf holds the listen address and port of
// the database node
type NetworkConf struct {
	Address string
	Port    uint32
}

// DatabaseConf holds the name of the state database
// and the path where the data is stored
type DatabaseConf struct {
	Name            string
	LedgerDirectory string
}

// QueueLengthConf holds the queue length of all
// queues within the node
type QueueLengthConf struct {
	Transaction               uint32
	ReorderedTransactionBatch uint32
	Block                     uint32
}

// BlockCreationConf holds the block creation parameters
// TODO consider moving this to shared-config if we want to have it consistent across nodes
type BlockCreationConf struct {
	MaxBlockSize                uint32
	MaxTransactionCountPerBlock uint32
	BlockTimeout                time.Duration
}

type BootstrapConf struct {
	Method string
	File   string
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
	conf.LocalConfig, err = ReadLocalConfig(fileName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read the local configuration from: '%s'", fileName)
	}

	if conf.LocalConfig.Bootstrap.File != "" {
		conf.SharedConfig, err = ReadSharedConfig(conf.LocalConfig.Bootstrap.File)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read the shared configuration from: '%s'", conf.LocalConfig.Bootstrap.File)
		}
	}

	return conf, nil
}

// ReadLocalConfig reads the local config from the file and returns it.
func ReadLocalConfig(localConfigFile string) (*LocalConfiguration, error) {
	if localConfigFile == "" {
		return nil, errors.New("path to the local configuration file is empty")
	}

	v := viper.New()
	v.SetConfigFile(localConfigFile)

	v.SetDefault("node.database.name", "leveldb")
	v.SetDefault("node.database.ledgerDirectory", "./tmp/")

	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrapf(err, "error reading local config file: %s", localConfigFile)
	}

	conf := &LocalConfiguration{}
	if err := v.UnmarshalExact(conf); err != nil {
		return nil, errors.Wrapf(err, "unable to unmarshal local config file: '%s' into struct", localConfigFile)
	}
	return conf, nil
}
