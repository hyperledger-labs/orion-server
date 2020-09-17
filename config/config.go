package config

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

const (
	name     = "config"
	filetype = "yml"
)

// Configurations holds the complete configuration
// of a database node
type Configurations struct {
	Node      NodeConf
	Consensus ConsensusConf
	Admin     AdminConf
	RootCA    RootCAConf
}

// NodeConf holds the identity information of the
// database node along with network and underlying
// state database configuration
type NodeConf struct {
	Identity    IdentityConf
	Network     NetworkConf
	Database    DatabaseConf
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

// ConsensusConf holds the employed consensus algorithm
// along with block size in MB and block timeout in
// milliseconds
type ConsensusConf struct {
	Algorithm                   string
	MaxBlockSize                uint32
	MaxTransactionCountPerBlock uint32
	BlockTimeout                time.Duration
}

// AdminConf holds the credentials of the blockchain
// database cluster admin such as the ID and path to
// the x509 certificate
type AdminConf struct {
	ID              string
	CertificatePath string
}

// RootCAConf holds the path to the
// x509 certificate of the certificate authority
// who issues all certificates
type RootCAConf struct {
	CertificatePath string
}

// Read reads configurations from the config file and returns the config
func Read(configFilePath string) (*Configurations, error) {
	if configFilePath == "" {
		return nil, errors.New("path to the configuration file is empty")
	}

	v := viper.New()
	v.AddConfigPath(configFilePath)
	v.SetConfigName(name)
	v.SetConfigType(filetype)

	v.SetDefault("node.database.name", "leveldb")
	v.SetDefault("node.database.ledgerDirectory", "./tmp/")

	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrap(err, "error reading config file")
	}

	conf := &Configurations{}
	if err := v.UnmarshalExact(conf); err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal config file into struct")
	}
	return conf, nil
}
