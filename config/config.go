package config

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

const (
	// PathEnv is an environment variable that can hold
	// the absolute path of the config file
	PathEnv = "BCDB_CONFIG_PATH"

	name     = "config"
	filetype = "yml"
)

// Configurations holds the complete configuration
// of a database node
type Configurations struct {
	Node   NodeConf
	Admin  AdminConf
	RootCA RootCAConf
}

// NodeConf holds the identity information of the
// database node along with network and underlying
// state database configuration
type NodeConf struct {
	Identity IdentityConf
	Network  NetworkConf
	Database DatabaseConf
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

var conf *Configurations
var configPath string

// Init initializes Configurations by reading the config file
func Init() error {
	configPath = os.Getenv(PathEnv)
	if configPath == "" {
		log.Printf(
			"node config path environment %s is empty. Using the %s.%s located in the current directory if exist.",
			PathEnv,
			name,
			filetype,
		)
		configPath = "./"
	}

	viper.SetConfigName(name)
	viper.AddConfigPath(configPath)
	viper.SetConfigType(filetype)
	viper.AutomaticEnv()

	viper.SetDefault("node.database.name", "leveldb")
	viper.SetDefault("node.database.ledgerDirectory", "./tmp/")

	if err := viper.ReadInConfig(); err != nil {
		return errors.Wrapf(err, "error reading config file")
	}

	conf = &Configurations{}

	if err := viper.GetViper().UnmarshalExact(conf); err != nil {
		return errors.Wrapf(err, "unable to unmarshal config file into struct")
	}
	return nil
}

// Node returns the node configuration
func Node() *NodeConf {
	return &conf.Node
}

// NodeNetwork returns the network configuration
func NodeNetwork() *NetworkConf {
	return &conf.Node.Network
}

// NodeIdentity returns the identity information
func NodeIdentity() *IdentityConf {
	return &conf.Node.Identity
}

// Database returns the database configuration
func Database() *DatabaseConf {
	return &conf.Node.Database
}

// RootCA returns the root certificate authority
func RootCA() *RootCAConf {
	return &conf.RootCA
}

// Admin returns the admin configuration
func Admin() *AdminConf {
	return &conf.Admin
}

// Certificates holds the certificate content of the
// node, admin, and the root CA
type Certificates struct {
	Node   []byte
	Admin  []byte
	RootCA []byte
}

// Certs retuns certificate content of the node, admin,
// and the root CA
func Certs() (*Certificates, error) {
	certsPath := []string{
		conf.Node.Identity.CertificatePath,
		conf.Admin.CertificatePath,
		conf.RootCA.CertificatePath,
	}

	for i, path := range certsPath {
		if !filepath.IsAbs(path) {
			certsPath[i] = filepath.Join(configPath, path)
		}
	}

	certsContent := [][]byte{}
	for _, path := range certsPath {
		content, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, errors.Wrapf(err, "error while reading file %s", path)
		}
		certsContent = append(certsContent, content)
	}

	return &Certificates{
		Node:   certsContent[0],
		Admin:  certsContent[1],
		RootCA: certsContent[2],
	}, nil
}
