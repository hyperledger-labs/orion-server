package config

import (
	"log"
	"os"

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
// of the database server
type Configurations struct {
	Server ServerConf
	RootCa RootCAConf
	Admin  AdminConf
}

// ServerConf holds the identity information of the
// database server along with network and underlying
// state database configuration
type ServerConf struct {
	ID       string
	Network  NetworkConf
	Identity IdentityConf
	Database DatabaseConf
}

// NetworkConf holds the listen address and port of
// the database server
type NetworkConf struct {
	Address string
	Port    int
}

// IdentityConf holds the path to x509 certificate
// and the private key associated with the database server
type IdentityConf struct {
	Certificate string
	Key         string
}

// DatabaseConf holds the name of the state database
// and the absolute path where the data is stored
type DatabaseConf struct {
	Name            string
	LedgerDirectory string
}

// RootCAConf holds the absolute path to the
// x509 certificate of the certificate authority
// who issues users' certificate
type RootCAConf struct {
	Certificate string
}

// AdminConf holds the credentials of the database
// server admin such as the username and absolute
// path to the x509 certificate
type AdminConf struct {
	Username    string
	DBName      string // TODO: remove
	Certificate string
}

var conf *Configurations

// Init initializes Configurations by reading the config file
func Init() error {
	path := os.Getenv(PathEnv)
	if path == "" {
		log.Printf(
			"Server config path environment %s is empty. Using the %s.%s located in the current directory if exist.",
			PathEnv,
			name,
			filetype,
		)
		path = "./"
	}

	viper.SetConfigName(name)
	viper.AddConfigPath(path)
	viper.SetConfigType(filetype)
	viper.AutomaticEnv()

	viper.SetDefault("server.database.name", "leveldb")
	viper.SetDefault("server.database.ledgerDirectory", "./tmp/")

	if err := viper.ReadInConfig(); err != nil {
		return errors.Wrapf(err, "error reading config file")
	}

	conf = &Configurations{}

	if err := viper.GetViper().UnmarshalExact(conf); err != nil {
		return errors.Wrapf(err, "unable to unmarshal config file into struct")
	}
	return nil
}

// Server returns the server configuration
func Server() *ServerConf {
	return &conf.Server
}

// ServerNetwork returns the network configuration
func ServerNetwork() *NetworkConf {
	return &conf.Server.Network
}

// ServerIdentity returns the identity information
func ServerIdentity() *IdentityConf {
	return &conf.Server.Identity
}

// Database returns the database configuration
func Database() *DatabaseConf {
	return &conf.Server.Database
}

// RootCA returns the root certificate authority
func RootCA() *RootCAConf {
	return &conf.RootCa
}

// Admin returns the admin configuration
func Admin() *AdminConf {
	return &conf.Admin
}
