package config

import (
	"log"
	"os"

	"github.com/spf13/viper"
)

const (
	PathEnv = "BCDB_CONFIG_PATH"

	name     = "config"
	filetype = "yml"
)

type Configurations struct {
	Server ServerConf
	RootCa RootCAConf
	Admin  AdminConf
}

type ServerConf struct {
	ID       string
	Network  NetworkConf
	Identity IdentityConf
	Database DatabaseConf
}

type NetworkConf struct {
	Address string
	Port    int
}

type IdentityConf struct {
	Certificate string
	Key         string
}

type DatabaseConf struct {
	Name            string
	LedgerDirectory string
}

type RootCAConf struct {
	Certificate string
}

type AdminConf struct {
	Username    string
	DBName      string
	Certificate string
}

var conf *Configurations

func Init() {
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
		log.Fatalf("Error reading config file, %v\n", err)
	}

	conf = &Configurations{}
	if err := viper.GetViper().UnmarshalExact(conf); err != nil {
		log.Fatalf("Unable to decode into struct, %v", err)
	}
}

func Server() *ServerConf {
	return &conf.Server
}

func ServerNetwork() *NetworkConf {
	return &conf.Server.Network
}

func ServerIdentity() *IdentityConf {
	return &conf.Server.Identity
}

func Database() *DatabaseConf {
	return &conf.Server.Database
}

func RootCA() *RootCAConf {
	return &conf.RootCa
}

func Admin() *AdminConf {
	return &conf.Admin
}
