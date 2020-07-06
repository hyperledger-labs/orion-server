package config

import (
	"log"

	"github.com/spf13/viper"
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

func init() {
	// TODO: use environment variable to find
	// the absolute path of the config file
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	viper.SetConfigType("yml")
	viper.AutomaticEnv()
	conf = &Configurations{}

	viper.SetDefault("server.database.name", "leveldb")
	viper.SetDefault("server.database.ledgerDirectory", "./tmp/")

	if err := viper.ReadInConfig(); err != nil {
		log.Printf("Error reading config file, %v\n", err)
	}

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
