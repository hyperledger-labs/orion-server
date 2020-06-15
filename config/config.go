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
	ID      string
	Network NetworkConf
	Crypto  CryptoConf
}

type NetworkConf struct {
	Address string
	Port    int
}

type CryptoConf struct {
	Certificate string
	Key         string
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

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file, %s", err)
	}

	err := viper.Unmarshal(conf)
	if err != nil {
		log.Fatalf("Unable to decode into struct, %v", err)
	}
}

func Server() *ServerConf {
	return &conf.Server
}

func ServerNetwork() *NetworkConf {
	return &conf.Server.Network
}

func ServerCrypto() *CryptoConf {
	return &conf.Server.Crypto
}

func RootCA() *RootCAConf {
	return &conf.RootCa
}

func Admin() *AdminConf {
	return &conf.Admin
}
