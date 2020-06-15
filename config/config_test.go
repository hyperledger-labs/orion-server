package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServerConfig(t *testing.T) {
	expectedServerConf := &ServerConf{
		ID: "bdb-node-1",
		Network: NetworkConf{
			Address: "localhost",
			Port:    6001,
		},
		Crypto: CryptoConf{
			Certificate: "peer1.crt",
			Key:         "peer1.key",
		},
		Database: DatabaseConf{
			Name:            "leveldb",
			LedgerDirectory: "./tmp/",
		},
	}

	t.Run("test-server-conf", func(t *testing.T) {
		t.Parallel()
		serverConf := Server()
		require.Equal(t, expectedServerConf, serverConf)
	})

	t.Run("test-network-conf", func(t *testing.T) {
		t.Parallel()
		networkConf := ServerNetwork()
		require.Equal(t, &expectedServerConf.Network, networkConf)
	})

	t.Run("test-crypto-conf", func(t *testing.T) {
		t.Parallel()
		cryptoConf := ServerCrypto()
		require.Equal(t, &expectedServerConf.Crypto, cryptoConf)
	})

	t.Run("test-database-conf", func(t *testing.T) {
		t.Parallel()
		databaseConf := Database()
		require.Equal(t, &expectedServerConf.Database, databaseConf)
	})
}

func TestRootCAConfig(t *testing.T) {
	expectedRootCAConfig := &RootCAConf{
		Certificate: "ca.crt",
	}
	rootCAConf := RootCA()
	require.Equal(t, expectedRootCAConfig, rootCAConf)
}

func TestAdminConfig(t *testing.T) {
	expectedAdminConfig := &AdminConf{
		Username:    "admin",
		DBName:      "admin",
		Certificate: "admin.crt",
	}
	adminConf := Admin()
	require.Equal(t, expectedAdminConfig, adminConf)
}
