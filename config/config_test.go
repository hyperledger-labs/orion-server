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
	}
	t.Run("test-server-conf", func(t *testing.T) {
		serverConf := Server()
		require.Equal(t, expectedServerConf, serverConf)
	})
	t.Run("test-network-conf", func(t *testing.T) {
		networkConf := ServerNetwork()
		require.Equal(t, &expectedServerConf.Network, networkConf)
	})
	t.Run("test-crypto-conf", func(t *testing.T) {
		cryptoConf := ServerCrypto()
		require.Equal(t, &expectedServerConf.Crypto, cryptoConf)
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
