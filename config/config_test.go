package config

import (
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	if err := Init(); err != nil {
		log.Fatalf("error while initializing configuration, %v", err)
	}
	os.Exit(m.Run())
}

func TestNodeConfig(t *testing.T) {
	expectedNodeConf := &NodeConf{
		Network: NetworkConf{
			Address: "127.0.0.1",
			Port:    6001,
		},
		Identity: IdentityConf{
			ID:              "bdb-node-1",
			CertificatePath: "./testdata/node.cert",
			KeyPath:         "./testdata/node.key",
		},
		Database: DatabaseConf{
			Name:            "leveldb",
			LedgerDirectory: "./tmp/",
		},
	}

	t.Run("test-Node-conf", func(t *testing.T) {
		t.Parallel()
		NodeConf := Node()
		require.Equal(t, expectedNodeConf, NodeConf)
	})

	t.Run("test-network-conf", func(t *testing.T) {
		t.Parallel()
		networkConf := NodeNetwork()
		require.Equal(t, &expectedNodeConf.Network, networkConf)
	})

	t.Run("test-crypto-conf", func(t *testing.T) {
		t.Parallel()
		cryptoConf := NodeIdentity()
		require.Equal(t, &expectedNodeConf.Identity, cryptoConf)
	})

	t.Run("test-database-conf", func(t *testing.T) {
		t.Parallel()
		databaseConf := Database()
		require.Equal(t, &expectedNodeConf.Database, databaseConf)
	})
}

func TestRootCAConfig(t *testing.T) {
	expectedRootCAConfig := &RootCAConf{
		CertificatePath: "./testdata/rootca.cert",
	}
	rootCAConf := RootCA()
	require.Equal(t, expectedRootCAConfig, rootCAConf)
}

func TestAdminConfig(t *testing.T) {
	expectedAdminConfig := &AdminConf{
		ID:              "admin",
		CertificatePath: "./testdata/admin.cert",
	}
	adminConf := Admin()
	require.Equal(t, expectedAdminConfig, adminConf)
}

func TestCerts(t *testing.T) {
	t.Run("successfully-returns", func(t *testing.T) {
		t.Parallel()
		c, err := Certs()
		require.NoError(t, err)
		require.NotNil(t, c.Node)
		require.NotNil(t, c.Admin)
		require.NotNil(t, c.RootCA)
	})

	t.Run("error-due-to-missing-files", func(t *testing.T) {
		t.Parallel()
		conf.Node.Identity.CertificatePath = "/"
		c, err := Certs()
		require.Contains(t, err.Error(), "error while reading file /")
		require.Nil(t, c)
	})
}
