// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/config"
)

var expectedLocalConfig = &config.LocalConfiguration{
	Node: config.NodeConf{
		Identity: config.IdentityConf{
			ID:              "bdb-node-1",
			CertificatePath: "./testdata/node.cert",
			KeyPath:         "./testdata/node.key",
		},
		Network: config.NetworkConf{
			Address: "127.0.0.1",
			Port:    6001,
		},
		Database: config.DatabaseConf{
			Name:            "leveldb",
			LedgerDirectory: "./tmp/",
		},
		QueueLength: config.QueueLengthConf{
			Transaction:               1000,
			ReorderedTransactionBatch: 100,
			Block:                     100,
		},
		LogLevel: "info",
	},
	BlockCreation: config.BlockCreationConf{
		MaxBlockSize:                2,
		MaxTransactionCountPerBlock: 1,
		BlockTimeout:                50 * time.Millisecond,
	},
	Replication: config.ReplicationConf{
		WALDir:  "./tmp/etcdraft/wal",
		SnapDir: "./tmp/etcdraft/snapshot",
		Network: config.NetworkConf{
			Address: "127.0.0.1",
			Port:    7050,
		},
		TLS: config.TLSConf{
			Enabled:               false,
			ClientAuthRequired:    false,
			ServerCertificatePath: "./testdata/cluster/server.cert",
			ServerKeyPath:         "./testdata/cluster/server.key",
			ClientCertificatePath: "./testdata/cluster/client.cert",
			ClientKeyPath:         "./testdata/cluster/client.key",
			CaConfig: config.CAConfiguration{
				RootCACertsPath:         []string{"./testdata/cluster/rootca.cert"},
				IntermediateCACertsPath: []string{"./testdata/cluster/midca.cert"},
			},
		},
	},
	Bootstrap: config.BootstrapConf{
		Method: "genesis",
		File:   "./testdata/3node-shared-config-bootstrap.yml",
	},
}

func TestConfig(t *testing.T) {
	t.Parallel()

	t.Run("successful", func(t *testing.T) {
		t.Parallel()

		config, err := config.Read("./testdata")
		require.NoError(t, err)
		require.Equal(t, expectedLocalConfig, config.LocalConfig)
		require.Equal(t, expectedSharedConfig, config.SharedConfig)
	})

	t.Run("empty-config-path", func(t *testing.T) {
		t.Parallel()
		config, err := config.Read("")
		require.EqualError(t, err, "path to the configuration file is empty")
		require.Nil(t, config)
	})

	t.Run("missing-config-file", func(t *testing.T) {
		t.Parallel()
		config, err := config.Read("/abc")
		require.EqualError(t, err, "failed to read the status of the configuration path: '/abc': stat /abc: no such file or directory")
		require.Nil(t, config)
	})

	t.Run("unmarshal-error", func(t *testing.T) {
		t.Parallel()
		config, err := config.Read("./testdata/bad-local-config.yml")
		require.EqualError(t, err, "failed to read the local configuration from: './testdata/bad-local-config.yml': unable to unmarshal local config file: './testdata/bad-local-config.yml' into struct: 1 error(s) decoding:\n\n* cannot parse 'Node.Network.Port' as uint: strconv.ParseUint: parsing \"abcd\": invalid syntax")
		require.Nil(t, config)
	})
}

func TestLocalConfig(t *testing.T) {
	t.Run("successful", func(t *testing.T) {
		config, err := config.ReadLocalConfig("./testdata/config.yml")
		require.NoError(t, err)
		require.Equal(t, expectedLocalConfig, config)
	})

	t.Run("empty-config-path", func(t *testing.T) {
		config, err := config.ReadLocalConfig("")
		require.EqualError(t, err, "path to the local configuration file is empty")
		require.Nil(t, config)
	})

	t.Run("missing-config-file", func(t *testing.T) {
		config, err := config.ReadLocalConfig("/abc.yml")
		require.EqualError(t, err, "error reading local config file: /abc.yml: open /abc.yml: no such file or directory")
		require.Nil(t, config)
	})

	t.Run("unmarshal-error", func(t *testing.T) {
		config, err := config.ReadLocalConfig("./testdata/3node-shared-config-bootstrap.yml")
		require.EqualError(t, err, "unable to unmarshal local config file: './testdata/3node-shared-config-bootstrap.yml' into struct: 1 error(s) decoding:\n\n* '' has invalid keys: admin, caconfig, consensus, nodes")
		require.Nil(t, config)
	})
}
