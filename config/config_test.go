// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var expectedLocalConfig = &LocalConfiguration{
	Server: ServerConf{
		Identity: IdentityConf{
			ID:              "bdb-node-1",
			CertificatePath: "./testdata/node.cert",
			KeyPath:         "./testdata/node.key",
		},
		Network: NetworkConf{
			Address: "127.0.0.1",
			Port:    6001,
		},
		Database: DatabaseConf{
			Name:            "leveldb",
			LedgerDirectory: "./tmp/",
		},
		Provenance: ProvenanceConf{
			Disabled: true,
		},
		QueueLength: QueueLengthConf{
			Transaction:               1000,
			ReorderedTransactionBatch: 100,
			Block:                     100,
		},
		QueryProcessing: QueryProcessingConf{
			ResponseSizeLimitInBytes: 1048576,
		},
		LogLevel: "info",
		TLS: TLSConf{
			Enabled:               false,
			ClientAuthRequired:    false,
			ServerCertificatePath: "./testdata/tls/server/server.cert",
			ServerKeyPath:         "./testdata/tls/server/server.key",
			CaConfig: CAConfiguration{
				RootCACertsPath:         []string{"./testdata/tls/CA/rootca.cert"},
				IntermediateCACertsPath: []string{"./testdata/tls/CA/midca.cert"},
			},
		},
	},
	BlockCreation: BlockCreationConf{
		MaxBlockSize:                2,
		MaxTransactionCountPerBlock: 1,
		BlockTimeout:                50 * time.Millisecond,
	},
	Replication: ReplicationConf{
		WALDir:  "./tmp/etcdraft/wal",
		SnapDir: "./tmp/etcdraft/snapshot",
		AuxDir:  "./tmp/orion/auxiliary",
		Network: NetworkConf{
			Address: "127.0.0.1",
			Port:    7050,
		},
		TLS: TLSConf{
			Enabled:               false,
			ClientAuthRequired:    false,
			ServerCertificatePath: "./testdata/cluster/server.cert",
			ServerKeyPath:         "./testdata/cluster/server.key",
			ClientCertificatePath: "./testdata/cluster/client.cert",
			ClientKeyPath:         "./testdata/cluster/client.key",
			CaConfig: CAConfiguration{
				RootCACertsPath:         []string{"./testdata/cluster/rootca.cert"},
				IntermediateCACertsPath: []string{"./testdata/cluster/midca.cert"},
			},
		},
	},
	Bootstrap: BootstrapConf{
		Method: "genesis",
		File:   "./testdata/3node-shared-config-bootstrap.yml",
	},
	Prometheus: PrometheusConf{
		Enabled: true,
		Network: NetworkConf{
			Address: "127.0.0.1",
			Port:    8050,
		},
		TLS: TLSConf{
			Enabled:               false,
			ClientAuthRequired:    false,
			ServerCertificatePath: "./testdata/cluster/server.cert",
			ServerKeyPath:         "./testdata/cluster/server.key",
			CaConfig: CAConfiguration{
				RootCACertsPath:         []string{"./testdata/cluster/rootca.cert"},
				IntermediateCACertsPath: []string{"./testdata/cluster/midca.cert"},
			},
		},
	},
}

func TestConfig(t *testing.T) {
	t.Parallel()

	t.Run("successful genesis", func(t *testing.T) {
		t.Parallel()

		config, err := Read("./testdata")
		require.NoError(t, err)
		require.Equal(t, expectedLocalConfig, config.LocalConfig)
		require.Equal(t, expectedSharedConfig, config.SharedConfig)
	})

	t.Run("successful join", func(t *testing.T) {
		t.Parallel()

		config, err := Read("./testdata/config-join.yml")
		require.NoError(t, err)
		require.NotNil(t, config.LocalConfig)
		require.Nil(t, config.SharedConfig)
		require.NotNil(t, config.JoinBlock)
		require.Equal(t, uint64(10), config.JoinBlock.GetHeader().GetBaseHeader().GetNumber())
	})

	t.Run("empty-config-path", func(t *testing.T) {
		t.Parallel()
		config, err := Read("")
		require.EqualError(t, err, "path to the configuration file is empty")
		require.Nil(t, config)
	})

	t.Run("missing-config-file", func(t *testing.T) {
		t.Parallel()
		config, err := Read("/abc")
		require.EqualError(t, err, "failed to read the status of the configuration path: '/abc': stat /abc: no such file or directory")
		require.Nil(t, config)
	})

	t.Run("unmarshal-error", func(t *testing.T) {
		t.Parallel()
		config, err := Read("./testdata/bad-local-config.yml")
		require.EqualError(t, err, "failed to read the local configuration from: './testdata/bad-local-config.yml': unable to unmarshal local config file: './testdata/bad-local-config.yml' into struct: 1 error(s) decoding:\n\n* cannot parse 'Server.Network.Port' as uint: strconv.ParseUint: parsing \"abcd\": invalid syntax")
		require.Nil(t, config)
	})
}

func TestLocalConfig(t *testing.T) {
	t.Run("successful", func(t *testing.T) {
		config, err := readLocalConfig("./testdata/config.yml")
		require.NoError(t, err)
		require.Equal(t, expectedLocalConfig, config)
	})

	t.Run("empty-config-path", func(t *testing.T) {
		config, err := readLocalConfig("")
		require.EqualError(t, err, "path to the local configuration file is empty")
		require.Nil(t, config)
	})

	t.Run("missing-config-file", func(t *testing.T) {
		config, err := readLocalConfig("/abc.yml")
		require.EqualError(t, err, "error reading local config file: open /abc.yml: no such file or directory")
		require.Nil(t, config)
	})

	t.Run("unmarshal-error", func(t *testing.T) {
		config, err := readLocalConfig("./testdata/3node-shared-config-bootstrap.yml")
		require.EqualError(t, err, "unable to unmarshal local config file: './testdata/3node-shared-config-bootstrap.yml' into struct: 1 error(s) decoding:\n\n* '' has invalid keys: admin, caconfig, consensus, nodes")
		require.Nil(t, config)
	})
}
