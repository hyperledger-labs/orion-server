// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	t.Parallel()

	t.Run("successful", func(t *testing.T) {
		t.Parallel()
		expectedConfigurations := &Configurations{
			Node: NodeConf{
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
				QueueLength: QueueLengthConf{
					Transaction:               1000,
					ReorderedTransactionBatch: 100,
					Block:                     100,
				},
				LogLevel: "info",
			},
			Consensus: ConsensusConf{
				Algorithm:                   "raft",
				MaxBlockSize:                2,
				MaxTransactionCountPerBlock: 1,
				BlockTimeout:                50 * time.Millisecond,
			},
			Admin: AdminConf{
				ID:              "admin",
				CertificatePath: "./testdata/admin.cert",
			},
			CAConfig: CAConfiguration{
				RootCACertsPath: []string{"./testdata/rootca.cert"},
			},
		}

		config, err := Read("./")
		require.NoError(t, err)
		require.Equal(t, expectedConfigurations, config)
	})

	t.Run("empty-config-path", func(t *testing.T) {
		t.Parallel()
		config, err := Read("")
		require.Contains(t, err.Error(), "path to the configuration file is empty")
		require.Nil(t, config)
	})

	t.Run("missing-config-file", func(t *testing.T) {
		t.Parallel()
		config, err := Read("/abc")
		require.Contains(t, err.Error(), "error reading config file: Config File \"config\" Not Found")
		require.Nil(t, config)
	})

	t.Run("unmarshal-error", func(t *testing.T) {
		t.Parallel()
		config, err := Read("./testdata")
		require.Contains(t, err.Error(), "cannot parse 'Node.Network.Port' as uint: strconv.ParseUint: parsing \"abcd\": invalid syntax")
		require.Nil(t, config)
	})
}
