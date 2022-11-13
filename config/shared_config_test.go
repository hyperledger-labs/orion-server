// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var expectedSharedConfig = &SharedConfiguration{
	Nodes: []*NodeConf{
		{
			NodeID:          "bcdb-node1",
			Host:            "bcdb1.example.com",
			Port:            6001,
			CertificatePath: "./testdata/cluster/bcdb-node1/node.cert",
		},
		{
			NodeID:          "bcdb-node2",
			Host:            "bcdb2.example.com",
			Port:            6001,
			CertificatePath: "./testdata/cluster/bcdb-node2/node.cert",
		},
		{
			NodeID:          "bcdb-node3",
			Host:            "bcdb3.example.com",
			Port:            6001,
			CertificatePath: "./testdata/cluster/bcdb-node3/node.cert",
		},
	},
	Consensus: &ConsensusConf{
		Algorithm: "raft",
		Members: []*PeerConf{
			{
				NodeId:   "bcdb-node1",
				RaftId:   1,
				PeerHost: "raft1.example.com",
				PeerPort: 7050,
			},
			{
				NodeId:   "bcdb-node2",
				RaftId:   2,
				PeerHost: "raft2.example.com",
				PeerPort: 7050,
			},
			{
				NodeId:   "bcdb-node3",
				RaftId:   3,
				PeerHost: "raft3.example.com",
				PeerPort: 7050,
			},
		},
		Observers: []*PeerConf{
			{
				NodeId:   "bcdb-node4",
				RaftId:   0,
				PeerHost: "raft4.example.com",
				PeerPort: 7050,
			},
		},
		RaftConfig: &RaftConf{
			TickInterval:         "100ms",
			ElectionTicks:        50,
			HeartbeatTicks:       5,
			MaxInflightBlocks:    50,
			SnapshotIntervalSize: 1000000000000,
		},
	},
	CAConfig: CAConfiguration{
		RootCACertsPath:         []string{"./testdata/rootca.cert"},
		IntermediateCACertsPath: []string{"./testdata/midca.cert"},
	},
	Admin: AdminConf{
		ID:              "admin",
		CertificatePath: "./testdata/admin.cert",
	},
	Ledger: LedgerConf{
		StateMerklePatriciaTrieDisabled: false,
	},
}

func TestSharedConfig(t *testing.T) {
	t.Run("successful", func(t *testing.T) {
		config, err := readSharedConfig("./testdata/3node-shared-config-bootstrap.yml")
		require.NoError(t, err)
		require.Equal(t, expectedSharedConfig, config)
		require.False(t, config.Ledger.StateMerklePatriciaTrieDisabled)
	})

	t.Run("successful: mp-trie disabled", func(t *testing.T) {
		config, err := readSharedConfig("./testdata/3node-shared-config-bootstrap-mptrie-disabled.yml")
		require.NoError(t, err)
		expectedSharedConfig.Ledger.StateMerklePatriciaTrieDisabled = true
		require.Equal(t, expectedSharedConfig, config)
		expectedSharedConfig.Ledger.StateMerklePatriciaTrieDisabled = false
	})

	t.Run("empty-config-path", func(t *testing.T) {
		config, err := readSharedConfig("")
		require.EqualError(t, err, "path to the shared configuration file is empty")
		require.Nil(t, config)
	})

	t.Run("missing-config-file", func(t *testing.T) {
		config, err := readSharedConfig("/abc.yml")
		require.EqualError(t, err, "error reading shared config file: /abc.yml: open /abc.yml: no such file or directory")
		require.Nil(t, config)
	})

	t.Run("unmarshal-error", func(t *testing.T) {
		config, err := readSharedConfig("./testdata/bad-shared-config-bootstrap.yml")
		require.EqualError(t, err, "unable to unmarshal shared config file: './testdata/bad-shared-config-bootstrap.yml' into struct: 2 error(s) decoding:\n\n* '' has invalid keys: admiiiin, clusterrrrrr\n* 'Consensus' has invalid keys: algorithmmmmm")
		require.Nil(t, config)
	})
}
