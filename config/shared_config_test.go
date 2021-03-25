// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package config_test

import (
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/config"
	"testing"
)

var expectedSharedConfig = &config.SharedConfiguration{
	Nodes: []config.DBNodeConf{
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
	Consensus: config.ConsensusConf{
		Algorithm: "raft",
		Members: []config.PeerConf{
			{
				NodeID:   "bcdb-node1",
				RaftID:   1,
				PeerHost: "raft1.example.com",
				PeerPort: 7050,
			},
			{
				NodeID:   "bcdb-node2",
				RaftID:   2,
				PeerHost: "raft2.example.com",
				PeerPort: 7050,
			},
			{
				NodeID:   "bcdb-node3",
				RaftID:   3,
				PeerHost: "raft3.example.com",
				PeerPort: 7050,
			},
		},
		Observers: []config.PeerConf{
			{
				NodeID:   "bcdb-node4",
				RaftID:   0,
				PeerHost: "raft4.example.com",
				PeerPort: 7050,
			},
		},
		Raft: config.RaftConf{
			TickInterval:   100000000,
			ElectionTicks:  50,
			HeartbeatTicks: 5,
		},
	},
	CAConfig: config.CAConfiguration{
		RootCACertsPath:         []string{"./testdata/rootca.cert"},
		IntermediateCACertsPath: []string{"./testdata/midca.cert"},
	},
	Admin: config.AdminConf{
		ID:              "admin",
		CertificatePath: "./testdata/admin.cert",
	},
}

func TestSharedConfig(t *testing.T) {
	t.Run("successful", func(t *testing.T) {
		config, err := config.ReadSharedConfig("./testdata/3node-shared-config-bootstrap.yml")
		require.NoError(t, err)
		require.Equal(t, expectedSharedConfig, config)
	})

	t.Run("empty-config-path", func(t *testing.T) {
		config, err := config.ReadSharedConfig("")
		require.EqualError(t, err, "path to the shared configuration file is empty")
		require.Nil(t, config)
	})

	t.Run("missing-config-file", func(t *testing.T) {
		config, err := config.ReadSharedConfig("/abc.yml")
		require.EqualError(t, err, "error reading shared config file: /abc.yml: open /abc.yml: no such file or directory")
		require.Nil(t, config)
	})

	t.Run("unmarshal-error", func(t *testing.T) {
		config, err := config.ReadSharedConfig("./testdata/bad-shared-config-bootstrap.yml")
		require.EqualError(t, err, "unable to unmarshal shared config file: './testdata/bad-shared-config-bootstrap.yml' into struct: 2 error(s) decoding:\n\n* '' has invalid keys: admiiiin, clusterrrrrr\n* 'Consensus' has invalid keys: algorithmmmmm")
		require.Nil(t, config)
	})
}
