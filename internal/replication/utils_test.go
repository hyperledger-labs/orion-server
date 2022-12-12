// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestClassifyClusterReConfig(t *testing.T) {
	clusterConfig := testClusterConfig()

	t.Run("nodes changed", func(t *testing.T) {
		updatedConfig := proto.Clone(clusterConfig).(*types.ClusterConfig)
		updatedConfig.Nodes[0].Port++

		nodes, consensus, ca, admins, _ := ClassifyClusterReConfig(clusterConfig, updatedConfig)
		require.True(t, nodes)
		require.False(t, consensus)
		require.False(t, ca)
		require.False(t, admins)
	})

	t.Run("consensus changed", func(t *testing.T) {
		updatedConfig := proto.Clone(clusterConfig).(*types.ClusterConfig)
		updatedConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize++

		nodes, consensus, ca, admins, _ := ClassifyClusterReConfig(clusterConfig, updatedConfig)
		require.False(t, nodes)
		require.True(t, consensus)
		require.False(t, ca)
		require.False(t, admins)
	})

	t.Run("ca changed", func(t *testing.T) {
		updatedConfig := proto.Clone(clusterConfig).(*types.ClusterConfig)
		updatedConfig.CertAuthConfig.Roots = append(updatedConfig.CertAuthConfig.Roots, []byte("root-certificate-2"))

		nodes, consensus, ca, admins, _ := ClassifyClusterReConfig(clusterConfig, updatedConfig)
		require.False(t, nodes)
		require.False(t, consensus)
		require.True(t, ca)
		require.False(t, admins)
	})

	t.Run("admins changed", func(t *testing.T) {
		updatedConfig := proto.Clone(clusterConfig).(*types.ClusterConfig)
		updatedConfig.Admins = append(updatedConfig.Admins, &types.Admin{
			Id:          "admin2",
			Certificate: []byte("admin2-certificate"),
		})

		nodes, consensus, ca, admins, _ := ClassifyClusterReConfig(clusterConfig, updatedConfig)
		require.False(t, nodes)
		require.False(t, consensus)
		require.False(t, ca)
		require.True(t, admins)
	})
}

func TestVerifyConsensusReConfig(t *testing.T) {
	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	lg, err = logger.New(c)
	require.NoError(t, err)

	clusterConfig := testClusterConfig()

	t.Run("valid (todo): add a peer", func(t *testing.T) {
		updateConfig := proto.Clone(clusterConfig.ConsensusConfig).(*types.ConsensusConfig)
		updateConfig.Members = append(updateConfig.Members, &types.PeerConfig{
			NodeId:   "node4",
			RaftId:   6,
			PeerHost: "127.0.0.1",
			PeerPort: 7094,
		})
		err := VerifyConsensusReConfig(clusterConfig.ConsensusConfig, updateConfig, lg)
		require.NoError(t, err)
	})

	t.Run("valid: remove a peer", func(t *testing.T) {
		updateConfig := proto.Clone(clusterConfig.ConsensusConfig).(*types.ConsensusConfig)
		updateConfig.Members = updateConfig.Members[0:2]
		err := VerifyConsensusReConfig(clusterConfig.ConsensusConfig, updateConfig, lg)
		require.NoError(t, err)
	})

	t.Run("valid: change endpoints", func(t *testing.T) {
		updateConfig := proto.Clone(clusterConfig.ConsensusConfig).(*types.ConsensusConfig)
		for i := 0; i < len(updateConfig.Members); i++ {
			updateConfig.Members[i].PeerPort = updateConfig.Members[i].PeerPort + 1000
			err := VerifyConsensusReConfig(clusterConfig.ConsensusConfig, updateConfig, lg)
			require.NoError(t, err)
		}
	})

	t.Run("valid: change raft config", func(t *testing.T) {
		updateConfig := proto.Clone(clusterConfig.ConsensusConfig).(*types.ConsensusConfig)
		updateConfig.RaftConfig.SnapshotIntervalSize++
		err := VerifyConsensusReConfig(clusterConfig.ConsensusConfig, updateConfig, lg)
		require.NoError(t, err)
	})

	t.Run("invalid: too many adds", func(t *testing.T) {
		updateConfig := proto.Clone(clusterConfig.ConsensusConfig).(*types.ConsensusConfig)
		updateConfig.Members = append(updateConfig.Members, &types.PeerConfig{
			NodeId:   "node4",
			RaftId:   6,
			PeerHost: "127.0.0.1",
			PeerPort: 7094,
		})
		updateConfig.Members = append(updateConfig.Members, &types.PeerConfig{
			NodeId:   "node5",
			RaftId:   7,
			PeerHost: "127.0.0.1",
			PeerPort: 7095,
		})

		err := VerifyConsensusReConfig(clusterConfig.ConsensusConfig, updateConfig, lg)
		require.EqualError(t, err, "cannot make more than one membership change at a time: 2 added, 0 removed")
	})

	t.Run("invalid: too many removals", func(t *testing.T) {
		updateConfig := proto.Clone(clusterConfig.ConsensusConfig).(*types.ConsensusConfig)
		updateConfig.Members = updateConfig.Members[0:1]
		err := VerifyConsensusReConfig(clusterConfig.ConsensusConfig, updateConfig, lg)
		require.EqualError(t, err, "cannot make more than one membership change at a time: 0 added, 2 removed")
	})

	t.Run("invalid: add and remove", func(t *testing.T) {
		updateConfig := proto.Clone(clusterConfig.ConsensusConfig).(*types.ConsensusConfig)
		updateConfig.Members[2] = &types.PeerConfig{
			NodeId:   "node4",
			RaftId:   6,
			PeerHost: "127.0.0.1",
			PeerPort: 7094,
		}
		err := VerifyConsensusReConfig(clusterConfig.ConsensusConfig, updateConfig, lg)
		require.EqualError(t, err, "cannot make more than one membership change at a time: 1 added, 1 removed")
	})

	t.Run("invalid: add a peer with non-unique RaftID", func(t *testing.T) {
		updateConfig := proto.Clone(clusterConfig.ConsensusConfig).(*types.ConsensusConfig)
		updateConfig.Members = append(updateConfig.Members, &types.PeerConfig{
			NodeId:   "node4",
			RaftId:   4,
			PeerHost: "127.0.0.1",
			PeerPort: 7094,
		})
		err := VerifyConsensusReConfig(clusterConfig.ConsensusConfig, updateConfig, lg)
		require.EqualError(t, err, "the RaftId of a new peer must be unique,  > MaxRaftId [5]; but: NodeId=node4, RaftID=4")
	})
}

func testClusterConfig() *types.ClusterConfig {
	clusterConfig := &types.ClusterConfig{
		Admins: []*types.Admin{
			{
				Id:          "admin",
				Certificate: []byte("admin-certificate"),
			},
		},
		CertAuthConfig: &types.CAConfig{
			Roots:         [][]byte{[]byte("root-certificate")},
			Intermediates: [][]byte{[]byte("inter-certificate")},
		},
		ConsensusConfig: &types.ConsensusConfig{
			Algorithm: "raft",
			RaftConfig: &types.RaftConfig{
				TickInterval:         "20ms",
				ElectionTicks:        100,
				HeartbeatTicks:       10,
				MaxInflightBlocks:    50,
				SnapshotIntervalSize: 1000000,
				MaxRaftId:            5,
			},
		},
	}

	for n := uint32(1); n <= uint32(3); n++ {
		nodeID := fmt.Sprintf("node%d", n)
		nodeConfig := &types.NodeConfig{
			Id:          nodeID,
			Address:     "127.0.0.1",
			Port:        6090 + n,
			Certificate: []byte("bogus-cert"),
		}
		peerConfig := &types.PeerConfig{
			NodeId:   nodeID,
			RaftId:   uint64(n),
			PeerHost: "127.0.0.1",
			PeerPort: 7090 + n,
		}
		clusterConfig.Nodes = append(clusterConfig.Nodes, nodeConfig)
		clusterConfig.ConsensusConfig.Members = append(clusterConfig.ConsensusConfig.Members, peerConfig)
	}

	return clusterConfig
}
