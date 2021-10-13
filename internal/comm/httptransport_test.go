// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package comm_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/comm"
	"github.com/hyperledger-labs/orion-server/internal/comm/mocks"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/raftpb"
)

func TestNewHTTPTransport(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(1)

	tr1 := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs[0],
		Logger:    lg,
	})
	require.NotNil(t, tr1)

	err = tr1.SetConsensusListener(&mocks.ConsensusListener{})
	require.NoError(t, err)
	err = tr1.SetConsensusListener(&mocks.ConsensusListener{})
	require.EqualError(t, err, "ConsensusListener already set")

	err = tr1.UpdateClusterConfig(sharedConfig)
	require.NoError(t, err)
	err = tr1.UpdateClusterConfig(sharedConfig)
	require.EqualError(t, err, "dynamic re-config of http transport is not supported yet")
}

func TestHTTPTransport_SendConsensus(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(2)

	cl1 := &mocks.ConsensusListener{}
	tr1 := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs[0],
		Logger:    lg,
	})
	require.NotNil(t, tr1)
	err = tr1.SetConsensusListener(cl1)
	require.NoError(t, err)
	err = tr1.UpdateClusterConfig(sharedConfig)
	require.NoError(t, err)

	cl2 := &mocks.ConsensusListener{}
	tr2 := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs[1],
		Logger:    lg,
	})
	require.NotNil(t, tr1)
	err = tr2.SetConsensusListener(cl2)
	require.NoError(t, err)
	err = tr2.UpdateClusterConfig(sharedConfig)
	require.NoError(t, err)

	err = tr1.Start()
	require.NoError(t, err)
	defer tr1.Close()

	err = tr2.Start()
	require.NoError(t, err)
	defer tr2.Close()

	tr1.SendConsensus([]raftpb.Message{{To: 2}})
	require.Eventually(t,
		func() bool {
			return cl2.ProcessCallCount() == 1
		},
		10*time.Second, 10*time.Millisecond,
	)

	tr2.SendConsensus([]raftpb.Message{{To: 1}})
	tr2.SendConsensus([]raftpb.Message{{To: 1}})
	require.Eventually(t,
		func() bool {
			return cl1.ProcessCallCount() == 2
		},
		10*time.Second, 10*time.Millisecond,
	)
}

func newTestSetup(numServers int) ([]*config.LocalConfiguration, *types.ClusterConfig) {
	configs := make([]*config.LocalConfiguration, 0)
	for i := 0; i < numServers; i++ {
		config := &config.LocalConfiguration{
			Server: config.ServerConf{
				Identity: config.IdentityConf{
					ID: fmt.Sprintf("node%d", i+1),
				},
			},
			Replication: config.ReplicationConf{
				Network: config.NetworkConf{
					Address: "127.0.0.1",
					Port:    uint32(33000 + i),
				},
				TLS: config.TLSConf{Enabled: false},
			},
			Bootstrap: config.BootstrapConf{},
		}

		configs = append(configs, config)
	}

	clusterConf := &types.ClusterConfig{ConsensusConfig: &types.ConsensusConfig{Algorithm: "raft"}}
	for i := 0; i < numServers; i++ {
		peer := &types.PeerConfig{
			NodeId:   fmt.Sprintf("node%d", i+1),
			RaftId:   uint64(i + 1),
			PeerHost: "127.0.0.1",
			PeerPort: uint32(33000 + i),
		}
		clusterConf.ConsensusConfig.Members = append(clusterConf.ConsensusConfig.Members, peer)
	}

	return configs, clusterConf
}
