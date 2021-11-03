// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package comm_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/comm"
	"github.com/hyperledger-labs/orion-server/internal/comm/mocks"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewHTTPTransport(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(t, 1)

	tr1, err := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs[0],
		Logger:    lg,
	})
	require.NoError(t, err)
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
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(t, 2)

	cl1 := &mocks.ConsensusListener{}
	tr1, _ := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs[0],
		Logger:    lg,
	})
	require.NotNil(t, tr1)
	err = tr1.SetConsensusListener(cl1)
	require.NoError(t, err)
	err = tr1.UpdateClusterConfig(sharedConfig)
	require.NoError(t, err)

	cl2 := &mocks.ConsensusListener{}
	tr2, _ := comm.NewHTTPTransport(&comm.Config{
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

// Scenario: both sides enable TLS.
// Messages arrive.
func TestHTTPTransport_SendConsensus_TLS(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(t, 2)
	for _, c := range localConfigs {
		c.Replication.TLS.Enabled = true
	}

	cl1 := &mocks.ConsensusListener{}
	tr1, err := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs[0],
		Logger:    lg,
	})
	require.NoError(t, err)
	require.NotNil(t, tr1)
	err = tr1.SetConsensusListener(cl1)
	require.NoError(t, err)
	err = tr1.UpdateClusterConfig(sharedConfig)
	require.NoError(t, err)

	cl2 := &mocks.ConsensusListener{}
	tr2, err := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs[1],
		Logger:    lg,
	})
	require.NoError(t, err)
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

// Scenario: one side enables TLS, the other not.
// Messages do not arrive, nodes are reported unreachable.
// log messages indicate that
// - the TLS enabled side expects TLS handshakes and does not get it
// - the unsecure side tries to use schema 'http' and gets a bad request
func TestHTTPTransport_SendConsensus_HalfTLS(t *testing.T) {
	// the raft log messages use fields, so we can't capture them with "zap.Hook", which does not carry the "Context".
	loggerCore, observedLogs := observer.New(zapcore.DebugLevel)
	observedLogger := zap.New(loggerCore).Sugar()
	lg := &logger.SugarLogger{SugaredLogger: observedLogger}

	localConfigs, sharedConfig := newTestSetup(t, 2)
	localConfigs[0].Replication.TLS.Enabled = true

	cl1 := &mocks.ConsensusListener{}
	tr1, err := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs[0],
		Logger:    lg,
	})
	require.NoError(t, err)
	require.NotNil(t, tr1)
	err = tr1.SetConsensusListener(cl1)
	require.NoError(t, err)
	err = tr1.UpdateClusterConfig(sharedConfig)
	require.NoError(t, err)

	cl2 := &mocks.ConsensusListener{}
	tr2, err := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs[1],
		Logger:    lg,
	})
	require.NoError(t, err)
	require.NotNil(t, tr1)
	err = tr2.SetConsensusListener(cl2)
	require.NoError(t, err)
	err = tr2.UpdateClusterConfig(sharedConfig)
	require.NoError(t, err)

	err = tr1.Start()
	require.NoError(t, err)

	err = tr2.Start()
	require.NoError(t, err)

	tr1.SendConsensus([]raftpb.Message{{To: 2}})
	tr1.SendConsensus([]raftpb.Message{{To: 2}})

	tr2.SendConsensus([]raftpb.Message{{To: 1}})
	tr2.SendConsensus([]raftpb.Message{{To: 1}})
	tr2.SendConsensus([]raftpb.Message{{To: 1}})

	require.Eventually(t, func() bool { return cl1.ReportUnreachableCallCount() == 2 }, 5*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return cl2.ReportUnreachableCallCount() == 3 }, 5*time.Second, 100*time.Millisecond)

	require.Equal(t, 0, cl1.ProcessCallCount())
	require.Equal(t, 0, cl2.ProcessCallCount())

	require.Equal(t, 0, cl1.IsIDRemovedCallCount())
	require.Equal(t, 0, cl2.IsIDRemovedCallCount())

	tr1.Close()
	tr2.Close()

	t.Log("Analyzing logs:")
	require.True(t, observedLogs.Len() > 0)
	failedHandshake := 0
	failedHttp := 0
	for _, entry := range observedLogs.All() {
		if strings.Contains(entry.Message, "peer deactivated") {
			ctxMap := entry.ContextMap()
			id, ok := ctxMap["peer-id"]
			require.True(t, ok)
			errS, ok := ctxMap["error"]
			require.True(t, ok)

			fmt.Printf("%s | %s | %v \n", entry.Time, entry.Message, entry.ContextMap())
			switch id {
			case "1":
				if strings.Contains(errS.(string), "failed to write 1 on pipeline (unexpected http status Bad Request while posting to \"http://127.0.0.1:33000/raft\")") {
					failedHttp++
				}
			case "2":
				if strings.Contains(errS.(string), "(tls: first record does not look like a TLS handshake)") {
					failedHandshake++
				}
			default:
				t.Failed()
			}
		}
	}
	require.True(t, failedHandshake > 0)
	require.True(t, failedHttp > 0)
}

// Scenario: both sides enable TLS, but trust different CAs.
// Messages do not arrive, nodes are reported unreachable.
// log messages indicate that
// - the TLS handshake fails because the certificate cannot be verified against the CA.
func TestHTTPTransport_SendConsensus_TLS_CAMismatch(t *testing.T) {
	// the raft log messages use fields, so we can't capture them with "zap.Hook", which does not carry the "Context".
	loggerCore, observedLogs := observer.New(zapcore.DebugLevel)
	observedLogger := zap.New(loggerCore).Sugar()
	lg := &logger.SugarLogger{SugaredLogger: observedLogger}

	//each setup has a different root CA
	localConfigs1, sharedConfig1 := newTestSetup(t, 2)
	for _, c := range localConfigs1 {
		c.Replication.TLS.Enabled = true
	}
	localConfigs2, sharedConfig2 := newTestSetup(t, 2)
	for _, c := range localConfigs2 {
		c.Replication.TLS.Enabled = true
	}

	cl1 := &mocks.ConsensusListener{}
	tr1, err := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs1[0],
		Logger:    lg,
	})
	require.NoError(t, err)
	require.NotNil(t, tr1)
	err = tr1.SetConsensusListener(cl1)
	require.NoError(t, err)
	err = tr1.UpdateClusterConfig(sharedConfig1)
	require.NoError(t, err)

	cl2 := &mocks.ConsensusListener{}
	tr2, err := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs2[1],
		Logger:    lg,
	})
	require.NoError(t, err)
	require.NotNil(t, tr1)
	err = tr2.SetConsensusListener(cl2)
	require.NoError(t, err)
	err = tr2.UpdateClusterConfig(sharedConfig2)
	require.NoError(t, err)

	err = tr1.Start()
	require.NoError(t, err)

	err = tr2.Start()
	require.NoError(t, err)

	tr1.SendConsensus([]raftpb.Message{{To: 2}})
	tr1.SendConsensus([]raftpb.Message{{To: 2}})

	tr2.SendConsensus([]raftpb.Message{{To: 1}})
	tr2.SendConsensus([]raftpb.Message{{To: 1}})
	tr2.SendConsensus([]raftpb.Message{{To: 1}})

	require.Eventually(t, func() bool { return cl1.ReportUnreachableCallCount() == 2 }, 5*time.Second, 100*time.Millisecond)
	require.Eventually(t, func() bool { return cl2.ReportUnreachableCallCount() == 3 }, 5*time.Second, 100*time.Millisecond)

	require.Equal(t, 0, cl1.ProcessCallCount())
	require.Equal(t, 0, cl2.ProcessCallCount())

	require.Equal(t, 0, cl1.IsIDRemovedCallCount())
	require.Equal(t, 0, cl2.IsIDRemovedCallCount())

	tr1.Close()
	tr2.Close()

	t.Log("Analyzing logs:")
	require.True(t, observedLogs.Len() > 0)
	failedHandshake := 0
	for _, entry := range observedLogs.All() {
		fmt.Printf("%s | %s | %v \n", entry.Time, entry.Message, entry.ContextMap())

		if strings.Contains(entry.Message, "peer deactivated") {
			ctxMap := entry.ContextMap()
			id, ok := ctxMap["peer-id"]
			require.True(t, ok)
			errS, ok := ctxMap["error"]
			require.True(t, ok)

			fmt.Printf("%s | %s | %v \n", entry.Time, entry.Message, entry.ContextMap())
			switch id {
			case "1":
				fallthrough
			case "2":
				if strings.Contains(errS.(string), "x509: certificate signed by unknown authority") {
					failedHandshake++
				}
			default:
				t.Failed()
			}
		}
	}
	require.True(t, failedHandshake > 2)
}

// Scenario: missing certificate.
func TestNewHTTPTransport_TLS_FilePathFailure(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, _ := newTestSetup(t, 1)
	localConfigs[0].Replication.TLS.Enabled = true
	localConfigs[0].Replication.TLS.ServerCertificatePath = "/bogus-path"
	_, err = comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs[0],
		Logger:    lg,
	})
	require.EqualError(t, err, "failed to read local config Replication.TLS.ServerCertificatePath: open /bogus-path: no such file or directory")
}

func newTestSetup(t *testing.T, numServers int) ([]*config.LocalConfiguration, *types.ClusterConfig) {
	var nodeIDs []string
	for i := 0; i < numServers; i++ {
		nodeIDs = append(nodeIDs, fmt.Sprintf("node%d", i+1))
	}
	cryptoDir := testutils.GenerateTestClientCrypto(t, nodeIDs, true)
	auxDir, err := ioutil.TempDir("/tmp", "UnitTestAux")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(auxDir)
	})

	configs := make([]*config.LocalConfiguration, 0)
	for i := 0; i < numServers; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		config := &config.LocalConfiguration{
			Server: config.ServerConf{
				Identity: config.IdentityConf{
					ID: nodeID,
				},
			},
			Replication: config.ReplicationConf{
				AuxDir: path.Join(auxDir, nodeID),
				Network: config.NetworkConf{
					Address: "127.0.0.1",
					Port:    uint32(33000 + i),
				},
				TLS: config.TLSConf{
					Enabled:               false,
					ClientAuthRequired:    false,
					ServerCertificatePath: path.Join(cryptoDir, nodeID+".pem"),
					ServerKeyPath:         path.Join(cryptoDir, nodeID+".key"),
					ClientCertificatePath: path.Join(cryptoDir, nodeID+".pem"),
					ClientKeyPath:         path.Join(cryptoDir, nodeID+".key"),
					CaConfig: config.CAConfiguration{
						RootCACertsPath:         []string{path.Join(cryptoDir, testutils.RootCAFileName+".pem")},
						IntermediateCACertsPath: []string{path.Join(cryptoDir, testutils.IntermediateCAFileName+".pem")},
					},
				},
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
