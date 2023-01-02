// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package comm_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/comm"
	"github.com/hyperledger-labs/orion-server/internal/comm/mocks"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
	"github.com/xiang90/probing"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// Scenario: normal construction.
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

	err = tr1.SetClusterConfig(sharedConfig)
	require.NoError(t, err)
	err = tr1.SetClusterConfig(sharedConfig)
	require.EqualError(t, err, "cluster config already exists")
}

// Scenario: send consensus messages from one peer to the next.
// TLS not enabled.
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
	err = tr1.SetClusterConfig(sharedConfig)
	require.NoError(t, err)

	cl2 := &mocks.ConsensusListener{}
	tr2, _ := comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfigs[1],
		Logger:    lg,
	})
	require.NotNil(t, tr1)
	err = tr2.SetConsensusListener(cl2)
	require.NoError(t, err)
	err = tr2.SetClusterConfig(sharedConfig)
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

// Scenario: send consensus messages from one peer to the next.
// Both sides enable TLS.
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
	err = tr1.SetClusterConfig(sharedConfig)
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
	err = tr2.SetClusterConfig(sharedConfig)
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

// Scenario: send consensus messages from one peer to the next.
// One side enables TLS, the other not.
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
	err = tr1.SetClusterConfig(sharedConfig)
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
	err = tr2.SetClusterConfig(sharedConfig)
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

// Scenario: send consensus messages from one peer to the next.
// Both sides enable TLS, but trust different CAs.
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
	err = tr1.SetClusterConfig(sharedConfig1)
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
	err = tr2.SetClusterConfig(sharedConfig2)
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

// Scenario: update the endpoints of a peer.
// - generate a test configuration for 3 servers.
// - server 3 starts on a new port, not reflected yet in shared config.
// - servers 1,2 will not be able to reach it, but server 3 will be able to reach them.
// - servers are updated with the shared config, now messages get through.
func TestHTTPTransport_UpdatePeers(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(3)

	lg, err := logger.New(
		&logger.Config{
			Level:         "info",
			OutputPath:    []string{"stdout"},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
		},
		zap.Hooks(func(entry zapcore.Entry) error {
			//http transport starting to serve peers on: 127.0.0.1:33000 | 33001 | 33003
			if strings.Contains(entry.Message, "http transport starting to serve peers on: 127.0.0.1:3300") {
				t.Logf("Server started: %s | %s", entry.Caller, entry.Message)
				wg.Done()
			}
			return nil
		}),
	)
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(t, 3)

	tr1, cl1, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 0)
	require.NoError(t, err)
	defer tr1.Close()

	tr2, cl2, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 0)
	require.NoError(t, err)
	defer tr2.Close()

	// server 3 starts on a new port, not reflected yet in shared config.
	// servers 1,2 will not be able to reach it. However, server 3 can reach servers 1,2.
	localConfigs[2].Replication.Network.Port++
	tr3, cl3, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 2, 5)
	require.NoError(t, err)
	defer tr3.Close()

	wg.Wait()

	// ActivePeers reports are asymmetric
	// tr1, tr2 only report each other
	var activePeers map[string]*types.PeerConfig
	for _, tr := range []*comm.HTTPTransport{tr1, tr2} {
		require.Eventually(t, func() bool {
			activePeers = tr.ActivePeers(10*time.Millisecond, true)
			return len(activePeers) == 2
		}, 10*time.Second, 100*time.Millisecond)
		for _, peerId := range []string{"node1", "node2"} {
			require.Equal(t, peerId, activePeers[peerId].NodeId)
		}
	}
	// tr3 reports everyone
	require.Eventually(t, func() bool {
		activePeers = tr3.ActivePeers(10*time.Millisecond, true)
		return len(activePeers) == 3
	}, 10*time.Second, 100*time.Millisecond)
	for _, peerId := range []string{"node1", "node2", "node3"} {
		require.Equal(t, peerId, activePeers[peerId].NodeId)
	}

	// PullBlocks to server 3 will not succeed
	timeout1, _ := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = tr1.PullBlocks(timeout1, 1, 5, 0)
	require.EqualError(t, err, "PullBlocks canceled: context deadline exceeded")
	timeout2, _ := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = tr2.PullBlocks(timeout2, 1, 5, 0)
	require.EqualError(t, err, "PullBlocks canceled: context deadline exceeded")

	// However, server 3 can reach servers 1,2
	err = tr3.SendConsensus([]raftpb.Message{{To: 1}})
	require.NoError(t, err)
	err = tr3.SendConsensus([]raftpb.Message{{To: 2}})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return cl1.ProcessCallCount() == 1 }, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return cl2.ProcessCallCount() == 1 }, 10*time.Second, 10*time.Millisecond)

	t.Log("=== re-config endpoint ===")

	// emulate a config tx update to change the endpoint of server 3
	updatedConfig := proto.Clone(sharedConfig).(*types.ClusterConfig)
	updatedConfig.ConsensusConfig.Members[2].PeerPort = localConfigs[2].Replication.Network.Port

	err = tr1.UpdatePeers(nil, nil, updatedConfig.ConsensusConfig.Members[2:], updatedConfig)
	require.NoError(t, err)
	err = tr2.UpdatePeers(nil, nil, updatedConfig.ConsensusConfig.Members[2:], updatedConfig)
	require.NoError(t, err)
	err = tr3.UpdatePeers(nil, nil, updatedConfig.ConsensusConfig.Members[2:], updatedConfig)
	require.NoError(t, err)

	// messages are received
	err = tr1.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr1.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	require.Eventually(t,
		func() bool {
			return cl3.ProcessCallCount() == 2
		},
		10*time.Second, 10*time.Millisecond,
	)

	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	require.Eventually(t,
		func() bool {
			return cl3.ProcessCallCount() == 5
		},
		10*time.Second, 10*time.Millisecond,
	)

	// PullBlocks will succeed
	timeout1, _ = context.WithTimeout(context.Background(), 10*time.Second)
	blocks, err := tr1.PullBlocks(timeout1, 1, 5, 0)
	require.NoError(t, err)
	require.Len(t, blocks, 5)
	timeout2, _ = context.WithTimeout(context.Background(), 10*time.Second)
	blocks, err = tr2.PullBlocks(timeout2, 1, 5, 0)
	require.NoError(t, err)
	require.Len(t, blocks, 5)

	// ActivePeers reports are now symmetric
	for _, tr := range []*comm.HTTPTransport{tr1, tr2, tr3} {
		require.Eventually(t, func() bool {
			activePeers = tr.ActivePeers(10*time.Millisecond, true)
			return len(activePeers) == 3
		}, 10*time.Second, 100*time.Millisecond)
		for _, peerId := range []string{"node1", "node2", "node3"} {
			require.Equal(t, peerId, activePeers[peerId].NodeId)
		}
	}
}

// Scenario: add peers.
// - generate a test configuration for 2 servers and start them.
// - start a 3rd server; check connectivity to it;
// - emulate a config tx update to add it; check connectivity to it.
func TestHTTPTransport_AddPeers(t *testing.T) {
	loggerCore, observedLogs := observer.New(zapcore.DebugLevel)
	observedLogger := zap.New(loggerCore).Sugar()
	lg := &logger.SugarLogger{SugaredLogger: observedLogger}

	localConfigs, sharedConfig := newTestSetup(t, 2)

	tr1, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 0)
	require.NoError(t, err)
	defer tr1.Close()

	tr2, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 0)
	require.NoError(t, err)
	defer tr2.Close()

	localConfigsUpdated, sharedConfigUpdated := newTestSetup(t, 3)
	// server 3 is not reflected yet in shared config of servers 1,2; they will not be able to reach it.
	tr3, cl3, err := startTransportWithLedger(t, lg, localConfigsUpdated, sharedConfigUpdated, 2, 5)
	require.NoError(t, err)
	defer tr3.Close()

	// messages to unknown peers are ignored, but messages show up in the logs
	err = tr1.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	ignoredLogEntryCount := 0
	countIgnoredLogEntries := func(num int) bool {
		for _, entry := range observedLogs.TakeAll() {
			if strings.Contains(entry.Message, "ignored message send request; unknown remote peer target") {
				ctxMap := entry.ContextMap()
				id, ok := ctxMap["unknown-target-peer-id"]
				require.True(t, ok)

				fmt.Printf("%s | %s | %v \n", entry.Time, entry.Message, entry.ContextMap())
				switch id {
				case "3":
					ignoredLogEntryCount++
				default:
					t.Failed()
				}
			}
		}
		return ignoredLogEntryCount == num
	}
	require.Eventually(t, func() bool { return countIgnoredLogEntries(3) }, 10*time.Second, 10*time.Millisecond)

	// PullBlocks will not succeed
	timeout1, _ := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = tr1.PullBlocks(timeout1, 1, 5, 0)
	require.EqualError(t, err, "PullBlocks canceled: context deadline exceeded")
	timeout2, _ := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = tr2.PullBlocks(timeout2, 1, 5, 0)
	require.EqualError(t, err, "PullBlocks canceled: context deadline exceeded")

	// emulate a config tx update to change the endpoint of server 3
	err = tr1.UpdatePeers(sharedConfigUpdated.ConsensusConfig.Members[2:], nil, nil, sharedConfigUpdated)
	require.NoError(t, err)
	err = tr2.UpdatePeers(sharedConfigUpdated.ConsensusConfig.Members[2:], nil, nil, sharedConfigUpdated)
	require.NoError(t, err)

	// messages are received
	err = tr1.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr1.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	require.Eventually(t,
		func() bool {
			return cl3.ProcessCallCount() == 2
		},
		10*time.Second, 10*time.Millisecond,
	)
	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	require.Eventually(t,
		func() bool {
			return cl3.ProcessCallCount() == 5
		},
		10*time.Second, 10*time.Millisecond,
	)

	// PullBlocks will succeed
	timeout1, _ = context.WithTimeout(context.Background(), 10*time.Second)
	blocks, err := tr1.PullBlocks(timeout1, 1, 5, 0)
	require.NoError(t, err)
	require.Len(t, blocks, 5)
	timeout2, _ = context.WithTimeout(context.Background(), 10*time.Second)
	blocks, err = tr2.PullBlocks(timeout2, 1, 5, 0)
	require.NoError(t, err)
	require.Len(t, blocks, 5)
}

// Scenario: remove peers.
// - generate a test configuration for 3 servers and start them.
// - check connectivity to the 3rd server.
// - emulate a config tx to remove the 3rd server; check connectivity to it.
func TestHTTPTransport_RemovePeers(t *testing.T) {
	loggerCore, observedLogs := observer.New(zapcore.DebugLevel)
	observedLogger := zap.New(loggerCore).Sugar()
	lg := &logger.SugarLogger{SugaredLogger: observedLogger}

	localConfigs, sharedConfig := newTestSetup(t, 3)

	tr1, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 0)
	require.NoError(t, err)
	defer tr1.Close()

	tr2, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 0)
	require.NoError(t, err)
	defer tr2.Close()

	// server 3 is reflected in shared config of servers 1,2.
	tr3, cl3, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 2, 5)
	require.NoError(t, err)
	defer tr3.Close()

	// messages are received
	err = tr1.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr1.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	require.Eventually(t,
		func() bool {
			return cl3.ProcessCallCount() == 2
		},
		10*time.Second, 10*time.Millisecond,
	)
	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	require.Eventually(t,
		func() bool {
			return cl3.ProcessCallCount() == 5
		},
		10*time.Second, 10*time.Millisecond,
	)

	// PullBlocks will succeed
	timeout1, _ := context.WithTimeout(context.Background(), 10*time.Second)
	blocks, err := tr1.PullBlocks(timeout1, 1, 5, 0)
	require.NoError(t, err)
	require.Len(t, blocks, 5)
	timeout2, _ := context.WithTimeout(context.Background(), 10*time.Second)
	blocks, err = tr2.PullBlocks(timeout2, 1, 5, 0)
	require.NoError(t, err)
	require.Len(t, blocks, 5)

	// emulate a config tx to remove 3rd server
	sharedConfigUpdated := proto.Clone(sharedConfig).(*types.ClusterConfig)
	sharedConfigUpdated.ConsensusConfig.Members = sharedConfigUpdated.ConsensusConfig.Members[0:2]
	err = tr1.UpdatePeers(nil, sharedConfig.ConsensusConfig.Members[2:], nil, sharedConfigUpdated)
	require.NoError(t, err)
	err = tr2.UpdatePeers(nil, sharedConfig.ConsensusConfig.Members[2:], nil, sharedConfigUpdated)
	require.NoError(t, err)

	// messages to unknown peers are ignored, but show up in the logs
	err = tr1.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	err = tr2.SendConsensus([]raftpb.Message{{To: 3}})
	require.NoError(t, err)
	ignoredLogEntryCount := 0
	countIgnoredLogEntries := func(num int) bool {
		for _, entry := range observedLogs.TakeAll() {
			if strings.Contains(entry.Message, "ignored message send request; unknown remote peer target") {
				ctxMap := entry.ContextMap()
				id, ok := ctxMap["unknown-target-peer-id"]
				require.True(t, ok)

				fmt.Printf("%s | %s | %v \n", entry.Time, entry.Message, entry.ContextMap())
				switch id {
				case "3":
					ignoredLogEntryCount++
				default:
					t.Failed()
				}
			}
		}
		return ignoredLogEntryCount == num
	}
	require.Eventually(t, func() bool { return countIgnoredLogEntries(3) }, 10*time.Second, 10*time.Millisecond)

	// PullBlocks will not succeed
	timeout1, _ = context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = tr1.PullBlocks(timeout1, 1, 5, 0)
	require.EqualError(t, err, "PullBlocks canceled: context deadline exceeded")
	timeout2, _ = context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, err = tr2.PullBlocks(timeout2, 1, 5, 0)
	require.EqualError(t, err, "PullBlocks canceled: context deadline exceeded")

	require.Equal(t, 5, cl3.ProcessCallCount())
}

// Scenario: test the probing endpoint is responding
func TestHTTPTransport_Probing(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(t, 2)

	tr1, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 0)
	require.NoError(t, err)
	defer tr1.Close()

	time.Sleep(time.Second)

	resp, err := http.Get("http://127.0.0.1:33000/raft/probing")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	health := &probing.Health{}
	err = json.Unmarshal(body, health)
	require.NoError(t, err)
	require.True(t, health.OK)
	t.Logf("Health: %+v", health)
	resp.Body.Close()

	resp, err = http.Get("http://127.0.0.1:33001/raft/probing")
	require.EqualError(t, err, "Get \"http://127.0.0.1:33001/raft/probing\": dial tcp 127.0.0.1:33001: connect: connection refused")
	require.Nil(t, resp)
}

// Scenario: check ActivePeers report in a well configured cluster
// - increase number of nodes from 1 to 5
// - decrease number of nodes to 3
func TestHTTPTransport_ActivePeers(t *testing.T) {
	t.Skip("Flaky test - port 33004")
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	numActiveCond := func(tr *comm.HTTPTransport, num int, self bool) bool {
		peers := tr.ActivePeers(10*time.Millisecond, self)
		return len(peers) == num
	}

	localConfigs, sharedConfig := newTestSetup(t, 5)

	tr1, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 0)
	require.NoError(t, err)
	defer tr1.Close()

	require.Eventually(t, func() bool { return numActiveCond(tr1, 1, true) }, 30*time.Second, 1000*time.Millisecond)
	require.Eventually(t, func() bool { return numActiveCond(tr1, 0, false) }, 30*time.Second, 1000*time.Millisecond)

	tr2, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 0)
	require.NoError(t, err)
	defer tr2.Close()

	require.Eventually(t, func() bool { return numActiveCond(tr2, 2, true) }, 10*time.Second, 1000*time.Millisecond)
	require.Eventually(t, func() bool { return numActiveCond(tr2, 1, false) }, 10*time.Second, 1000*time.Millisecond)

	tr3, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 2, 0)
	require.NoError(t, err)
	defer tr3.Close()

	require.Eventually(t, func() bool { return numActiveCond(tr3, 3, true) }, 10*time.Second, 1000*time.Millisecond)
	require.Eventually(t, func() bool { return numActiveCond(tr3, 2, false) }, 10*time.Second, 1000*time.Millisecond)

	for _, tr := range []*comm.HTTPTransport{tr1, tr2, tr3} {
		activePeers := tr.ActivePeers(10*time.Millisecond, true)
		for _, peerId := range []string{"node1", "node2", "node3"} {
			require.Equal(t, peerId, activePeers[peerId].NodeId)
		}
	}

	tr4, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 3, 0)
	require.NoError(t, err)

	tr5, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 4, 0)
	require.NoError(t, err)

	require.Eventually(t, func() bool { return numActiveCond(tr4, 4, false) }, 10*time.Second, 1000*time.Millisecond)
	require.Eventually(t, func() bool { return numActiveCond(tr5, 4, false) }, 10*time.Second, 1000*time.Millisecond)

	for _, tr := range []*comm.HTTPTransport{tr1, tr2, tr3, tr4, tr5} {
		activePeers := tr.ActivePeers(10*time.Millisecond, true)
		for _, peerId := range []string{"node1", "node2", "node3", "node4", "node5"} {
			require.Equal(t, peerId, activePeers[peerId].NodeId)
		}
	}

	tr4.Close()
	tr5.Close()

	require.Eventually(t, func() bool { return numActiveCond(tr1, 2, false) }, 10*time.Second, 1000*time.Millisecond)
	require.Eventually(t, func() bool { return numActiveCond(tr2, 2, false) }, 10*time.Second, 1000*time.Millisecond)
	require.Eventually(t, func() bool { return numActiveCond(tr3, 2, false) }, 10*time.Second, 1000*time.Millisecond)

	for _, tr := range []*comm.HTTPTransport{tr1, tr2, tr3} {
		activePeers := tr.ActivePeers(10*time.Millisecond, true)
		for _, peerId := range []string{"node1", "node2", "node3"} {
			require.Equal(t, peerId, activePeers[peerId].NodeId)
		}
	}
}

func newTestSetup(t *testing.T, numServers int) ([]*config.LocalConfiguration, *types.ClusterConfig) {
	var nodeIDs []string
	for i := 0; i < numServers; i++ {
		nodeIDs = append(nodeIDs, fmt.Sprintf("node%d", i+1))
	}
	cryptoDir := testutils.GenerateTestCrypto(t, nodeIDs, true)
	auxDir := t.TempDir()

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
					ClientCertificatePath: path.Join(cryptoDir, "client_"+nodeID+".pem"),
					ClientKeyPath:         path.Join(cryptoDir, "client_"+nodeID+".key"),
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
