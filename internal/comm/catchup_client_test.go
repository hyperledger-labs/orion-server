// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package comm_test

import (
	"context"
	"io/ioutil"
	"os"
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
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewCatchUpClient(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	h := comm.NewCatchUpClient(lg, nil)
	require.NotNil(t, h)
}

func TestCatchUpClient_UpdateMembers(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	h := comm.NewCatchUpClient(lg, nil)
	require.NotNil(t, h)

	peer1 := &types.PeerConfig{
		NodeId:   "node1",
		RaftId:   1,
		PeerHost: "127.0.0.1",
		PeerPort: 9001,
	}

	peer2 := &types.PeerConfig{
		NodeId:   "node2",
		RaftId:   2,
		PeerHost: "127.0.0.1",
		PeerPort: 9002,
	}
	err = h.UpdateMembers([]*types.PeerConfig{peer1, peer2})
	require.NoError(t, err)

	peer2.PeerHost = "not a legal address"
	err = h.UpdateMembers([]*types.PeerConfig{peer1, peer2})
	require.Contains(t, err.Error(), "failed to convert PeerConfig")
}

func TestCatchUpClient_GetHeight(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(t, 2)

	tr1, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 5)
	require.NoError(t, err)
	defer tr1.Close()

	cc := comm.NewCatchUpClient(lg, nil)
	require.NotNil(t, cc)
	err = cc.UpdateMembers(sharedConfig.ConsensusConfig.Members)
	require.NoError(t, err)

	h, err := cc.GetHeight(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(5), h)
}

func TestCatchUpClient_GetBlocks(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(t, 2)

	t.Run("first server up, second server down, then up", func(t *testing.T) {
		tr1, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 5)
		require.NoError(t, err)
		defer tr1.Close()

		cc := comm.NewCatchUpClient(lg, nil)
		require.NotNil(t, cc)
		err = cc.UpdateMembers(sharedConfig.ConsensusConfig.Members)
		require.NoError(t, err)

		blocks, err := cc.GetBlocks(context.Background(), 1, 2, 4)
		require.NoError(t, err)
		require.Equal(t, 3, len(blocks))

		blocks, err = cc.GetBlocks(context.Background(), 2, 2, 4)
		require.EqualError(t, err, "Get \"http://127.0.0.1:33001/bcdb-peer/blocks?end=4&start=2\": dial tcp 127.0.0.1:33001: connect: connection refused")

		tr2, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 5)
		require.NoError(t, err)
		defer tr2.Close()

		blocks, err = cc.GetBlocks(context.Background(), 2, 2, 4)
		require.NoError(t, err)
		require.Equal(t, 3, len(blocks))
	})

	t.Run("wrong servers, then update to correct servers", func(t *testing.T) {
		tr1, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 5)
		require.NoError(t, err)
		defer tr1.Close()

		tr2, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 5)
		require.NoError(t, err)
		defer tr2.Close()

		cc := comm.NewCatchUpClient(lg, nil)
		require.NotNil(t, cc)
		wrongConfig := proto.Clone(sharedConfig.ConsensusConfig).(*types.ConsensusConfig)
		for i := 0; i < len(wrongConfig.Members); i++ {
			wrongConfig.Members[i].PeerPort = wrongConfig.Members[i].PeerPort + 100
		}
		err = cc.UpdateMembers(wrongConfig.Members)
		require.NoError(t, err)

		_, err = cc.GetBlocks(context.Background(), 1, 2, 4)
		require.EqualError(t, err, "Get \"http://127.0.0.1:33100/bcdb-peer/blocks?end=4&start=2\": dial tcp 127.0.0.1:33100: connect: connection refused")
		_, err = cc.GetBlocks(context.Background(), 2, 2, 4)
		require.EqualError(t, err, "Get \"http://127.0.0.1:33101/bcdb-peer/blocks?end=4&start=2\": dial tcp 127.0.0.1:33101: connect: connection refused")

		err = cc.UpdateMembers(sharedConfig.ConsensusConfig.Members)
		require.NoError(t, err)

		blocks, err := cc.GetBlocks(context.Background(), 1, 1, 5)
		require.NoError(t, err)
		require.Equal(t, 5, len(blocks))
		blocks, err = cc.GetBlocks(context.Background(), 2, 2, 4)
		require.NoError(t, err)
		require.Equal(t, 3, len(blocks))
	})
}

func TestCatchUpClient_PullBlocks(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(t, 3)

	tr1, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 5)
	require.NoError(t, err)
	defer tr1.Close()

	tr2, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 10)
	require.NoError(t, err)
	defer tr2.Close()

	cc := comm.NewCatchUpClient(lg, nil)
	require.NotNil(t, cc)
	err = cc.UpdateMembers(sharedConfig.ConsensusConfig.Members)
	require.NoError(t, err)

	//get all from the leader hint
	blocks, err := cc.PullBlocks(context.Background(), 1, 3, 1)
	require.NoError(t, err)
	require.Equal(t, 3, len(blocks))
	//get some from the leader hint
	blocks, err = cc.PullBlocks(context.Background(), 1, 8, 1)
	require.NoError(t, err)
	require.Equal(t, 5, len(blocks))
	//get all from member 2, wrong leader hint
	blocks, err = cc.PullBlocks(context.Background(), 6, 9, 1)
	require.NoError(t, err)
	require.Equal(t, 4, len(blocks))
	//get all from one of 1/2, no hint
	blocks, err = cc.PullBlocks(context.Background(), 1, 3, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(blocks))
	//get all from member 2, no hint
	blocks, err = cc.PullBlocks(context.Background(), 6, 9, 0)
	require.NoError(t, err)
	require.Equal(t, 4, len(blocks))
}

func TestCatchUpClient_PullBlocksLoop(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(t, 4)

	tr1, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 50)
	require.NoError(t, err)
	defer tr1.Close()

	tr2, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 100)
	require.NoError(t, err)
	defer tr2.Close()

	tr3, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 2, 150)
	require.NoError(t, err)
	defer tr3.Close()

	cc := comm.NewCatchUpClient(lg, nil)
	require.NotNil(t, cc)
	err = cc.UpdateMembers(sharedConfig.ConsensusConfig.Members)
	require.NoError(t, err)

	type testCase struct {
		name string
		hint uint64
	}
	testCases := []testCase{
		{name: "wrong hint", hint: 1},
		{name: "no hint", hint: 0},
		{name: "good hint", hint: 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ledger4 := &memLedger{}

			var num uint64
			var target uint64 = 150
			for num < target {
				blocks, err := cc.PullBlocks(context.Background(), num+1, target, tc.hint)
				require.NoError(t, err)
				for _, block := range blocks {
					err = ledger4.Append(block)
					require.NoError(t, err)
					num = block.Header.BaseHeader.Number
				}
			}

			h, err := ledger4.Height()
			require.NoError(t, err)
			require.Equal(t, target, h)
		})
	}
}

// Scenario:
// - Define a 3 node cluster.
// - Start pulling blocks until block 150.
// - Start 1st transport with ledger at height 50,
// - Wait until client gets them all and starts retrying, until it gets to retry max
// - Start 2nd transport with ledger at height 100,
// - Wait until client gets them all and starts retrying, until it gets to retry max
// - Start 3rd transport with ledger at height 150,
// - Wait until client gets them all.
func TestCatchUpClient_PullBlocksRetry(t *testing.T) {
	dir, err := ioutil.TempDir("", "catchup")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	var retryCount int
	var wgRetry1 sync.WaitGroup
	wgRetry1.Add(1)
	var wgRetry2 sync.WaitGroup
	wgRetry2.Add(1)

	lg, err := logger.New(
		&logger.Config{
			Level:         "debug", //must be debug
			OutputPath:    []string{"stdout", path.Join(dir, "test.log")},
			ErrOutputPath: []string{"stderr"},
			Encoding:      "console",
		},
		zap.Hooks(func(entry zapcore.Entry) error {
			if strings.Contains(entry.Message, "Retry interval max reached") {
				t.Logf("Retry is working! %s | %s", entry.Caller, entry.Message)
				if retryCount == 0 {
					wgRetry1.Done()
				}
				if retryCount == 1 {
					wgRetry2.Done()
				}
				retryCount++
			}
			return nil
		}),
	)
	require.NoError(t, err)

	mn := comm.RetryIntervalMin
	mx := comm.RetryIntervalMax
	comm.RetryIntervalMin = 100 * time.Microsecond
	comm.RetryIntervalMax = 1 * time.Millisecond
	defer func() {
		comm.RetryIntervalMin = mn
		comm.RetryIntervalMax = mx
	}()

	localConfigs, sharedConfig := newTestSetup(t, 3)

	tr1, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 50)
	require.NoError(t, err)
	defer tr1.Close()

	cc := comm.NewCatchUpClient(lg, nil)
	require.NotNil(t, cc)
	err = cc.UpdateMembers(sharedConfig.ConsensusConfig.Members)
	require.NoError(t, err)

	ledger4 := &memLedger{}

	var num uint64
	var target uint64 = 150
	var wgTarget sync.WaitGroup
	wgTarget.Add(1)

	pullBlocksLoop := func() {
		for num < target {
			blocks, err := cc.PullBlocks(context.Background(), num+1, target, 0)
			require.NoError(t, err)
			for _, block := range blocks {
				err = ledger4.Append(block)
				require.NoError(t, err)
				num = block.Header.BaseHeader.Number
			}
		}
		wgTarget.Done()
	}

	go pullBlocksLoop()

	wgRetry1.Wait()
	tr2, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 100)
	require.NoError(t, err)
	defer tr2.Close()

	wgRetry2.Wait()
	tr3, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 2, 150)
	require.NoError(t, err)
	defer tr3.Close()

	wgTarget.Wait()

	h, err := ledger4.Height()
	require.NoError(t, err)
	require.Equal(t, target, h)
}

func TestCatchUpClient_PullBlocksCancel(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	mn := comm.RetryIntervalMin
	mx := comm.RetryIntervalMax
	comm.RetryIntervalMin = 100 * time.Microsecond
	comm.RetryIntervalMax = time.Millisecond
	defer func() {
		comm.RetryIntervalMin = mn
		comm.RetryIntervalMax = mx
	}()

	localConfigs, sharedConfig := newTestSetup(t, 3)

	tr1, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 50)
	require.NoError(t, err)
	defer tr1.Close()

	cc := comm.NewCatchUpClient(lg, nil)
	require.NotNil(t, cc)
	err = cc.UpdateMembers(sharedConfig.ConsensusConfig.Members)
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		blocks, err := cc.PullBlocks(ctx, 51, 100, 0)
		wg.Done()
		require.EqualError(t, err, "PullBlocks canceled: context canceled")
		require.Nil(t, blocks)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()
	wg.Wait()
}

func TestCatchUpClient_PullBlocksWithTLS(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(t, 3)
	for _, conf := range localConfigs {
		conf.Replication.TLS.Enabled = true
	}

	tr1, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 5)
	require.NoError(t, err)
	defer tr1.Close()

	tr2, _, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 10)
	require.NoError(t, err)
	defer tr2.Close()

	tr3, err := comm.NewHTTPTransport(&comm.Config{
		LocalConf:    localConfigs[2],
		Logger:       lg,
		LedgerReader: nil,
	})
	require.NoError(t, err)

	cc := comm.NewCatchUpClient(lg, tr3.ClientTLSConfig())
	require.NotNil(t, cc)
	err = cc.UpdateMembers(sharedConfig.ConsensusConfig.Members)
	require.NoError(t, err)

	//get all from the leader hint
	blocks, err := cc.PullBlocks(context.Background(), 1, 3, 1)
	require.NoError(t, err)
	require.Equal(t, 3, len(blocks))
	//get some from the leader hint
	blocks, err = cc.PullBlocks(context.Background(), 1, 8, 1)
	require.NoError(t, err)
	require.Equal(t, 5, len(blocks))
	//get all from member 2, wrong leader hint
	blocks, err = cc.PullBlocks(context.Background(), 6, 9, 1)
	require.NoError(t, err)
	require.Equal(t, 4, len(blocks))
	//get all from one of 1/2, no hint
	blocks, err = cc.PullBlocks(context.Background(), 1, 3, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(blocks))
	//get all from member 2, no hint
	blocks, err = cc.PullBlocks(context.Background(), 6, 9, 0)
	require.NoError(t, err)
	require.Equal(t, 4, len(blocks))
}

func startTransportWithLedger(t *testing.T, lg *logger.SugarLogger, localConfigs []*config.LocalConfiguration, sharedConfig *types.ClusterConfig, index, height uint64) (*comm.HTTPTransport, *mocks.ConsensusListener, error) {
	ledger := &memLedger{}
	for n := uint64(1); n <= height; n++ {
		ledger.Append(&types.Block{Header: &types.BlockHeader{BaseHeader: &types.BlockHeaderBase{Number: n}}})
	}
	cl := &mocks.ConsensusListener{}
	tr, err := comm.NewHTTPTransport(&comm.Config{
		LocalConf:    localConfigs[index],
		Logger:       lg,
		LedgerReader: ledger,
	})
	require.NoError(t, err)
	require.NotNil(t, tr)
	err = tr.SetConsensusListener(cl)
	require.NoError(t, err)
	err = tr.SetClusterConfig(sharedConfig)
	require.NoError(t, err)

	err = tr.Start()
	require.NoError(t, err)
	return tr, cl, err
}
