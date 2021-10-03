// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package comm_test

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/config"
	"github.com/IBM-Blockchain/bcdb-server/internal/comm"
	"github.com/IBM-Blockchain/bcdb-server/internal/comm/mocks"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestNewCatchUpClient(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	h := comm.NewCatchUpClient(lg)
	require.NotNil(t, h)
}

func TestCatchUpClient_UpdateMembers(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	h := comm.NewCatchUpClient(lg)
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
	require.EqualError(t, err, "failed to convert PeerConfig [node_id:\"node2\" raft_id:2 peer_host:\"not a legal address\" peer_port:9002 ] to url: parse \"http://not a legal address:9002\": invalid character \" \" in host name")
}

func TestCatchUpClient_GetHeight(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(2)

	tr1, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 5)
	require.NoError(t, err)
	defer tr1.Close()

	cc := comm.NewCatchUpClient(lg)
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

	localConfigs, sharedConfig := newTestSetup(2)

	tr1, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 5)
	require.NoError(t, err)
	defer tr1.Close()

	cc := comm.NewCatchUpClient(lg)
	require.NotNil(t, cc)
	err = cc.UpdateMembers(sharedConfig.ConsensusConfig.Members)
	require.NoError(t, err)

	blocks, err := cc.GetBlocks(context.Background(), 1, 2, 4)
	require.NoError(t, err)
	require.Equal(t, 3, len(blocks))

	blocks, err = cc.GetBlocks(context.Background(), 2, 2, 4)
	require.EqualError(t, err, "Get \"http://127.0.0.1:33001/bcdb-peer/blocks?end=4&start=2\": dial tcp 127.0.0.1:33001: connect: connection refused")

	tr2, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 5)
	require.NoError(t, err)
	defer tr2.Close()

	blocks, err = cc.GetBlocks(context.Background(), 2, 2, 4)
	require.NoError(t, err)
	require.Equal(t, 3, len(blocks))
}

func TestCatchUpClient_PullBlocks(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	})
	require.NoError(t, err)

	localConfigs, sharedConfig := newTestSetup(3)

	tr1, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 5)
	require.NoError(t, err)
	defer tr1.Close()

	tr2, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 10)
	require.NoError(t, err)
	defer tr2.Close()

	cc := comm.NewCatchUpClient(lg)
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

	localConfigs, sharedConfig := newTestSetup(4)

	tr1, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 50)
	require.NoError(t, err)
	defer tr1.Close()

	tr2, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 100)
	require.NoError(t, err)
	defer tr2.Close()

	tr3, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 2, 150)
	require.NoError(t, err)
	defer tr3.Close()

	cc := comm.NewCatchUpClient(lg)
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

func TestCatchUpClient_PullBlocksRetry(t *testing.T) {
	dir, err := ioutil.TempDir("", "catchup")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	lg, err := logger.New(&logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout", path.Join(dir, "test.log")},
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

	localConfigs, sharedConfig := newTestSetup(3)

	tr1, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 50)
	require.NoError(t, err)
	defer tr1.Close()

	cc := comm.NewCatchUpClient(lg)
	require.NotNil(t, cc)
	err = cc.UpdateMembers(sharedConfig.ConsensusConfig.Members)
	require.NoError(t, err)

	ledger4 := &memLedger{}

	var num uint64
	var target uint64 = 150
	var wg sync.WaitGroup
	wg.Add(1)

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
		wg.Done()
	}

	go pullBlocksLoop()

	//TODO do "eventually" on `Retry interval max reached` instead, as this `Sleep` may creates a flake, see: https://github.com/IBM-Blockchain/bcdb-server/issues/188
	time.Sleep(100 * time.Millisecond)
	tr2, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 1, 100)
	require.NoError(t, err)
	defer tr2.Close()

	time.Sleep(100 * time.Millisecond)
	tr3, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 2, 150)
	require.NoError(t, err)
	defer tr3.Close()

	wg.Wait()

	h, err := ledger4.Height()
	require.NoError(t, err)
	require.Equal(t, target, h)

	logBytes, err := ioutil.ReadFile(path.Join(dir, "test.log"))
	require.NoError(t, err)
	require.Contains(t, string(logBytes), "Retry interval max reached")
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

	localConfigs, sharedConfig := newTestSetup(3)

	tr1, err := startTransportWithLedger(t, lg, localConfigs, sharedConfig, 0, 50)
	require.NoError(t, err)
	defer tr1.Close()

	cc := comm.NewCatchUpClient(lg)
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

func startTransportWithLedger(t *testing.T, lg *logger.SugarLogger, localConfigs []*config.LocalConfiguration, sharedConfig *types.ClusterConfig, index, height uint64) (*comm.HTTPTransport, error) {
	ledger := &memLedger{}
	for n := uint64(1); n <= height; n++ {
		ledger.Append(&types.Block{Header: &types.BlockHeader{BaseHeader: &types.BlockHeaderBase{Number: n}}})
	}
	cl := &mocks.ConsensusListener{}
	tr := comm.NewHTTPTransport(&comm.Config{
		LocalConf:    localConfigs[index],
		Logger:       lg,
		LedgerReader: ledger,
	})
	require.NotNil(t, tr)
	err := tr.SetConsensusListener(cl)
	require.NoError(t, err)
	err = tr.UpdateClusterConfig(sharedConfig)
	require.NoError(t, err)

	err = tr.Start()
	require.NoError(t, err)
	return tr, err
}
