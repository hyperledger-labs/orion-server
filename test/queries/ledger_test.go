// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queries

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
)

// Scenario:
// GET /ledger/block/{blockNumber}
// GET /ledger/block/{blockNumber}?augmented=false
// GET /ledger/block/{blockNumber}?augmented=true
// where {blockNumber} is 1 (genesis), 2-11 (data)
func TestLedgerBlockQueries(t *testing.T) {
	dir, err := ioutil.TempDir("", "int-test")
	require.NoError(t, err)

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())
	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	s := c.Servers[leaderIndex]

	// add a few data blocks
	var txIDs []string
	for i := 1; i <= 10; i++ {
		txID, rcpt, err := s.WriteDataTx(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), []byte{uint8(i), uint8(i)})
		require.NoError(t, err)
		require.Equal(t, uint64(0), rcpt.GetTxIndex())
		txIDs = append(txIDs, txID)
	}

	t.Run("get the genesis block", func(t *testing.T) {
		blockRespEnv, err := s.QueryBlockHeader(t, 1, false, "admin")
		require.NoError(t, err)
		require.NotNil(t, blockRespEnv)
		blockResp := blockRespEnv.GetResponse()
		require.NotNil(t, blockResp)
		require.Equal(t, uint64(1), blockResp.GetBlockHeader().GetBaseHeader().GetNumber())

		blockRespEnv, err = s.QueryBlockHeader(t, 1, true, "admin")
		require.NoError(t, err)
		require.NotNil(t, blockRespEnv)
		blockResp = blockRespEnv.GetResponse()
		require.NotNil(t, blockResp)
		require.Equal(t, uint64(1), blockResp.GetBlockHeader().GetBaseHeader().GetNumber())

		augBlockRespEnv, err := s.QueryAugmentedBlockHeader(t, 1, "admin")
		require.NoError(t, err)
		require.NotNil(t, augBlockRespEnv)
		augBlockResp := augBlockRespEnv.GetResponse()
		require.NotNil(t, augBlockResp)
		require.Equal(t, uint64(1), augBlockResp.GetBlockHeader().GetHeader().GetBaseHeader().GetNumber())
		require.Len(t, augBlockResp.GetBlockHeader().GetTxIds(), 1)
		t.Logf("genesis txID: %s", augBlockResp.GetBlockHeader().GetTxIds()[0])
	})

	t.Run("get data blocks", func(t *testing.T) {
		for n := uint64(2); n <= 11; n++ {
			blockRespEnv, err := s.QueryBlockHeader(t, n, false, "admin")
			require.NoError(t, err)
			require.NotNil(t, blockRespEnv)
			blockResp := blockRespEnv.GetResponse()
			require.NotNil(t, blockResp)
			require.Equal(t, n, blockResp.GetBlockHeader().GetBaseHeader().GetNumber())

			blockRespEnv, err = s.QueryBlockHeader(t, n, true, "admin")
			require.NoError(t, err)
			require.NotNil(t, blockRespEnv)
			blockResp = blockRespEnv.GetResponse()
			require.NotNil(t, blockResp)
			require.Equal(t, n, blockResp.GetBlockHeader().GetBaseHeader().GetNumber())

			augBlockRespEnv, err := s.QueryAugmentedBlockHeader(t, n, "admin")
			require.NoError(t, err)
			require.NotNil(t, augBlockRespEnv)
			augBlockResp := augBlockRespEnv.GetResponse()
			require.NotNil(t, augBlockResp)
			require.Equal(t, n, augBlockResp.GetBlockHeader().GetHeader().GetBaseHeader().GetNumber())
			require.Len(t, augBlockResp.GetBlockHeader().GetTxIds(), 1)
			require.Equal(t, txIDs[n-2], augBlockResp.GetBlockHeader().GetTxIds()[0])
			t.Logf("block %d txID: %s", n, augBlockResp.GetBlockHeader().GetTxIds()[0])
		}

		blockRespEnv, err := s.QueryLastBlockStatus(t)
		require.NoError(t, err)
		require.NotNil(t, blockRespEnv)
		blockResp := blockRespEnv.GetResponse()
		require.NotNil(t, blockResp)
		require.Equal(t, uint64(11), blockResp.GetBlockHeader().GetBaseHeader().GetNumber())
	})

	t.Run("add blocks and get data blocks again", func(t *testing.T) {
		// add a few data blocks
		for i := 11; i <= 20; i++ {
			txID, rcpt, err := s.WriteDataTx(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), []byte{uint8(i), uint8(i)})
			require.NoError(t, err)
			require.Equal(t, uint64(0), rcpt.GetTxIndex())
			txIDs = append(txIDs, txID)
		}

		for n := uint64(12); n <= 21; n++ {
			blockRespEnv, err := s.QueryBlockHeader(t, n, false, "admin")
			require.NoError(t, err)
			require.NotNil(t, blockRespEnv)
			blockResp := blockRespEnv.GetResponse()
			require.NotNil(t, blockResp)
			require.Equal(t, n, blockResp.GetBlockHeader().GetBaseHeader().GetNumber())

			blockRespEnv, err = s.QueryBlockHeader(t, n, true, "admin")
			require.NoError(t, err)
			require.NotNil(t, blockRespEnv)
			blockResp = blockRespEnv.GetResponse()
			require.NotNil(t, blockResp)
			require.Equal(t, n, blockResp.GetBlockHeader().GetBaseHeader().GetNumber())

			augBlockRespEnv, err := s.QueryAugmentedBlockHeader(t, n, "admin")
			require.NoError(t, err)
			require.NotNil(t, augBlockRespEnv)
			augBlockResp := augBlockRespEnv.GetResponse()
			require.NotNil(t, augBlockResp)
			require.Equal(t, n, augBlockResp.GetBlockHeader().GetHeader().GetBaseHeader().GetNumber())
			require.Len(t, augBlockResp.GetBlockHeader().GetTxIds(), 1)
			require.Equal(t, txIDs[n-2], augBlockResp.GetBlockHeader().GetTxIds()[0])
			t.Logf("block %d txID: %s", n, augBlockResp.GetBlockHeader().GetTxIds()[0])
		}

		blockRespEnv, err := s.QueryLastBlockStatus(t)
		require.NoError(t, err)
		require.NotNil(t, blockRespEnv)
		blockResp := blockRespEnv.GetResponse()
		require.NotNil(t, blockResp)
		require.Equal(t, uint64(21), blockResp.GetBlockHeader().GetBaseHeader().GetNumber())
	})

	t.Run("get block 0: not found", func(t *testing.T) {
		blockRespEnv, err := s.QueryBlockHeader(t, 0, false, "admin")
		require.EqualError(t, err, "error while issuing /ledger/block/0: error while processing 'GET /ledger/block/0' because block not found: 0")
		require.Nil(t, blockRespEnv)

		blockRespEnv, err = s.QueryBlockHeader(t, 0, true, "admin")
		require.EqualError(t, err, "error while issuing /ledger/block/0?augmented=false: error while processing 'GET /ledger/block/0?augmented=false' because block not found: 0")
		require.Nil(t, blockRespEnv)

		augBlockRespEnv, err := s.QueryAugmentedBlockHeader(t, 0, "admin")
		require.EqualError(t, err, "error while issuing /ledger/block/0?augmented=true: error while processing 'GET /ledger/block/0?augmented=true' because block not found: 0")
		require.Nil(t, augBlockRespEnv)
	})

	t.Run("get block 100: not found", func(t *testing.T) {
		blockRespEnv, err := s.QueryBlockHeader(t, 100, false, "admin")
		require.EqualError(t, err, "error while issuing /ledger/block/100: error while processing 'GET /ledger/block/100' because block not found: 100")
		require.Nil(t, blockRespEnv)

		blockRespEnv, err = s.QueryBlockHeader(t, 100, true, "admin")
		require.EqualError(t, err, "error while issuing /ledger/block/100?augmented=false: error while processing 'GET /ledger/block/100?augmented=false' because block not found: 100")
		require.Nil(t, blockRespEnv)

		augBlockRespEnv, err := s.QueryAugmentedBlockHeader(t, 100, "admin")
		require.EqualError(t, err, "error while issuing /ledger/block/100?augmented=true: error while processing 'GET /ledger/block/100?augmented=true' because block not found: 100")
		require.Nil(t, augBlockRespEnv)
	})

}
