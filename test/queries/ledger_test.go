// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queries

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/state"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Scenario:
// GET /ledger/block/{blockNumber}
// GET /ledger/block/{blockNumber}?augmented=false
// GET /ledger/block/{blockNumber}?augmented=true
// where {blockNumber} is 1 (genesis), 2-11 (data)
func TestLedgerBlockQueries(t *testing.T) {
	dir := t.TempDir()

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
		txID, rcpt, _, err := s.WriteDataTx(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), []byte{uint8(i), uint8(i)})
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
			txID, rcpt, _, err := s.WriteDataTx(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), []byte{uint8(i), uint8(i)})
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

// Scenario:
// HTTP GET "/ledger/path?start={startId}&end={endId}" gets the shortest path between blocks
// HTTP GET "/ledger/path?start={startId}&end={endId}" with invalid query params
// where {startId} and {endId} are: out of range, reverse order
func TestLedgerPathQueries(t *testing.T) {
	dir := t.TempDir()

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
	for i := 1; i <= 41; i++ {
		txID, rcpt, _, err := s.WriteDataTx(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), []byte{uint8(i), uint8(i)})
		require.NoError(t, err)
		require.Equal(t, uint64(0), rcpt.GetTxIndex())
		txIDs = append(txIDs, txID)
	}
	blockRespEnv, err := s.QueryLastBlockStatus(t)
	require.NoError(t, err)
	require.NotNil(t, blockRespEnv)
	blockResp := blockRespEnv.GetResponse()
	require.NotNil(t, blockResp)
	require.Equal(t, uint64(42), blockResp.GetBlockHeader().GetBaseHeader().GetNumber())

	type testCase struct {
		name         string
		start        uint64
		end          uint64
		expectBlocks []uint64
		expectedErr  error
	}

	testCases := []testCase{
		{
			name:         "1 to 10",
			start:        1,
			end:          10,
			expectBlocks: []uint64{10, 9, 1}, //zero-based: {9, 8, 0},
		},
		{
			name:         "5 to 15",
			start:        5,
			end:          15,
			expectBlocks: []uint64{15, 13, 9, 5}, //zero-based: {14, 12, 8, 4},
		},
		{
			name:         "2 to 40",
			start:        2,
			end:          40,
			expectBlocks: []uint64{40, 39, 37, 33, 17, 9, 5, 3, 2}, //zero-based: {39, 38, 36, 32, 16, 8, 4, 2, 1},
		},
		{
			name:         "1 to 1",
			start:        1,
			end:          1,
			expectBlocks: []uint64{1},
		},
		{
			name:         "6 to 6",
			start:        6,
			end:          6,
			expectBlocks: []uint64{6},
		},
		{
			name:        "error: 6 to 2: reverse range",
			start:       6,
			end:         2,
			expectedErr: errors.New("error while issuing /ledger/path?start=6&end=2: query error: startId=6 > endId=2"),
		},
		{
			name:        "error: 6 to 100: end out of range",
			start:       6,
			end:         100,
			expectedErr: errors.New("error while issuing /ledger/path?start=6&end=100: error while processing 'GET /ledger/path?start=6&end=100' because can't find path in blocks skip list between 100 6: block not found: 100"),
		},
		{
			name:        "error: 0 to 6: start out of range",
			start:       0,
			end:         6,
			expectedErr: errors.New("error while issuing /ledger/path?start=0&end=6: error while processing 'GET /ledger/path?start=0&end=6' because start block number must be >=1"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resEnv, err := s.QueryLedgerPath(t, tc.start, tc.end, "admin")
			if tc.expectedErr == nil {
				require.NoError(t, err)
				require.NotNil(t, resEnv)
				res := resEnv.GetResponse()
				require.Equal(t, "node-1", res.GetHeader().NodeId)
				require.Equal(t, len(tc.expectBlocks), len(res.GetBlockHeaders()))

				for i, h := range res.GetBlockHeaders() {
					require.Equal(t, tc.expectBlocks[i], h.GetBaseHeader().GetNumber())
				}

				require.NoError(t, verifyLedgerPath(res.GetBlockHeaders()))
				require.NoError(t, verifyLedgerPath(res.GetBlockHeaders()[0:1]))
			} else {
				require.EqualError(t, err, tc.expectedErr.Error())
			}
		})
	}

	t.Run("double path: genesis (1) to 20 to last (42)", func(t *testing.T) {
		expectA := []uint64{20, 19, 17, 1}          //zero-based: {19, 18, 16, 0}
		expectB := []uint64{42, 41, 33, 25, 21, 20} //zero-based: {41, 40, 32, 24, 20, 19}

		resEnv, err := s.QueryLedgerPath(t, 1, 20, "admin")
		require.NoError(t, err)
		require.NotNil(t, resEnv)
		resA := resEnv.GetResponse()
		assert.Equal(t, "node-1", resA.GetHeader().NodeId)
		assert.Equal(t, len(expectA), len(resA.GetBlockHeaders()))

		for i, h := range resA.GetBlockHeaders() {
			assert.Equal(t, expectA[i], h.GetBaseHeader().GetNumber())
		}

		resEnv, err = s.QueryLedgerPath(t, 20, 42, "admin")
		require.NoError(t, err)
		require.NotNil(t, resEnv)
		resB := resEnv.GetResponse()
		assert.Equal(t, "node-1", resB.GetHeader().NodeId)
		assert.Equal(t, len(expectB), len(resB.GetBlockHeaders()))

		for i, h := range resB.GetBlockHeaders() {
			assert.Equal(t, expectB[i], h.GetBaseHeader().GetNumber())
		}

		path := append([]*types.BlockHeader{}, resB.GetBlockHeaders()...)
		path = append(path, resA.GetBlockHeaders()[1:]...)
		assert.NoError(t, verifyLedgerPath(path))
	})
}

// verify the hash chain from the last block to the first
func verifyLedgerPath(lp []*types.BlockHeader) error {
	currentBlockHeader := lp[0]
	for _, nextBlockHeader := range lp[1:] {
		headerBytes, err := proto.Marshal(nextBlockHeader)
		if err != nil {
			return err
		}
		nextBlockHash, err := crypto.ComputeSHA256Hash(headerBytes)
		if err != nil {
			return err
		}

		hashFound := false
		for _, hash := range currentBlockHeader.GetSkipchainHashes() {
			if bytes.Equal(nextBlockHash, hash) {
				hashFound = true
				break
			}
		}

		if !hashFound {
			return errors.Errorf("hash of block %d not found in list of skip list hashes of block %d", nextBlockHeader.GetBaseHeader().GetNumber(), currentBlockHeader.GetBaseHeader().GetNumber())
		}

		currentBlockHeader = nextBlockHeader
	}
	return nil
}

// Scenario:
// HTTP GET "/ledger/tx/receipt/{txId}" gets transaction receipt
// HTTP GET "/ledger/proof/tx/{blockId}?idx={idx}" gets proof for tx with index idx inside block blockId
// HTTP GET "/ledger/proof/tx/{blockId}?idx={idx}" with invalid query params
func TestLedgerTxProof(t *testing.T) {
	dir := t.TempDir()

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

	// add a few data blocks, one tx per block
	var txIDs []string
	var txRcpt []*types.TxReceipt
	var txEnvs []proto.Message
	for i := 1; i <= 10; i++ {
		txID, rcpt, txEnv, err := s.WriteDataTx(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), []byte{uint8(i), uint8(i)})
		require.NoError(t, err)
		require.Equal(t, uint64(0), rcpt.GetTxIndex())
		txIDs = append(txIDs, txID)
		txRcpt = append(txRcpt, rcpt)
		txEnvs = append(txEnvs, txEnv)
	}
	blockRespEnv, err := s.QueryLastBlockStatus(t)
	require.NoError(t, err)
	require.NotNil(t, blockRespEnv)
	blockResp := blockRespEnv.GetResponse()
	require.NotNil(t, blockResp)
	require.Equal(t, uint64(11), blockResp.GetBlockHeader().GetBaseHeader().GetNumber())

	t.Run("single tx in a block", func(t *testing.T) {
		for blockNum := uint64(2); blockNum <= 11; blockNum++ {
			respEnv, err := s.GetTxProof(t, "admin", blockNum, 0)
			require.NoError(t, err)
			require.NotNil(t, respEnv)
			require.Len(t, respEnv.GetResponse().GetHashes(), 1)

			ok, err := verifyTxProof(respEnv.GetResponse().GetHashes(), txRcpt[blockNum-2], txEnvs[blockNum-2])
			require.NoError(t, err)
			require.True(t, ok)
		}
	})

	t.Run("invalid: index out of range", func(t *testing.T) {
		respEnv, err := s.GetTxProof(t, "admin", 2, 1)
		require.EqualError(t, err, "error while issuing /ledger/proof/tx/2?idx=1: error while processing 'GET /ledger/proof/tx/2?idx=1' because node with index 1 is not part of merkle tree (0, 0)")
		require.Nil(t, respEnv)
	})

	t.Run("invalid: block out of range", func(t *testing.T) {
		respEnv, err := s.GetTxProof(t, "admin", 200, 0)
		require.EqualError(t, err, "error while issuing /ledger/proof/tx/200?idx=0: error while processing 'GET /ledger/proof/tx/200?idx=0' because requested block number [200] cannot be greater than the last committed block number [11]")
		require.Nil(t, respEnv)

		respEnv, err = s.GetTxProof(t, "admin", 0, 0)
		require.EqualError(t, err, "error while issuing /ledger/proof/tx/0?idx=0: error while processing 'GET /ledger/proof/tx/0?idx=0' because block not found: 0")
		require.Nil(t, respEnv)
	})
}

// Scenario:
// HTTP GET "/ledger/tx/receipt/{txId}" gets transaction receipt
// HTTP GET "/ledger/proof/tx/{blockId}?idx={idx}" gets proof for tx with index idx inside block blockId
// HTTP GET "/ledger/proof/tx/{blockId}?idx={idx}" with invalid query params
func TestLedgerAsyncTxProof(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		BlockCreationOverride: &config.BlockCreationConf{
			MaxBlockSize:                1024 * 1024,
			MaxTransactionCountPerBlock: 100,
			BlockTimeout:                1 * time.Second,
		},
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

	// add at least 10 data blocks, maximum 100 txs per block
	var txEnvs []*types.DataTxEnvelope
	for i := 1; i <= 1000; i++ {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{"admin"},
			TxId:            uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: worldstate.DefaultDBName,
					DataWrites: []*types.DataWrite{
						{
							Key:   fmt.Sprintf("key-%d", i),
							Value: []byte{uint8(i), uint8(i)},
						},
					},
				},
			},
		}

		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"admin": testutils.SignatureFromTx(t, s.AdminSigner(), dataTx)},
		}
		txEnvs = append(txEnvs, txEnv)
	}

	//post txs
	for _, txEnv := range txEnvs {
		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)
	}

	rcptEnvMap := make(map[*types.TxReceipt]*types.DataTxEnvelope)
	lastCommittedBlockNum := uint64(0)
	txsNumInBlocks := map[uint64]int{}
	allIn := func() bool {
		for _, txEnv := range txEnvs {
			resp, err := s.QueryTxReceipt(t, txEnv.GetPayload().GetTxId(), "admin")
			if err != nil {
				rcptEnvMap = make(map[*types.TxReceipt]*types.DataTxEnvelope)
				txsNumInBlocks = map[uint64]int{}
				return false
			}
			rcpt := resp.GetResponse().GetReceipt()
			if rcpt == nil {
				rcptEnvMap = make(map[*types.TxReceipt]*types.DataTxEnvelope)
				txsNumInBlocks = map[uint64]int{}
				return false
			}
			rcptEnvMap[rcpt] = txEnv
			blockNum := rcpt.GetHeader().GetBaseHeader().GetNumber()
			if blockNum > lastCommittedBlockNum {
				lastCommittedBlockNum = blockNum
			}
			v, found := txsNumInBlocks[blockNum]
			if found {
				txsNumInBlocks[blockNum] = v + 1
			} else {
				txsNumInBlocks[blockNum] = 0
			}
		}
		return true
	}
	require.Eventually(t, allIn, 30*time.Second, 100*time.Millisecond)

	t.Run("valid", func(t *testing.T) {
		for rcpt, env := range rcptEnvMap {
			blockNum := rcpt.GetHeader().GetBaseHeader().GetNumber()
			txIndex := rcpt.GetTxIndex()
			respEnv, err := s.GetTxProof(t, "admin", blockNum, txIndex)
			require.NoError(t, err)
			require.NotNil(t, respEnv)
			ok, err := verifyTxProof(respEnv.GetResponse().GetHashes(), rcpt, env)
			require.NoError(t, err)
			require.True(t, ok)
		}
	})

	t.Run("invalid: index out of range", func(t *testing.T) {
		for i := uint64(2); i <= lastCommittedBlockNum; i++ {
			respEnv, err := s.GetTxProof(t, "admin", i, 101)
			require.EqualError(t, err, "error while issuing /ledger/proof/tx/"+strconv.FormatUint(i, 10)+"?idx=101: error while processing 'GET /ledger/proof/tx/"+strconv.FormatUint(i, 10)+"?idx=101' "+
				"because node with index 101 is not part of merkle tree (0, "+strconv.Itoa(txsNumInBlocks[i])+")")
			require.Nil(t, respEnv)
		}
	})

	t.Run("invalid: block out of range", func(t *testing.T) {
		respEnv, err := s.GetTxProof(t, "admin", 200, 0)
		require.EqualError(t, err, "error while issuing /ledger/proof/tx/200?idx=0: error while processing 'GET /ledger/proof/tx/200?idx=0' "+
			"because requested block number [200] cannot be greater than the last committed block number ["+strconv.FormatUint(lastCommittedBlockNum, 10)+"]")
		require.Nil(t, respEnv)

		respEnv, err = s.GetTxProof(t, "admin", 0, 0)
		require.EqualError(t, err, "error while issuing /ledger/proof/tx/0?idx=0: error while processing 'GET /ledger/proof/tx/0?idx=0' because block not found: 0")
		require.Nil(t, respEnv)
	})
}

// Verify the validity of the proof with respect to the Tx and TxReceipt.
// receipt stores the block header and the tx-index in that block. The block header contains the Merkle tree root and the tx validation info. The validation info is indexed by the tx-index.
// tx stores the transaction envelope content.
func verifyTxProof(intermediateHashes [][]byte, receipt *types.TxReceipt, tx proto.Message) (bool, error) {
	txEnv, ok := tx.(*types.DataTxEnvelope)
	if !ok {
		return false, errors.Errorf("tx [%s] is not data transaction, only data transaction supported so far", tx.String())
	}
	valInfo := receipt.GetHeader().GetValidationInfo()[receipt.GetTxIndex()]
	txBytes, err := json.Marshal(txEnv)
	if err != nil {
		return false, errors.Wrapf(err, "can't serialize tx [%s] to json", tx.String())
	}
	viBytes, err := json.Marshal(valInfo)
	if err != nil {
		return false, errors.Wrapf(err, "can't serialize validation info [%s] to json", valInfo.String())
	}
	txHash, err := crypto.ComputeSHA256Hash(append(txBytes, viBytes...))
	if err != nil {
		return false, errors.Wrap(err, "can't calculate concatenated hash of tx and its validation info")
	}
	var currHash []byte
	for i, pHash := range intermediateHashes {
		if i == 0 {
			if !bytes.Equal(txHash, pHash) {
				return false, nil
			}
			currHash = txHash
			continue
		}
		currHash, err = crypto.ConcatenateHashes(currHash, pHash)
		if err != nil {
			return false, errors.Wrap(err, "can't calculate hash of two concatenated hashes")
		}
	}

	return bytes.Equal(receipt.GetHeader().GetTxMerkleTreeRootHash(), currHash), nil
}

// Scenario:
// HTTP GET "/ledger/proof/data/{blockId}/{dbname}/{key}?deleted={true|false}" gets proof for value associated with (dbname, key) in block blockId,
// HTTP GET "/ledger/proof/data/{blockId}/{dbname}/{key}" gets proof for value associated with (dbname, key) in block blockId
func TestLedgerDataProof(t *testing.T) {
	dir := t.TempDir()

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

	for i := 0; i < 5; i++ {
		txID, rcpt, _, err := s.WriteDataTx(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), []byte{uint8(i)})
		require.NoError(t, err)
		require.NotNil(t, rcpt)
		require.True(t, txID != "")
		require.True(t, len(rcpt.GetHeader().GetValidationInfo()) > 0)
		require.Equal(t, types.Flag_VALID, rcpt.Header.ValidationInfo[rcpt.TxIndex].Flag)
		t.Logf("tx submitted: %s, %+v", txID, rcpt)

		//get data proof
		respEnv, err := s.GetDataProof(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), "admin", rcpt.GetHeader().GetBaseHeader().GetNumber(), false)
		require.NoError(t, err)
		require.NotNil(t, respEnv)

		//verify data proof
		valHash, err := calculateValueHash(worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), []byte{uint8(i)})
		require.NoError(t, err)
		require.NotNil(t, valHash)
		ok, err := verifyDataProof(respEnv.GetResponse().GetPath(), valHash, rcpt.GetHeader().GetStateMerkleTreeRootHash(), false)
		require.NoError(t, err)
		require.True(t, ok)
	}

	//change "key1" data
	txID, rcpt2, _, err := s.WriteDataTx(t, worldstate.DefaultDBName, "key1", []byte{uint8(2)})
	require.NoError(t, err)
	require.NotNil(t, rcpt2)
	require.True(t, txID != "")
	require.True(t, len(rcpt2.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt2.Header.ValidationInfo[rcpt2.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt2)

	t.Run("valid: 'key1' exists with value 2", func(t *testing.T) {
		respEnv, err := s.GetDataProof(t, worldstate.DefaultDBName, "key1", "admin", rcpt2.GetHeader().GetBaseHeader().GetNumber(), false)
		require.NoError(t, err)
		require.NotNil(t, respEnv)

		valHash, err := calculateValueHash(worldstate.DefaultDBName, "key1", []byte{uint8(2)})
		require.NoError(t, err)
		require.NotNil(t, valHash)
		ok, err := verifyDataProof(respEnv.GetResponse().GetPath(), valHash, rcpt2.GetHeader().GetStateMerkleTreeRootHash(), false)
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("invalid: non-existing key", func(t *testing.T) {
		respEnv, err := s.GetDataProof(t, worldstate.DefaultDBName, "key8", "admin", 3, false)
		require.EqualError(t, err, "error while issuing /ledger/proof/data/bdb/a2V5OA?block=3: error while processing 'GET /ledger/proof/data/bdb/a2V5OA?block=3' because no proof for block 3, db bdb, key key8, isDeleted false found")
		require.Nil(t, respEnv)
	})

	t.Run("invalid: block out of range", func(t *testing.T) {
		respEnv, err := s.GetDataProof(t, worldstate.DefaultDBName, "key1", "admin", 10, false)
		require.EqualError(t, err, "error while issuing /ledger/proof/data/bdb/a2V5MQ?block=10: error while processing 'GET /ledger/proof/data/bdb/a2V5MQ?block=10' because block not found: 10")
		require.Nil(t, respEnv)
	})

	t.Run("invalid: isDeleted true but key still exists in the db", func(t *testing.T) {
		respEnv, err := s.GetDataProof(t, worldstate.DefaultDBName, "key1", "admin", rcpt2.GetHeader().GetBaseHeader().GetNumber(), true)
		require.EqualError(t, err, "error while issuing /ledger/proof/data/bdb/a2V5MQ?block=7&deleted=true: error while processing 'GET /ledger/proof/data/bdb/a2V5MQ?block=7&deleted=true' because no proof for block 7, db bdb, key key1, isDeleted true found")
		require.Nil(t, respEnv)
	})

	t.Run("invalid: key is in the db but in a different block", func(t *testing.T) {
		respEnv, err := s.GetDataProof(t, worldstate.DefaultDBName, "key3", "admin", 1, true)
		require.EqualError(t, err, "error while issuing /ledger/proof/data/bdb/a2V5Mw?block=1&deleted=true: error while processing 'GET /ledger/proof/data/bdb/a2V5Mw?block=1&deleted=true' because no proof for block 1, db bdb, key key3, isDeleted true found")
		require.Nil(t, respEnv)
	})

	// delete key1
	txID, rcpt3, _, err := s.DeleteDataTx(t, worldstate.DefaultDBName, "key1")
	require.NoError(t, err)
	require.NotNil(t, rcpt3)

	t.Run("valid: 'key1' deleted", func(t *testing.T) {
		respEnv, err := s.GetDataProof(t, worldstate.DefaultDBName, "key1", "admin", rcpt3.GetHeader().GetBaseHeader().GetNumber(), true)
		require.NoError(t, err)
		require.NotNil(t, respEnv)

		valHash, err := calculateValueHash(worldstate.DefaultDBName, "key1", []byte{uint8(2)})
		require.NoError(t, err)
		require.NotNil(t, valHash)
		ok, err := verifyDataProof(respEnv.GetResponse().GetPath(), valHash, rcpt3.GetHeader().GetStateMerkleTreeRootHash(), true)
		require.NoError(t, err)
		require.True(t, ok)
	})
}

func TestLedgerAsyncDataProof(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		BlockCreationOverride: &config.BlockCreationConf{
			MaxBlockSize:                1024 * 1024,
			MaxTransactionCountPerBlock: 10,
			BlockTimeout:                1 * time.Second,
		},
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

	// add at least 10 data blocks, maximum 10 txs per block
	var txEnvs []*types.DataTxEnvelope
	for i := 1; i <= 100; i++ {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{"admin"},
			TxId:            uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: worldstate.DefaultDBName,
					DataWrites: []*types.DataWrite{
						{
							Key:   fmt.Sprintf("key-%d", i),
							Value: []byte{uint8(i), uint8(i)},
						},
					},
				},
			},
		}

		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"admin": testutils.SignatureFromTx(t, s.AdminSigner(), dataTx)},
		}
		txEnvs = append(txEnvs, txEnv)
	}

	//post txs
	for _, txEnv := range txEnvs {
		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)
	}

	keyRcptMap := make(map[int]*types.TxReceipt)
	allIn := func() bool {
		for i, txEnv := range txEnvs {
			resp, err := s.QueryTxReceipt(t, txEnv.GetPayload().GetTxId(), "admin")
			if err != nil {
				return false
			}
			rcpt := resp.GetResponse().GetReceipt()
			if rcpt == nil {
				return false
			}
			keyRcptMap[i+1] = rcpt
		}
		return true
	}
	require.Eventually(t, allIn, 30*time.Second, 100*time.Millisecond)

	t.Run("valid", func(t *testing.T) {
		for i, rcpt := range keyRcptMap {
			respEnv, err := s.GetDataProof(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), "admin", rcpt.GetHeader().GetBaseHeader().GetNumber(), false)
			require.NoError(t, err)
			require.NotNil(t, respEnv)

			valHash, err := calculateValueHash(worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), []byte{uint8(i), uint8(i)})
			require.NoError(t, err)
			require.NotNil(t, valHash)
			ok, err := verifyDataProof(respEnv.GetResponse().GetPath(), valHash, rcpt.GetHeader().GetStateMerkleTreeRootHash(), false)
			require.NoError(t, err)
			require.True(t, ok)
		}
	})

	// change key1 value
	txID, rcpt2, _, err := s.WriteDataTx(t, worldstate.DefaultDBName, "key1", []byte{uint8(2)})
	require.NoError(t, err)
	require.NotNil(t, rcpt2)
	require.True(t, txID != "")
	require.True(t, len(rcpt2.GetHeader().GetValidationInfo()) > 0)
	require.Equal(t, types.Flag_VALID, rcpt2.Header.ValidationInfo[rcpt2.TxIndex].Flag)
	t.Logf("tx submitted: %s, %+v", txID, rcpt2)

	t.Run("valid: 'key1' exists with value 2", func(t *testing.T) {
		respEnv, err := s.GetDataProof(t, worldstate.DefaultDBName, "key1", "admin", rcpt2.GetHeader().GetBaseHeader().GetNumber(), false)
		require.NoError(t, err)
		require.NotNil(t, respEnv)

		valHash, err := calculateValueHash(worldstate.DefaultDBName, "key1", []byte{uint8(2)})
		require.NoError(t, err)
		require.NotNil(t, valHash)
		ok, err := verifyDataProof(respEnv.GetResponse().GetPath(), valHash, rcpt2.GetHeader().GetStateMerkleTreeRootHash(), false)
		require.NoError(t, err)
		require.True(t, ok)
	})

	// delete key1
	txID, rcpt3, _, err := s.DeleteDataTx(t, worldstate.DefaultDBName, "key1")
	require.NoError(t, err)
	require.NotNil(t, rcpt3)

	t.Run("valid: 'key1' deleted", func(t *testing.T) {
		respEnv, err := s.GetDataProof(t, worldstate.DefaultDBName, "key1", "admin", rcpt3.GetHeader().GetBaseHeader().GetNumber(), true)
		require.NoError(t, err)
		require.NotNil(t, respEnv)

		valHash, err := calculateValueHash(worldstate.DefaultDBName, "key1", []byte{uint8(2)})
		require.NoError(t, err)
		require.NotNil(t, valHash)
		ok, err := verifyDataProof(respEnv.GetResponse().GetPath(), valHash, rcpt3.GetHeader().GetStateMerkleTreeRootHash(), true)
		require.NoError(t, err)
		require.True(t, ok)
	})

}

// Verify validates correctness of path and checks is path first element contains valueHash
// and last element is trie root
func verifyDataProof(path []*types.MPTrieProofElement, valueHash, rootHash []byte, isDeleted bool) (bool, error) {
	pathLen := len(path)
	if pathLen == 0 {
		return false, errors.New("proof can't be empty")
	}

	// In case deleted value, node that contains it should contain []byte{1} between its hashes/bytes
	if isDeleted {
		isDeleteFound := false
		for _, hash := range path[0].GetHashes() {
			if bytes.Equal(hash, []byte{1}) {
				isDeleteFound = true
				break
			}
		}
		if !isDeleteFound {
			return false, nil
		}
	}

	hashToFind := valueHash

	// Validation algorithm just checks is hashToFind (current node/value hash) is part of hashes/bytes
	// list in node above. We start from value hash (valueHash) and continue to root stored in block
	for i := 0; i < pathLen; i++ {
		isHashFound := false
		for _, hash := range path[i].GetHashes() {
			if bytes.Equal(hash, hashToFind) {
				isHashFound = true
				break
			}
		}
		if !isHashFound {
			return false, nil
		}

		var err error
		// hash here calculated same way as node hash calculated
		hashToFind, err = state.CalcHash(path[i].GetHashes())
		if err != nil {
			return false, err
		}
	}
	// Check if calculated root hash if equal to supplied (stored in block)
	return bytes.Equal(rootHash, hashToFind), nil
}

func calculateValueHash(dbName, key string, value []byte) ([]byte, error) {
	stateTrieKey, err := state.ConstructCompositeKey(dbName, key)
	if err != nil {
		return nil, err
	}
	valueHash, err := state.CalculateKeyValueHash(stateTrieKey, value)
	if err != nil {
		return nil, err
	}
	return valueHash, nil
}

func TestLedgerAsyncDataMPTrieDisabled(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		BlockCreationOverride: &config.BlockCreationConf{
			MaxBlockSize:                1024 * 1024,
			MaxTransactionCountPerBlock: 10,
			BlockTimeout:                1 * time.Second,
		},
		DisableStateMPTrie: true,
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

	// add at least 10 data blocks, maximum 10 txs per block
	var txEnvs []*types.DataTxEnvelope
	for i := 1; i <= 100; i++ {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{"admin"},
			TxId:            uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: worldstate.DefaultDBName,
					DataWrites: []*types.DataWrite{
						{
							Key:   fmt.Sprintf("key-%d", i),
							Value: []byte{uint8(i), uint8(i)},
						},
					},
				},
			},
		}

		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"admin": testutils.SignatureFromTx(t, s.AdminSigner(), dataTx)},
		}
		txEnvs = append(txEnvs, txEnv)
	}

	//post txs
	for _, txEnv := range txEnvs {
		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)
	}

	keyRcptMap := make(map[int]*types.TxReceipt)
	allIn := func() bool {
		for i, txEnv := range txEnvs {
			resp, err := s.QueryTxReceipt(t, txEnv.GetPayload().GetTxId(), "admin")
			if err != nil {
				return false
			}
			rcpt := resp.GetResponse().GetReceipt()
			if rcpt == nil {
				return false
			}
			keyRcptMap[i+1] = rcpt
		}
		return true
	}
	require.Eventually(t, allIn, 30*time.Second, 100*time.Millisecond)

	t.Run("service unavailable", func(t *testing.T) {
		for i, rcpt := range keyRcptMap {
			respEnv, err := s.GetDataProof(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), "admin", rcpt.GetHeader().GetBaseHeader().GetNumber(), false)
			require.Error(t, err)
			require.Nil(t, respEnv)
		}
	})
}

// Scenario:
// HTTP GET "/ledger/tx/content/{blockId}?idx={idx}" gets the tx envelope with index idx inside block blockId
// HTTP GET "/ledger/tx/content/{blockId}?idx={idx}" with invalid query params
func TestLedgerTxContent(t *testing.T) {
	dir := t.TempDir()

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

	// add 2000 data blocks, one tx per block
	for i := 1; i <= 2000; i++ {
		_, rcpt, _, err := s.WriteDataTx(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", 1), []byte{uint8(1), uint8(1)})
		require.NoError(t, err)
		require.Equal(t, uint64(0), rcpt.GetTxIndex())
	}

	// get data blocks from block number 2000 to 1500 in a reverse order and save for future comparison
	var txContents []*types.GetTxResponseEnvelope
	for n := 2001; n >= 1501; n-- {
		txContent, err := s.QueryTxContent(t, uint64(n), 0, "admin")
		txContents = append(txContents, txContent)
		require.NoError(t, err)
		require.NotNil(t, txContent)
		rsp := txContent.GetResponse()
		require.NotNil(t, rsp)
		require.Equal(t, uint64(n), rsp.GetVersion().GetBlockNum())
		require.Equal(t, uint64(0), txContent.GetResponse().GetVersion().GetTxNum())
	}

	// perform data transactions to add 500 more blocks and
	for i := 2001; i <= 2500; i++ {
		_, rcpt, _, err := s.WriteDataTx(t, worldstate.DefaultDBName, fmt.Sprintf("key-%d", i), []byte{uint8(i), uint8(i)})
		require.NoError(t, err)
		require.Equal(t, uint64(0), rcpt.GetTxIndex())
	}

	// get data blocks from block number 2000 to 1500 again and compare txs
	for n := 2001; n >= 1501; n-- {
		txContent, err := s.QueryTxContent(t, uint64(n), 0, "admin")
		require.NoError(t, err)
		require.NotNil(t, txContent)
		require.True(t, proto.Equal(txContent.GetResponse(), txContents[2001-n].GetResponse()))
	}

	// invalid cases
	t.Run("invalid: index out of range", func(t *testing.T) {
		respEnv, err := s.QueryTxContent(t, 2, 1, "admin")
		require.EqualError(t, err, "error while processing 'GET /ledger/tx/content/2?idx=1' because transaction index out of range: 1")
		require.Nil(t, respEnv)
	})

	t.Run("invalid: block out of range", func(t *testing.T) {
		respEnv, err := s.QueryTxContent(t, 3000, 0, "admin")
		require.EqualError(t, err, "error while processing 'GET /ledger/tx/content/3000?idx=0' because requested block number [3000] cannot be greater than the last committed block number [2501]")
		require.Nil(t, respEnv)

		respEnv, err = s.QueryTxContent(t, 0, 0, "admin")
		require.EqualError(t, err, "error while processing 'GET /ledger/tx/content/0?idx=0' because block not found: 0")
		require.Nil(t, respEnv)
	})
}
