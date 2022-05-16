// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package queries

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
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
			respEnv.GetResponse().GetHashes()
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

	return bytes.Equal(receipt.GetHeader().GetTxMerkelTreeRootHash(), currHash), nil
}
