// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockstore

import (
	"fmt"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

type testEnv struct {
	storeDir string
	s        *Store
	cleanup  func(bool)
}

func newTestEnv(t *testing.T) *testEnv {
	storeDir := t.TempDir()

	lc := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(lc)
	require.NoError(t, err)

	c := &Config{
		StoreDir: storeDir,
		Logger:   logger,
	}

	store, err := Open(c)
	if err != nil {
		t.Fatalf("error while opening store on path %s, %v", storeDir, err)
	}

	return &testEnv{
		storeDir: storeDir,
		s:        store,
		cleanup: func(closeStore bool) {
			if closeStore {
				if err := store.Close(); err != nil {
					t.Errorf("error while closing the store %s, %v", storeDir, err)
				}
			}

			if err := os.RemoveAll(storeDir); err != nil {
				t.Fatalf("error while removing directory %s, %v", storeDir, err)
			}
		},
	}
}

func (e *testEnv) closeAndReOpenStore(t *testing.T) {
	logger := e.s.logger
	require.NoError(t, e.s.Close())
	e.s = nil

	store, err := Open(&Config{
		StoreDir: e.storeDir,
		Logger:   logger,
	})
	require.NoError(t, err)
	e.s = store

	e.cleanup = func(closeStore bool) {
		if closeStore {
			if err := store.Close(); err != nil {
				t.Fatalf("error while closing the store %s, %v", e.storeDir, err)
			}
		}
	}
}

func TestCommitAndQuery(t *testing.T) {
	chunkSizeLimit = 64 * 1024 * 1024
	t.Run("commit blocks and query", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup(false)

		totalBlocks := uint64(1000)
		var prevBlockBaseHash, prevBlockHash []byte
		blockHashes := [][]byte{nil}

		assertBlocks := func(startBlockNum, endBlockNum uint64) {
			var prevBlockBaseHash, prevBlockHash []byte

			if startBlockNum > 1 {
				var err error

				prevBlockBaseHash, err = env.s.GetBaseHeaderHash(startBlockNum - 1)
				require.NoError(t, err)

				prevBlockHash, err = env.s.GetHash(startBlockNum - 1)
				require.NoError(t, err)
			}

			for blockNumber := startBlockNum; blockNumber <= endBlockNum; blockNumber++ {
				expectedBlock := createSampleUserTxBlock(blockNumber, prevBlockBaseHash, prevBlockHash)
				expectedBlock.Header.SkipchainHashes = calculateBlockHashes(t, blockHashes, blockNumber)

				block, err := env.s.Get(blockNumber)
				require.NoError(t, err)
				require.True(t, proto.Equal(expectedBlock, block))
				blockHeader, err := env.s.GetHeader(blockNumber)
				require.NoError(t, err)
				require.True(t, proto.Equal(expectedBlock.GetHeader(), blockHeader))
				augmentedBlockHeader, err := env.s.GetAugmentedHeader(blockNumber)
				require.NoError(t, err)
				require.True(t, proto.Equal(augmentedBlockHeader.GetHeader(), blockHeader))
				require.Equal(t, augmentedBlockHeader.GetTxIds()[0], fmt.Sprintf("txid-%d", blockNumber))
				for i, linkedBlockNum := range CalculateSkipListLinks(blockNumber) {
					linkedBlockHash, err := env.s.GetHash(linkedBlockNum)
					require.NoError(t, err)
					require.Equal(t, linkedBlockHash, blockHeader.GetSkipchainHashes()[i])
				}

				blockHash, err := env.s.GetHash(blockNumber)
				require.NoError(t, err)
				expectedHash, err := ComputeBlockHash(expectedBlock)
				require.NoError(t, err)
				require.Equal(t, expectedHash, blockHash)
				storedBlockHash, err := ComputeBlockHash(block)
				require.NoError(t, err)
				require.Equal(t, storedBlockHash, blockHash)

				blockHeader, err = env.s.GetHeaderByHash(expectedHash)
				require.NoError(t, err)
				require.True(t, proto.Equal(expectedBlock.GetHeader(), blockHeader))
				expectedBaseHash, err := ComputeBlockBaseHash(expectedBlock)
				require.NoError(t, err)
				baseHash, err := env.s.GetBaseHeaderHash(blockNumber)
				require.NoError(t, err)
				require.Equal(t, expectedBaseHash, baseHash)

				prevBlockBaseHash = baseHash
				prevBlockHash = blockHash
			}
		}

		for blockNumber := uint64(1); blockNumber <= totalBlocks; blockNumber++ {
			b := createSampleUserTxBlock(blockNumber, prevBlockBaseHash, prevBlockHash)

			require.NoError(t, env.s.AddSkipListLinks(b))
			_, err := env.s.Commit(b)
			require.NoError(t, err)

			height, err := env.s.Height()
			require.NoError(t, err)
			require.Equal(t, blockNumber, height)

			blockHeaderBaseBytes, err := proto.Marshal(b.GetHeader().GetBaseHeader())
			require.NoError(t, err)
			prevBlockBaseHash, err = crypto.ComputeSHA256Hash(blockHeaderBaseBytes)
			require.NoError(t, err)

			blockHeaderBytes, err := proto.Marshal(b.GetHeader())
			require.NoError(t, err)
			prevBlockHash, err = crypto.ComputeSHA256Hash(blockHeaderBytes)
			require.NoError(t, err)

			blockHashes = append(blockHashes, prevBlockHash)

			if blockNumber%600 == 0 {
				// Note: it is necessary to read at least 4096 bytes in backward.
				// Otherwise, even without setting offset to the old position
				// after the read, the content is appended in the old offset
				// rather than writting to the offset where the read end.
				for j := blockNumber; j > blockNumber-100; j-- {
					assertBlocks(j, j)
				}
			}
		}

		assertBlocks(1, 1000)

		// close and reopen store
		env.closeAndReOpenStore(t)
		assertBlocks(1, 1000)
		env.s.Close()
	})

	t.Run("expected block is not received during commit", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup(true)

		_, err := env.s.Commit(nil)
		require.EqualError(t, err, "block cannot be nil")

		b := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 10,
				},
			},
		}
		_, err = env.s.Commit(b)
		require.EqualError(t, err, "expected block number [1] but received [10]")
	})

	t.Run("requested block is out of range", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup(true)

		block, err := env.s.Get(10)
		require.EqualError(t, err, "block store is empty")
		require.IsType(t, &errors.NotFoundErr{}, err)
		require.Nil(t, block)

		b := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 1,
				},
				ValidationInfo: []*types.ValidationInfo{
					{
						Flag: types.Flag_VALID,
					},
				},
			},
			Payload: &types.Block_UserAdministrationTxEnvelope{
				UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
					Payload: &types.UserAdministrationTx{
						UserId: "user1",
						TxId:   "tx1",
						UserDeletes: []*types.UserDelete{
							{
								UserId: "user1",
							},
						},
					},
					Signature: []byte("sign"),
				},
			},
		}
		_, err = env.s.Commit(b)
		require.NoError(t, err)

		block, err = env.s.Get(10)
		require.EqualError(t, err, "requested block number [10] cannot be greater than the last committed block number [1]")
		require.IsType(t, &errors.NotFoundErr{}, err)
		require.Nil(t, block)

		blockHeader, err := env.s.GetHeader(10)
		require.EqualError(t, err, "block not found: 10")
		require.IsType(t, &errors.NotFoundErr{}, err)
		require.Nil(t, blockHeader)

		blockHash, err := env.s.GetHash(10)
		require.EqualError(t, err, "block hash not found: 10")
		require.IsType(t, &errors.NotFoundErr{}, err)
		require.Nil(t, blockHash)

		blockHeader, err = env.s.GetHeaderByHash([]byte{0})
		require.EqualError(t, err, "block number by hash not found: 00")
		require.IsType(t, &errors.NotFoundErr{}, err)
		require.Nil(t, blockHeader)
	})
}

func TestTxValidationInfo(t *testing.T) {
	chunkSizeLimit = 4096
	t.Run("txID exist", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name  string
			block *types.Block
			txIDs []string
		}{
			{
				name: "data tx",
				block: &types.Block{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
						ValidationInfo: []*types.ValidationInfo{
							{
								Flag: types.Flag_VALID,
							},
							{
								Flag: types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
							},
							{
								Flag: types.Flag_VALID,
							},
						},
					},
					Payload: &types.Block_DataTxEnvelopes{
						DataTxEnvelopes: &types.DataTxEnvelopes{
							Envelopes: []*types.DataTxEnvelope{
								{
									Payload: &types.DataTx{
										TxId: "data-tx1",
									},
								},
								{
									Payload: &types.DataTx{
										TxId: "data-tx2",
									},
								},
								{
									Payload: &types.DataTx{
										TxId: "data-tx3",
									},
								},
							},
						},
					},
				},
				txIDs: []string{"data-tx1", "data-tx2", "data-tx3"},
			},
			{
				name: "config tx",
				block: &types.Block{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
						ValidationInfo: []*types.ValidationInfo{
							{
								Flag: types.Flag_VALID,
							},
						},
					},
					Payload: &types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: &types.ConfigTxEnvelope{
							Payload: &types.ConfigTx{
								TxId: "config-tx1",
							},
						},
					},
				},
				txIDs: []string{"config-tx1"},
			},
			{
				name: "db admin tx",
				block: &types.Block{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
						ValidationInfo: []*types.ValidationInfo{
							{
								Flag: types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
							},
						},
					},
					Payload: &types.Block_DbAdministrationTxEnvelope{
						DbAdministrationTxEnvelope: &types.DBAdministrationTxEnvelope{
							Payload: &types.DBAdministrationTx{
								TxId: "db-tx1",
							},
						},
					},
				},
				txIDs: []string{"db-tx1"},
			},
			{
				name: "user admin tx",
				block: &types.Block{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
						ValidationInfo: []*types.ValidationInfo{
							{
								Flag: types.Flag_INVALID_INCORRECT_ENTRIES,
							},
						},
					},
					Payload: &types.Block_UserAdministrationTxEnvelope{
						UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
							Payload: &types.UserAdministrationTx{
								TxId: "user-tx1",
							},
						},
					},
				},
				txIDs: []string{"user-tx1"},
			},
		}

		for _, tt := range tests {
			tt := tt
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				env := newTestEnv(t)
				defer env.cleanup(true)

				_, err := env.s.Commit(tt.block)
				require.NoError(t, err)

				for index, txID := range tt.txIDs {
					exist, err := env.s.DoesTxIDExist(txID)
					require.NoError(t, err)
					require.True(t, exist)

					txInfo, err := env.s.GetTxInfo(txID)
					require.NoError(t, err)
					require.Equal(t, uint64(index), txInfo.GetTxIndex())
					require.Equal(t, uint64(1), txInfo.GetBlockNumber())
					require.True(t, proto.Equal(tt.block.Header.ValidationInfo[index], txInfo.GetValidation()))

					valInfo, err := env.s.GetValidationInfo(txID)
					require.NoError(t, err)
					require.True(t, proto.Equal(tt.block.Header.ValidationInfo[index], valInfo))
				}
			})
		}
	})

	t.Run("txID does not exist", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup(true)

		exist, err := env.s.DoesTxIDExist("tx1")
		require.NoError(t, err)
		require.False(t, exist)

		valInfo, err := env.s.GetValidationInfo("tx1")
		require.EqualError(t, err, "txID not found: tx1")
		require.IsType(t, &errors.NotFoundErr{}, err)
		require.Nil(t, valInfo)
	})
}

func TestGetAugmentedHeader(t *testing.T) {
	chunkSizeLimit = 4096
	t.Run("data tx blocks", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup(true)

		var prevBlockBaseHash, prevBlockHash []byte
		blockHeaders := make([]*types.BlockHeader, 0)

		for blockNumber := uint64(1); blockNumber < 10; blockNumber++ {
			b := createSampleDataTxBlock(blockNumber, prevBlockBaseHash, prevBlockHash, 10)
			_, err := env.s.Commit(b)
			require.NoError(t, err)

			height, err := env.s.Height()
			require.NoError(t, err)
			require.Equal(t, blockNumber, height)
			blockHeaders = append(blockHeaders, b.GetHeader())
		}

		for blockNumber := uint64(1); blockNumber < 10; blockNumber++ {
			augmentedHeader, err := env.s.GetAugmentedHeader(blockNumber)
			require.NoError(t, err)
			require.NotNil(t, augmentedHeader)
			require.True(t, proto.Equal(augmentedHeader.GetHeader(), blockHeaders[blockNumber-1]))
			require.Equal(t, len(augmentedHeader.GetTxIds()), 10)
			for i, id := range augmentedHeader.GetTxIds() {
				require.Equal(t, id, fmt.Sprintf("tx-%d-%d", blockNumber, i))
			}
		}
	})
}

func calculateBlockHashes(t *testing.T, blockHashes [][]byte, blockNum uint64) [][]byte {
	var res [][]byte
	distance := uint64(1)

	for ((blockNum-1)%distance) == 0 && distance < blockNum {
		index := blockNum - distance

		res = append(res, blockHashes[index])
		distance *= SkipListBase
	}
	return res
}

func createSampleUserTxBlock(blockNumber uint64, prevBlockBaseHash []byte, prevBlockHash []byte) *types.Block {
	return &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number:                 blockNumber,
				PreviousBaseHeaderHash: prevBlockBaseHash,
				LastCommittedBlockHash: prevBlockHash,
				LastCommittedBlockNum:  blockNumber - 1,
			},
			TxMerkleTreeRootHash:    []byte(fmt.Sprintf("treehash-%d", blockNumber-1)),
			StateMerkleTreeRootHash: []byte(fmt.Sprintf("statehash-%d", blockNumber-1)),
			ValidationInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
		Payload: &types.Block_UserAdministrationTxEnvelope{
			UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
				Payload: &types.UserAdministrationTx{
					UserId: fmt.Sprintf("userid-%d", blockNumber),
					TxId:   fmt.Sprintf("txid-%d", blockNumber),
					UserDeletes: []*types.UserDelete{
						{
							UserId: fmt.Sprintf("userid-%d", blockNumber),
						},
					},
				},
				Signature: []byte("sign"),
			},
		},
	}
}

func createSampleDataTxBlock(blockNumber uint64, prevBlockBaseHash []byte, prevBlockHash []byte, txNum int) *types.Block {
	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number:                 blockNumber,
				PreviousBaseHeaderHash: prevBlockBaseHash,
				LastCommittedBlockHash: prevBlockHash,
				LastCommittedBlockNum:  blockNumber - 1,
			},
			TxMerkleTreeRootHash:    []byte(fmt.Sprintf("treehash-%d", blockNumber-1)),
			StateMerkleTreeRootHash: []byte(fmt.Sprintf("statehash-%d", blockNumber-1)),
			ValidationInfo:          []*types.ValidationInfo{},
		},
	}

	envelopes := make([]*types.DataTxEnvelope, 0)
	for i := 0; i < txNum; i++ {
		block.Header.ValidationInfo = append(block.Header.ValidationInfo, &types.ValidationInfo{Flag: types.Flag_VALID})
		envelopes = append(envelopes, &types.DataTxEnvelope{
			Payload: &types.DataTx{
				TxId: fmt.Sprintf("tx-%d-%d", blockNumber, i),
			},
		})
	}

	block.Payload = &types.Block_DataTxEnvelopes{
		DataTxEnvelopes: &types.DataTxEnvelopes{
			Envelopes: envelopes,
		},
	}

	return block
}
