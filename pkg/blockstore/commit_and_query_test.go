package blockstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
)

type testEnv struct {
	storeDir string
	s        *Store
	cleanup  func(bool)
}

func newTestEnv(t *testing.T) *testEnv {
	storeDir, err := ioutil.TempDir("", "blockstore")
	require.NoError(t, err)

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
		if rmErr := os.RemoveAll(storeDir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", storeDir, rmErr)
		}

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

func (e *testEnv) reopenStore(t *testing.T) {
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

		if err := os.RemoveAll(e.storeDir); err != nil {
			t.Fatalf("error while removing directory %s, %v", e.storeDir, err)
		}
	}
}

func TestCommitAndQuery(t *testing.T) {
	createSampleUserTxBlock := func(blockNumber uint64) *types.Block {
		return &types.Block{
			Header: &types.BlockHeader{
				Number: blockNumber,

				SkipchainHashes:  [][]byte{[]byte(fmt.Sprintf("hash-%d", blockNumber-1))},
				TransactionsHash: []byte(fmt.Sprintf("hash-%d", blockNumber)),
			},
			Payload: &types.Block_UserAdministrationTxEnvelope{
				UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
					Payload: &types.UserAdministrationTx{
						UserID: "user1",
						TxID:   fmt.Sprintf("txid-%d", blockNumber),
						UserDeletes: []*types.UserDelete{
							{
								UserID: "user1",
							},
						},
					},
					Signature: []byte("sign"),
				},
			},
			TxValidationInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		}
	}

	t.Run("commit blocks and query", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup(false)

		totalBlocks := uint64(1000)

		for blockNumber := uint64(1); blockNumber < totalBlocks; blockNumber++ {
			b := createSampleUserTxBlock(blockNumber)
			require.NoError(t, env.s.Commit(b))

			height, err := env.s.Height()
			require.NoError(t, err)
			require.Equal(t, blockNumber, height)
		}

		assertBlocks := func() {
			for blockNumber := uint64(1); blockNumber < totalBlocks; blockNumber++ {
				expectedBlock := createSampleUserTxBlock(blockNumber)

				block, err := env.s.Get(blockNumber)
				require.NoError(t, err)
				require.Equal(t, expectedBlock, block)
				require.True(t, proto.Equal(expectedBlock, block))

				blockHeader, err := env.s.GetHeader(blockNumber)
				require.NoError(t, err)
				require.True(t, proto.Equal(expectedBlock.GetHeader(), blockHeader))

				blockHash, err := env.s.GetHash(blockNumber)
				require.NoError(t, err)
				expectedHash, err := ComputeBlockHash(expectedBlock)
				require.NoError(t, err)
				require.Equal(t, expectedHash, blockHash)

				blockHeader, err = env.s.GetHeaderByHash(expectedHash)
				require.NoError(t, err)
				require.True(t, proto.Equal(expectedBlock.GetHeader(), blockHeader))
			}
		}

		assertBlocks()

		// close and reopen store
		env.reopenStore(t)
		assertBlocks()
		env.s.Close()
	})

	t.Run("expected block is not received during commit", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup(true)

		require.EqualError(t, env.s.Commit(nil), "block cannot be nil")

		b := &types.Block{
			Header: &types.BlockHeader{
				Number: 10,
			},
		}
		require.EqualError(t, env.s.Commit(b), "expected block number [1] but received [10]")
	})

	t.Run("requested block is out of range", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup(true)

		block, err := env.s.Get(10)
		require.EqualError(t, err, "block store is empty")
		require.Nil(t, block)

		b := &types.Block{
			Header: &types.BlockHeader{
				Number: 1,
			},
			Payload: &types.Block_UserAdministrationTxEnvelope{
				UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
					Payload: &types.UserAdministrationTx{
						UserID: "user1",
						UserDeletes: []*types.UserDelete{
							{
								UserID: "user1",
							},
						},
					},
					Signature: []byte("sign"),
				},
			},
			TxValidationInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		}
		require.NoError(t, env.s.Commit(b))

		block, err = env.s.Get(10)
		require.EqualError(t, err, "requested block number [10] cannot be greater than the last committed block number [1]")
		require.Nil(t, block)

		blockHeader, err := env.s.GetHeader(10)
		require.NoError(t, err)
		require.Nil(t, blockHeader)

		blockHash, err := env.s.GetHash(10)
		require.NoError(t, err)
		require.Nil(t, blockHash)

		blockHeader, err = env.s.GetHeaderByHash([]byte{0})
		require.NoError(t, err)
		require.Nil(t, blockHeader)
	})
}

func TestTxValidationInfo(t *testing.T) {
	t.Parallel()

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
						Number: 1,
					},
					Payload: &types.Block_DataTxEnvelopes{
						DataTxEnvelopes: &types.DataTxEnvelopes{
							Envelopes: []*types.DataTxEnvelope{
								{
									Payload: &types.DataTx{
										TxID: "data-tx1",
									},
								},
								{
									Payload: &types.DataTx{
										TxID: "data-tx2",
									},
								},
								{
									Payload: &types.DataTx{
										TxID: "data-tx3",
									},
								},
							},
						},
					},
					TxValidationInfo: []*types.ValidationInfo{
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
				txIDs: []string{"data-tx1", "data-tx2", "data-tx3"},
			},
			{
				name: "config tx",
				block: &types.Block{
					Header: &types.BlockHeader{
						Number: 1,
					},
					Payload: &types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: &types.ConfigTxEnvelope{
							Payload: &types.ConfigTx{
								TxID: "config-tx1",
							},
						},
					},
					TxValidationInfo: []*types.ValidationInfo{
						{
							Flag: types.Flag_VALID,
						},
					},
				},
				txIDs: []string{"config-tx1"},
			},
			{
				name: "db admin tx",
				block: &types.Block{
					Header: &types.BlockHeader{
						Number: 1,
					},
					Payload: &types.Block_DBAdministrationTxEnvelope{
						DBAdministrationTxEnvelope: &types.DBAdministrationTxEnvelope{
							Payload: &types.DBAdministrationTx{
								TxID: "db-tx1",
							},
						},
					},
					TxValidationInfo: []*types.ValidationInfo{
						{
							Flag: types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
						},
					},
				},
				txIDs: []string{"db-tx1"},
			},
			{
				name: "user admin tx",
				block: &types.Block{
					Header: &types.BlockHeader{
						Number: 1,
					},
					Payload: &types.Block_UserAdministrationTxEnvelope{
						UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
							Payload: &types.UserAdministrationTx{
								TxID: "user-tx1",
							},
						},
					},
					TxValidationInfo: []*types.ValidationInfo{
						{
							Flag: types.Flag_INVALID_INCORRECT_ENTRIES,
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

				require.NoError(t, env.s.Commit(tt.block))

				for index, txID := range tt.txIDs {
					exist, err := env.s.DoesTxIDExist(txID)
					require.NoError(t, err)
					require.True(t, exist)

					valInfo, err := env.s.GetValidationInfo(txID)
					require.True(t, proto.Equal(tt.block.TxValidationInfo[index], valInfo))
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
		require.NoError(t, err)
		require.Nil(t, valInfo)
	})
}
