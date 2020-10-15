package blockcreator

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
	"github.ibm.com/blockchaindb/server/pkg/queue"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type testEnv struct {
	creator        *BlockCreator
	db             worldstate.DB
	dbPath         string
	blockStore     *blockstore.Store
	blockStorePath string
	cleanup        func()
}

func newTestEnv(t *testing.T) *testEnv {
	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("/tmp", "creator")
	require.NoError(t, err)

	dbPath := filepath.Join(dir, "leveldb")
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    logger,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, err)
		}
		t.Fatalf("error while creating the leveldb instance, %v", err)
	}

	blockStorePath := filepath.Join(dir, "blockstore")
	blockStore, err := blockstore.Open(
		&blockstore.Config{
			StoreDir: blockStorePath,
			Logger:   logger,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, err)
		}
		t.Fatalf("error while creating the block store, %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close the db instance, %v", err)
		}

		if err := blockStore.Close(); err != nil {
			t.Errorf("failed to close the blockstore, %v", err)
		}

		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove directory %s, %v", dir, err)
		}
	}

	b := New(&Config{
		TxBatchQueue:    queue.New(10),
		BlockQueue:      queue.New(10),
		NextBlockNumber: 1,
		Logger:          logger,
		BlockStore:      blockStore,
	})
	go b.Run()

	return &testEnv{
		creator:        b,
		db:             db,
		dbPath:         dir,
		blockStore:     blockStore,
		blockStorePath: blockStorePath,
		cleanup:        cleanup,
	}
}

func TestBatchCreator(t *testing.T) {
	dataTx1 := &types.DataTxEnvelope{
		Payload: &types.DataTx{
			UserID: "user1",
			DBName: "db1",
			DataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
		},
	}

	dataTx2 := &types.DataTxEnvelope{
		Payload: &types.DataTx{
			UserID: "user2",
			DBName: "db2",
			DataDeletes: []*types.DataDelete{
				{
					Key: "key2",
				},
			},
		},
	}

	userAdminTx := &types.UserAdministrationTxEnvelope{
		Payload: &types.UserAdministrationTx{
			UserID: "user1",
			UserReads: []*types.UserRead{
				{
					UserID: "user1",
				},
			},
			UserWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID:          "user2",
						Certificate: []byte("certificate"),
					},
				},
			},
		},
	}

	dbAdminTx := &types.DBAdministrationTxEnvelope{
		Payload: &types.DBAdministrationTx{
			UserID:    "user1",
			CreateDBs: []string{"db1", "db2"},
			DeleteDBs: []string{"db3", "db4"},
		},
	}

	configTx := &types.ConfigTxEnvelope{
		Payload: &types.ConfigTx{
			UserID: "user1",
			NewConfig: &types.ClusterConfig{
				Nodes: []*types.NodeConfig{
					{
						ID: "node1",
					},
				},
				Admins: []*types.Admin{
					{
						ID: "admin1",
					},
				},
				RootCACertificate: []byte("root-ca"),
			},
		},
	}

	testCases := []struct {
		name                   string
		txBatches              []interface{}
		expectedBlocks         []*types.Block
		expectedSkipListHeight []int
	}{
		{
			name: "enqueue all types of transactions",
			txBatches: []interface{}{
				&types.Block_UserAdministrationTxEnvelope{
					UserAdministrationTxEnvelope: userAdminTx,
				},
				&types.Block_DBAdministrationTxEnvelope{
					DBAdministrationTxEnvelope: dbAdminTx,
				},
				&types.Block_DataTxEnvelopes{
					DataTxEnvelopes: &types.DataTxEnvelopes{
						Envelopes: []*types.DataTxEnvelope{
							dataTx1,
							dataTx2,
						},
					},
				},
				&types.Block_ConfigTxEnvelope{
					ConfigTxEnvelope: configTx,
				},
			},
			expectedBlocks: []*types.Block{
				{
					Header: &types.BlockHeader{
						Number: 1,
					},
					Payload: &types.Block_UserAdministrationTxEnvelope{
						UserAdministrationTxEnvelope: userAdminTx,
					},
				},
				{
					Header: &types.BlockHeader{
						Number: 2,
					},
					Payload: &types.Block_DBAdministrationTxEnvelope{
						DBAdministrationTxEnvelope: dbAdminTx,
					},
				},
				{
					Header: &types.BlockHeader{
						Number: 3,
					},
					Payload: &types.Block_DataTxEnvelopes{
						DataTxEnvelopes: &types.DataTxEnvelopes{
							Envelopes: []*types.DataTxEnvelope{
								dataTx1,
								dataTx2,
							},
						},
					},
				},
				{
					Header: &types.BlockHeader{
						Number: 4,
					},
					Payload: &types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: configTx,
					},
				},
			},
			expectedSkipListHeight: []int{
				0, 1, 2, 1,
			},
		},
	}
	for _, tt := range testCases {
		// Update test case blocks skip list hashes
		for index, expectedBlock := range tt.expectedBlocks {
			expectedBlock.Header.SkipchainHashes = calculateBlockHashes(t, tt.expectedBlocks, expectedBlock.Header.Number, tt.expectedSkipListHeight[index])
		}
	}

	enqueueTxBatchesAndAssertBlocks := func(t *testing.T, testEnv *testEnv, txBatches []interface{}, expectedBlocks []*types.Block) {
		for _, txBatch := range txBatches {
			testEnv.creator.txBatchQueue.Enqueue(txBatch)
		}

		hasBlockCountMatched := func() bool {
			if len(expectedBlocks) == testEnv.creator.blockQueue.Size() {
				return true
			}
			return false
		}
		require.Eventually(t, hasBlockCountMatched, 2*time.Second, 1000*time.Millisecond)

		for _, expectedBlock := range expectedBlocks {
			block := testEnv.creator.blockQueue.Dequeue().(*types.Block)
			require.True(t, proto.Equal(expectedBlock, block))
		}
	}

	t.Run("access hash from the blockstore", func(t *testing.T) {
		for _, tt := range testCases {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				testEnv := newTestEnv(t)
				defer testEnv.cleanup()

				// storing the block in block store so that the hash would be fetched from the store itself
				for _, expectedBlock := range tt.expectedBlocks {
					require.NoError(t, testEnv.blockStore.Commit(expectedBlock))
				}

				enqueueTxBatchesAndAssertBlocks(t, testEnv, tt.txBatches, tt.expectedBlocks)
			})
		}
	})

	t.Run("access hash from the cache", func(t *testing.T) {
		for _, tt := range testCases {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				testEnv := newTestEnv(t)
				defer testEnv.cleanup()

				// storing the block hash in the cache so that the hash would be fetched from the cache rather than
				// from the block store
				for _, expectedBlock := range tt.expectedBlocks {
					h, err := blockstore.ComputeBlockHash(expectedBlock)
					require.NoError(t, err)
					testEnv.creator.blockHashCache.Put(expectedBlock.Header.Number, h)
				}

				enqueueTxBatchesAndAssertBlocks(t, testEnv, tt.txBatches, tt.expectedBlocks)
			})
		}
	})
}

func TestCache(t *testing.T) {
	t.Run("testing block hashes cache", func(t *testing.T) {
		t.Parallel()
		cache := newCache(2)

		cache.Put(uint64(0), []byte{0})
		cache.Put(uint64(1), []byte{1})

		require.Equal(t, []byte{0}, cache.Get(uint64(0)))
		require.Equal(t, []byte{1}, cache.Get(uint64(1)))
		require.Nil(t, cache.Get(uint64(2)))

		cache.Put(uint64(2), []byte{2})
		require.Equal(t, []byte{2}, cache.Get(uint64(2)))
		require.Equal(t, []byte{1}, cache.Get(uint64(1)))
		require.Nil(t, cache.Get(uint64(0)))
	})

}

func calculateBlockHashes(t *testing.T, blocks []*types.Block, blockNum uint64, expectedHeight int) [][]byte {
	res := make([][]byte, 0)
	distance := uint64(1)
	blockNum -= 1

	for (blockNum%distance) == 0 && distance <= blockNum {
		index := blockNum - distance
		headerBytes, err := proto.Marshal(blocks[index].Header)
		require.NoError(t, err)
		blockHash, err := crypto.ComputeSHA256Hash(headerBytes)
		require.NoError(t, err)

		res = append(res, blockHash)
		distance *= skipListBase
	}
	require.Equal(t, expectedHeight, len(res))
	return res
}
