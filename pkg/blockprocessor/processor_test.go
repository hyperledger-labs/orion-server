package blockprocessor

import (
	"bytes"
	"encoding/json"
	"encoding/pem"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
	"github.ibm.com/blockchaindb/server/pkg/common/crypto"
	"github.ibm.com/blockchaindb/server/pkg/common/logger"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/queue"
	"github.ibm.com/blockchaindb/server/pkg/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type testEnv struct {
	blockProcessor      *BlockProcessor
	stopBlockProcessing chan struct{}
	db                  worldstate.DB
	dbPath              string
	blockStore          *blockstore.Store
	blockStorePath      string
	cleanup             func()
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

	dir, err := ioutil.TempDir("/tmp", "validatorAndCommitter")
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
		BlockQueue: queue.New(10),
		BlockStore: blockStore,
		DB:         db,
		Logger:     logger,
	})
	stopBlockProcessing := make(chan struct{})
	go b.Run(stopBlockProcessing)

	return &testEnv{
		blockProcessor:      b,
		stopBlockProcessing: stopBlockProcessing,
		db:                  db,
		dbPath:              dir,
		blockStore:          blockStore,
		blockStorePath:      blockStorePath,
		cleanup:             cleanup,
	}
}

func setup(t *testing.T, env *testEnv) {
	cert, err := ioutil.ReadFile("./testdata/sample.cert")
	require.NoError(t, err)
	dcCert, _ := pem.Decode(cert)

	configBlock := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number: 1,
			},
		},
		Payload: &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					UserID:               "adminUser",
					ReadOldConfigVersion: nil,
					NewConfig: &types.ClusterConfig{
						Nodes: []*types.NodeConfig{
							{
								ID:          "node1",
								Address:     "127.0.0.1",
								Certificate: dcCert.Bytes,
							},
						},
						Admins: []*types.Admin{
							{
								ID:          "admin1",
								Certificate: dcCert.Bytes,
							},
						},
					},
				},
			},
		},
	}

	env.blockProcessor.blockQueue.Enqueue(configBlock)
	assertConfigHasCommitted := func() bool {
		exist, err := env.blockProcessor.validator.configTxValidator.identityQuerier.DoesUserExist("admin1")
		if err != nil || !exist {
			return false
		}
		return true
	}
	require.Eventually(t, assertConfigHasCommitted, 2*time.Second, 100*time.Millisecond)

	user := &types.User{
		ID: "testUser",
		Privilege: &types.Privilege{
			DBPermission: map[string]types.Privilege_Access{
				worldstate.DefaultDBName: types.Privilege_ReadWrite,
			},
		},
	}

	u, err := proto.Marshal(user)
	require.NoError(t, err)

	createUser := []*worldstate.DBUpdates{
		{
			DBName: worldstate.UsersDBName,
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   string(identity.UserNamespace) + "testUser",
					Value: u,
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
			},
		},
	}

	require.NoError(t, env.db.Commit(createUser, 1))

	assertUserCreation := func() bool {
		exist, err := env.blockProcessor.validator.configTxValidator.identityQuerier.DoesUserExist("testUser")
		if err != nil || !exist {
			return false
		}
		return true
	}
	require.Eventually(t, assertUserCreation, 2*time.Second, 100*time.Millisecond)
}

func TestValidatorAndCommitter(t *testing.T) {
	t.Run("enqueue-one-block", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup()
		defer close(env.stopBlockProcessing)

		setup(t, env)

		testCases := []struct {
			block               *types.Block
			dbName              string
			key                 string
			expectedValue       []byte
			expectedMetadata    *types.Metadata
			expectedBlockHeight uint64
		}{
			{
				block:         createSampleBlock(2, "key1", []byte("value-1")),
				key:           "key1",
				expectedValue: []byte("value-1"),
				expectedMetadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    0,
					},
				},
				expectedBlockHeight: 2,
			},
			{
				block:         createSampleBlock(3, "key1", []byte("value-2")),
				key:           "key1",
				expectedValue: []byte("value-2"),
				expectedMetadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 3,
						TxNum:    0,
					},
				},
				expectedBlockHeight: 3,
			},
		}

		for _, tt := range testCases {
			env.blockProcessor.blockQueue.Enqueue(tt.block)
			require.Eventually(t, env.blockProcessor.blockQueue.IsEmpty, 2*time.Second, 100*time.Millisecond)

			assertCommittedValue := func() bool {
				val, metadata, err := env.db.Get(worldstate.DefaultDBName, tt.key)
				if err != nil ||
					!bytes.Equal(tt.expectedValue, val) ||
					!proto.Equal(tt.expectedMetadata, metadata) {
					return false
				}
				return true
			}
			require.Eventually(t, assertCommittedValue, 2*time.Second, 100*time.Millisecond)

			height, err := env.blockStore.Height()
			require.NoError(t, err)
			require.Equal(t, tt.expectedBlockHeight, height)

			block, err := env.blockStore.Get(tt.block.GetHeader().GetBaseHeader().GetNumber())
			require.NoError(t, err)
			require.True(t, proto.Equal(tt.block, block))
		}
	})

	t.Run("enqueue-more-than-one-block", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup()
		defer close(env.stopBlockProcessing)

		setup(t, env)

		genesisHash, err := env.blockStore.GetHash(uint64(1))
		require.NoError(t, err)

		testCases := []struct {
			blocks              []*types.Block
			key                 string
			expectedValue       []byte
			expectedMetadata    *types.Metadata
			expectedBlockHeight uint64
			expectedBlocks      []*types.Block
		}{
			{
				blocks: []*types.Block{
					createSampleBlock(2, "key1", []byte("value-1")),
					createSampleBlock(3, "key1", []byte("value-2")),
				},
				key:           "key1",
				expectedValue: []byte("value-2"),
				expectedMetadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 3,
						TxNum:    0,
					},
				},
				expectedBlockHeight: 3,
				expectedBlocks: []*types.Block{
					createSampleBlock(2, "key1", []byte("value-1")),
					createSampleBlock(3, "key1", []byte("value-2")),
				},
			},
		}

		for _, tt := range testCases {
			for _, block := range tt.expectedBlocks {
				block.Header.SkipchainHashes = calculateBlockHashes(t, genesisHash, tt.expectedBlocks, block.Header.BaseHeader.Number)
			}
		}

		for _, tt := range testCases {
			for _, block := range tt.blocks {
				env.blockProcessor.blockQueue.Enqueue(block)
			}
			require.Eventually(t, env.blockProcessor.blockQueue.IsEmpty, 2*time.Second, 100*time.Millisecond)

			assertCommittedValue := func() bool {
				val, metadata, err := env.db.Get(worldstate.DefaultDBName, "key1")
				if err != nil ||
					!bytes.Equal(tt.expectedValue, val) ||
					!proto.Equal(tt.expectedMetadata, metadata) {
					return false
				}
				return true
			}
			require.Eventually(t, assertCommittedValue, 2*time.Second, 100*time.Millisecond)

			height, err := env.blockStore.Height()
			require.NoError(t, err)
			require.Equal(t, tt.expectedBlockHeight, height)

			for index, expectedBlock := range tt.blocks {
				precalculatedSkipListBlock := tt.expectedBlocks[index]
				block, err := env.blockStore.Get(expectedBlock.GetHeader().GetBaseHeader().GetNumber())
				expectedBlockJSON, _ := json.Marshal(expectedBlock)
				blockJSON, _ := json.Marshal(block)
				require.NoError(t, err)
				require.True(t, proto.Equal(expectedBlock, block), "Expected\t %s\nBlock\t\t %s\n", expectedBlockJSON, blockJSON)
				require.Equal(t, precalculatedSkipListBlock.Header.SkipchainHashes, block.Header.SkipchainHashes)
			}
		}
	})
}

func TestFailureAndRecovery(t *testing.T) {
	t.Run("blockstore is ahead of stateDB by 1 block -- will recover successfully", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup()

		setup(t, env)

		block2 := createSampleBlock(2, "key1", []byte("value-1"))
		block2.Header.ValidationInfo = []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}
		require.NoError(t, env.blockProcessor.committer.blockStore.Commit(block2))

		blockStoreHeight, err := env.blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(2), blockStoreHeight)

		stateDBHeight, err := env.db.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(1), stateDBHeight)

		// before committing the block to the stateDB, mimic node crash
		// by stopping the block processor goroutine
		close(env.stopBlockProcessing)

		// mimic node restart by starting the block processor goroutine
		env.stopBlockProcessing = make(chan struct{})
		defer close(env.stopBlockProcessing)
		go env.blockProcessor.Run(env.stopBlockProcessing)

		assertStateDBHeight := func() bool {
			stateDBHeight, err = env.db.Height()
			if err != nil || stateDBHeight != uint64(2) {
				return false
			}

			return true
		}
		require.Eventually(t, assertStateDBHeight, 2*time.Second, 100*time.Millisecond)
	})

	t.Run("blockstore is behind stateDB by 1 block -- will result in panic", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup()

		setup(t, env)

		block2 := createSampleBlock(2, "key1", []byte("value-1"))
		block2.Header.ValidationInfo = []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}
		require.NoError(t, env.blockProcessor.committer.commitToStateDB(block2))

		blockStoreHeight, err := env.blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(1), blockStoreHeight)

		stateDBHeight, err := env.db.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(2), stateDBHeight)

		close(env.stopBlockProcessing)

		env.stopBlockProcessing = make(chan struct{})
		defer close(env.stopBlockProcessing)
		assertPanic := func() {
			env.blockProcessor.Run(env.stopBlockProcessing)
		}
		require.PanicsWithError(t, "error while recovering node: the height of state database [2] is higher than the height of block store [1]. The node cannot be recovered", assertPanic)
	})

	t.Run("blockstore is ahead of stateDB by 2 blocks -- will result in panic", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup()

		setup(t, env)

		block2 := createSampleBlock(2, "key1", []byte("value-1"))
		block2.Header.ValidationInfo = []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}
		require.NoError(t, env.blockProcessor.committer.commitToBlockStore(block2))

		block3 := createSampleBlock(3, "key1", []byte("value-2"))
		block3.Header.ValidationInfo = []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}
		require.NoError(t, env.blockProcessor.committer.commitToBlockStore(block3))

		blockStoreHeight, err := env.blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(3), blockStoreHeight)

		stateDBHeight, err := env.db.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(1), stateDBHeight)

		close(env.stopBlockProcessing)

		env.stopBlockProcessing = make(chan struct{})
		defer close(env.stopBlockProcessing)
		assertPanic := func() {
			env.blockProcessor.Run(env.stopBlockProcessing)
		}
		require.PanicsWithError(t, "error while recovering node: the difference between the height of the block store [3] and the state database [1] cannot be greater than 1 block. The node cannot be recovered", assertPanic)
	})
}

func createSampleBlock(blockNumber uint64, key string, value []byte) *types.Block {
	return &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number: blockNumber,
			},
			ValidationInfo: []*types.ValidationInfo{
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
							UserID: "testUser",
							DBName: worldstate.DefaultDBName,
							DataWrites: []*types.DataWrite{
								{
									Key:   key,
									Value: value,
								},
							},
						},
					},
				},
			},
		},
	}
}

func calculateBlockHashes(t *testing.T, genesisHash []byte, blocks []*types.Block, blockNum uint64) [][]byte {
	res := make([][]byte, 0)
	distance := uint64(1)
	blockNum--

	for (blockNum%distance) == 0 && distance <= blockNum {
		index := blockNum - distance
		if index == 0 {
			res = append(res, genesisHash)
		} else {
			headerBytes, err := proto.Marshal(blocks[index-1].Header)
			require.NoError(t, err)
			blockHash, err := crypto.ComputeSHA256Hash(headerBytes)
			require.NoError(t, err)

			res = append(res, blockHash)
		}
		distance *= blockstore.SkipListBase
	}
	return res
}
