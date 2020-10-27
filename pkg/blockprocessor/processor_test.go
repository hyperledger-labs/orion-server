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

	"github.ibm.com/blockchaindb/library/pkg/crypto"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/queue"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type testEnv struct {
	v              *BlockProcessor
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

	v := New(&Config{
		BlockQueue: queue.New(10),
		BlockStore: blockStore,
		DB:         db,
		Logger:     logger,
	})
	go v.Run()

	return &testEnv{
		v:              v,
		db:             db,
		dbPath:         dir,
		blockStore:     blockStore,
		blockStorePath: blockStorePath,
		cleanup:        cleanup,
	}
}

func TestValidatorAndCommitter(t *testing.T) {
	cert, err := ioutil.ReadFile("./testdata/sample.cert")
	require.NoError(t, err)
	dcCert, _ := pem.Decode(cert)

	setup := func(env *testEnv) {
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

		env.v.blockQueue.Enqueue(configBlock)
		assertConfigHasCommitted := func() bool {
			exist, err := env.v.validator.configTxValidator.identityQuerier.DoesUserExist("admin1")
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

		require.NoError(t, env.db.Commit(createUser))
	}

	createBlock2 := func() *types.Block {
		return &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 2,
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
										Key:   "key1",
										Value: []byte("value-1"),
									},
								},
							},
						},
					},
				},
			},
		}
	}

	createBlock3 := func() *types.Block {
		return &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 3,
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
										Key:   "key1",
										Value: []byte("new-value-1"),
									},
								},
							},
						},
					},
				},
			},
		}
	}

	t.Run("enqueue-one-block", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup()

		setup(env)

		testCases := []struct {
			block               *types.Block
			dbName              string
			key                 string
			expectedValue       []byte
			expectedMetadata    *types.Metadata
			expectedBlockHeight uint64
		}{
			{
				block:         createBlock2(),
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
				block:         createBlock3(),
				key:           "key1",
				expectedValue: []byte("new-value-1"),
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
			env.v.blockQueue.Enqueue(tt.block)
			require.Eventually(t, env.v.blockQueue.IsEmpty, 2*time.Second, 100*time.Millisecond)

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

		setup(env)

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
					createBlock2(),
					createBlock3(),
				},
				key:           "key1",
				expectedValue: []byte("new-value-1"),
				expectedMetadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 3,
						TxNum:    0,
					},
				},
				expectedBlockHeight: 3,
				expectedBlocks: []*types.Block{
					createBlock2(),
					createBlock3(),
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
				env.v.blockQueue.Enqueue(block)
			}
			require.Eventually(t, env.v.blockQueue.IsEmpty, 2*time.Second, 100*time.Millisecond)

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
				expectedBlockJson, _ := json.Marshal(expectedBlock)
				blockJson, _ := json.Marshal(block)
				require.NoError(t, err)
				require.True(t, proto.Equal(expectedBlock, block), "Expected\t %s\nBlock\t\t %s\n", expectedBlockJson, blockJson)
				require.Equal(t, precalculatedSkipListBlock.Header.SkipchainHashes, block.Header.SkipchainHashes)
			}
		}
	})
}

func calculateBlockHashes(t *testing.T, genesisHash []byte, blocks []*types.Block, blockNum uint64) [][]byte {
	res := make([][]byte, 0)
	distance := uint64(1)
	blockNum -= 1

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
