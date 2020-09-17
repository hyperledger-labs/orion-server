package blockprocessor

import (
	"bytes"
	"encoding/pem"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/library/pkg/logger"
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
	t.Parallel()

	setup := func(db worldstate.DB) {
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

		require.NoError(t, db.Commit(createUser))
	}

	cert, err := ioutil.ReadFile("./testdata/sample.cert")
	require.NoError(t, err)
	dcCert, _ := pem.Decode(cert)

	configBlock := &types.Block{
		Header: &types.BlockHeader{
			Number: 1,
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

	block2 := &types.Block{
		Header: &types.BlockHeader{
			Number: 2,
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

	block3 := &types.Block{
		Header: &types.BlockHeader{
			Number: 3,
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

	t.Run("enqueue-one-block", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup()

		setup(env.db)

		env.v.blockQueue.Enqueue(configBlock)
		require.Eventually(t, env.v.blockQueue.IsEmpty, 2*time.Second, 100*time.Millisecond)
		exist, err := env.v.validator.configTxValidator.identityQuerier.DoesUserExist("admin1")
		require.NoError(t, err)
		require.True(t, exist)

		testCases := []struct {
			block               *types.Block
			dbName              string
			key                 string
			expectedValue       []byte
			expectedMetadata    *types.Metadata
			expectedBlockHeight uint64
		}{
			{
				block:         block2,
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
				block:         block3,
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

			val, metadata, err := env.db.Get(worldstate.DefaultDBName, tt.key)
			require.NoError(t, err)
			require.Equal(t, tt.expectedValue, val)
			require.True(t, proto.Equal(tt.expectedMetadata, metadata))

			height, err := env.blockStore.Height()
			require.NoError(t, err)
			require.Equal(t, tt.expectedBlockHeight, height)

			block, err := env.blockStore.Get(tt.block.Header.Number)
			require.NoError(t, err)
			require.True(t, proto.Equal(tt.block, block))
		}
	})

	t.Run("enqueue-more-than-one-block", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup()

		setup(env.db)

		env.v.blockQueue.Enqueue(configBlock)
		require.Eventually(t, env.v.blockQueue.IsEmpty, 2*time.Second, 100*time.Millisecond)
		exist, err := env.v.validator.configTxValidator.identityQuerier.DoesUserExist("admin1")
		require.NoError(t, err)
		require.True(t, exist)

		testCases := []struct {
			blocks              []*types.Block
			key                 string
			expectedValue       []byte
			expectedMetadata    *types.Metadata
			expectedBlockHeight uint64
		}{
			{
				blocks: []*types.Block{
					block2,
					block3,
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
			},
		}

		for _, tt := range testCases {
			for _, block := range tt.blocks {
				env.v.blockQueue.Enqueue(block)
			}
			require.Eventually(t, env.v.blockQueue.IsEmpty, 2*time.Second, 100*time.Millisecond)

			assertCommittedValue := func() bool {
				val, metadata, err := env.db.Get(worldstate.DefaultDBName, "key1")
				if err != nil {
					return false
				}

				if !bytes.Equal(tt.expectedValue, val) {
					return false
				}

				if !proto.Equal(tt.expectedMetadata, metadata) {
					return false
				}

				height, err := env.blockStore.Height()
				if err != nil {
					return false
				}
				if !(tt.expectedBlockHeight == height) {
					return false
				}

				return true
			}

			require.Eventually(t, assertCommittedValue, 2*time.Second, 100*time.Millisecond)

			for _, expectedBlock := range tt.blocks {
				block, err := env.blockStore.Get(expectedBlock.Header.Number)
				require.NoError(t, err)
				require.True(t, proto.Equal(expectedBlock, block))
			}
		}
	})
}
