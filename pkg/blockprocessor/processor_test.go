package blockprocessor

import (
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
	dir, err := ioutil.TempDir("/tmp", "validatorAndCommitter")
	require.NoError(t, err)

	dbPath := filepath.Join(dir, "leveldb")
	db, err := leveldb.Open(dbPath)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, err)
		}
		t.Fatalf("error while creating the leveldb instance, %v", err)
	}

	blockStorePath := filepath.Join(dir, "blockstore")
	blockStore, err := blockstore.Open(blockStorePath)
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

	block1 := &types.Block{
		Header: &types.BlockHeader{
			Number: 1,
		},
		TransactionEnvelopes: []*types.TransactionEnvelope{
			{
				Payload: &types.Transaction{
					UserID: []byte("testUser"),
					DBName: worldstate.DefaultDBName,
					Writes: []*types.KVWrite{
						{
							Key:   "key1",
							Value: []byte("value-1"),
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
		TransactionEnvelopes: []*types.TransactionEnvelope{
			{
				Payload: &types.Transaction{
					UserID: []byte("testUser"),
					DBName: worldstate.DefaultDBName,
					Writes: []*types.KVWrite{
						{
							Key:   "key1",
							Value: []byte("value-2"),
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

		require.NoError(t, env.db.Commit(createUser))

		testCases := []struct {
			block               *types.Block
			key                 string
			expectedValue       []byte
			expectedMetadata    *types.Metadata
			expectedBlockHeight uint64
		}{
			{
				block:         block1,
				key:           "key1",
				expectedValue: []byte("value-1"),
				expectedMetadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    0,
					},
				},
				expectedBlockHeight: 1,
			},
			{
				block:         block2,
				key:           "key1",
				expectedValue: []byte("value-2"),
				expectedMetadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    0,
					},
				},
				expectedBlockHeight: 2,
			},
		}

		for _, tt := range testCases {
			env.v.blockQueue.Enqueue(tt.block)
			require.Eventually(t, env.v.blockQueue.IsEmpty, 2*time.Second, 100*time.Millisecond)

			val, metadata, err := env.db.Get(worldstate.DefaultDBName, "key1")
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

		require.NoError(t, env.db.Commit(createUser))

		testCases := []struct {
			blocks              []*types.Block
			key                 string
			expectedValue       []byte
			expectedMetadata    *types.Metadata
			expectedBlockHeight uint64
		}{
			{
				blocks: []*types.Block{
					block1,
					block2,
				},
				key:           "key1",
				expectedValue: []byte("value-2"),
				expectedMetadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    0,
					},
				},
				expectedBlockHeight: 2,
			},
		}

		for _, tt := range testCases {
			for _, block := range tt.blocks {
				env.v.blockQueue.Enqueue(block)
			}
			require.Eventually(t, env.v.blockQueue.IsEmpty, 2*time.Second, 100*time.Millisecond)

			val, metadata, err := env.db.Get(worldstate.DefaultDBName, "key1")
			require.NoError(t, err)
			require.Equal(t, tt.expectedValue, val)
			require.True(t, proto.Equal(tt.expectedMetadata, metadata))

			height, err := env.blockStore.Height()
			require.NoError(t, err)
			require.Equal(t, tt.expectedBlockHeight, height)

			for _, expectedBlock := range tt.blocks {
				block, err := env.blockStore.Get(expectedBlock.Header.Number)
				require.NoError(t, err)
				require.True(t, proto.Equal(expectedBlock, block))
			}
		}
	})
}
