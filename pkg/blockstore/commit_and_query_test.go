package blockstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
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

	store, err := Open(storeDir)
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
	require.NoError(t, e.s.Close())
	e.s = nil

	store, err := Open(e.storeDir)
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
	t.Run("commit blocks and query", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup(false)

		chunkSizeLimit = 4096
		totalBlocks := uint64(1000)
		for blockNumber := uint64(1); blockNumber < totalBlocks; blockNumber++ {
			b := &types.Block{
				Header: &types.BlockHeader{
					Number:                  blockNumber,
					PreviousBlockHeaderHash: []byte(fmt.Sprintf("hash-%d", blockNumber-1)),
					TransactionsHash:        []byte(fmt.Sprintf("hash-%d", blockNumber)),
				},
				TransactionEnvelopes: []*types.TransactionEnvelope{
					{
						Payload: &types.Transaction{
							Type:   0,
							DBName: "bdb",
						},
						Signature: []byte("sign"),
					},
				},
			}

			require.NoError(t, env.s.Commit(b))

			height, err := env.s.Height()
			require.NoError(t, err)
			require.Equal(t, blockNumber, height)
		}

		assertBlocks := func() {
			for blockNumber := uint64(1); blockNumber < totalBlocks; blockNumber++ {
				expectedBlock := &types.Block{
					Header: &types.BlockHeader{
						Number:                  blockNumber,
						PreviousBlockHeaderHash: []byte(fmt.Sprintf("hash-%d", blockNumber-1)),
						TransactionsHash:        []byte(fmt.Sprintf("hash-%d", blockNumber)),
					},
					TransactionEnvelopes: []*types.TransactionEnvelope{
						{
							Payload: &types.Transaction{
								Type:   0,
								DBName: "bdb",
							},
							Signature: []byte("sign"),
						},
					},
				}

				block, err := env.s.Get(blockNumber)
				require.NoError(t, err)
				require.True(t, proto.Equal(expectedBlock, block))
			}
		}

		assertBlocks()

		// close and reopen store
		env.reopenStore(t)
		assertBlocks()
		env.s.Close()
	})

	t.Run("expected block is not received during commit", func(t *testing.T) {
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
		env := newTestEnv(t)
		defer env.cleanup(true)

		block, err := env.s.Get(10)
		require.EqualError(t, err, "block store is empty")
		require.Nil(t, block)

		b := &types.Block{
			Header: &types.BlockHeader{
				Number: 1,
			},
		}
		require.NoError(t, env.s.Commit(b))

		block, err = env.s.Get(10)
		require.EqualError(t, err, "requested block number [10] cannot be greater than the last committed block number [1]")
		require.Nil(t, block)
	})
}
