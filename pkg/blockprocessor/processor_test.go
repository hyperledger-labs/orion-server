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
	"github.ibm.com/blockchaindb/server/pkg/queue"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type testEnv struct {
	v       *ValidatorAndCommitter
	db      worldstate.DB
	path    string
	cleanup func()
}

func newTestEnv(t *testing.T) *testEnv {
	dir, err := ioutil.TempDir("/tmp", "validatorAndCommitter")
	require.NoError(t, err)

	path := filepath.Join(dir, "leveldb")
	db, err := leveldb.New(path)
	if err != nil {
		if err := os.RemoveAll(dir); err != nil {
			t.Logf("failed to remove directory %s, %v", dir, err)
		}
		t.Fatalf("failed to create a leveldb instance, %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close the db instance, %v", err)
		}
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove directory %s, %v", dir, err)
		}
	}

	v := NewValidatorAndCommitter(queue.New(10), db)
	go v.Run()

	return &testEnv{
		v:       v,
		db:      db,
		path:    dir,
		cleanup: cleanup,
	}
}

func TestValidatorAndCommitter(t *testing.T) {
	t.Parallel()

	block1 := &types.Block{
		Header: &types.BlockHeader{
			Number: 1,
		},
		TransactionEnvelopes: []*types.TransactionEnvelope{
			{
				Payload: &types.Transaction{
					DBName: worldstate.UsersDBName,
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
					DBName: worldstate.UsersDBName,
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

		testCases := []struct {
			block             *types.Block
			expectedKey1Value *types.Value
		}{
			{
				block: block1,
				expectedKey1Value: &types.Value{
					Value: []byte("value-1"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
				},
			},
			{
				block: block2,
				expectedKey1Value: &types.Value{
					Value: []byte("value-2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    0,
						},
					},
				},
			},
		}

		for _, testCase := range testCases {
			env.v.blockQueue.Enqueue(testCase.block)
			require.Eventually(t, env.v.blockQueue.IsEmpty, 2*time.Second, 100*time.Millisecond)

			val, err := env.db.Get(worldstate.UsersDBName, "key1")
			require.NoError(t, err)
			require.True(t, proto.Equal(testCase.expectedKey1Value, val))
		}
	})

	t.Run("enqueue-more-than-one-block", func(t *testing.T) {
		t.Parallel()
		env := newTestEnv(t)
		defer env.cleanup()

		testCases := []struct {
			blocks            []*types.Block
			expectedKey1Value *types.Value
		}{
			{
				blocks: []*types.Block{
					block1,
					block2,
				},
				expectedKey1Value: &types.Value{
					Value: []byte("value-2"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    0,
						},
					},
				},
			},
		}

		for _, testCase := range testCases {
			for _, block := range testCase.blocks {
				env.v.blockQueue.Enqueue(block)
			}
			require.Eventually(t, env.v.blockQueue.IsEmpty, 2*time.Second, 100*time.Millisecond)

			val, err := env.db.Get(worldstate.UsersDBName, "key1")
			require.NoError(t, err)
			require.True(t, proto.Equal(testCase.expectedKey1Value, val))
		}
	})
}
