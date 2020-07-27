package blockprocessor

import (
	"io/ioutil"
	"os"
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
	cleanup func(t *testing.T)
}

func newTestEnv(t *testing.T) *testEnv {
	path, err := ioutil.TempDir("/tmp", "validatorAndCommitter")
	require.NoError(t, err)

	cleanup := func(t *testing.T) {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove path %s, %v", path, err)
		}
	}

	db, err := leveldb.NewLevelDB(path)
	if err != nil {
		defer cleanup(t)
		t.Fatalf("failed to create a leveldb instance, %v", err)
	}

	v := NewValidatorAndCommitter(queue.New(10), db)
	go v.Run()

	return &testEnv{
		v:       v,
		db:      db,
		path:    path,
		cleanup: cleanup,
	}
}

func TestValidatorAndCommitter(t *testing.T) {
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
		env := newTestEnv(t)
		defer env.cleanup(t)

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
		env := newTestEnv(t)
		defer env.cleanup(t)

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
