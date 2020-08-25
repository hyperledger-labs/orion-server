package server

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type txProcessorTestEnv struct {
	dbPath         string
	db             *leveldb.LevelDB
	blockStore     *blockstore.Store
	blockStorePath string
	txProcessor    *transactionProcessor
	cleanup        func()
}

func newTxProcessorTestEnv(t *testing.T) *txProcessorTestEnv {
	dir, err := ioutil.TempDir("/tmp", "transactionProcessor")
	require.NoError(t, err)

	dbPath := constructWorldStatePath(dir)
	db, err := leveldb.Open(dbPath)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, rmErr)
		}
		t.Fatalf("error while creating leveldb, %v", err)
	}

	blockStorePath := constructBlockStorePath(dir)
	blockStore, err := blockstore.Open(blockStorePath)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, rmErr)
		}
		t.Fatalf("error while creating blockstore, %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("error while closing the db instance, %v", err)
		}

		if err := blockStore.Close(); err != nil {
			t.Errorf("error while closing blockstore, %v", err)
		}

		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("error while removing directory %s, %v", dir, err)
		}
	}

	txProcConf := &txProcessorConfig{
		db:                 db,
		blockStore:         blockStore,
		blockHeight:        0,
		txQueueLength:      100,
		txBatchQueueLength: 100,
		blockQueueLength:   100,
		maxTxCountPerBatch: 1,
		batchTimeout:       50 * time.Millisecond,
	}
	txProcessor := newTransactionProcessor(txProcConf)

	return &txProcessorTestEnv{
		dbPath:         dbPath,
		db:             db,
		blockStorePath: blockStorePath,
		blockStore:     blockStore,
		txProcessor:    txProcessor,
		cleanup:        cleanup,
	}
}

func TestTransactionProcessor(t *testing.T) {
	t.Parallel()

	t.Run("commit a simple transaction", func(t *testing.T) {
		t.Parallel()
		env := newTxProcessorTestEnv(t)
		defer env.cleanup()

		tx := &types.TransactionEnvelope{
			Payload: &types.Transaction{
				DBName:    worldstate.DefaultDBName,
				TxID:      []byte("tx1"),
				DataModel: types.Transaction_KV,
				Reads:     []*types.KVRead{},
				Writes: []*types.KVWrite{
					{
						Key:   "test-key1",
						Value: []byte("test-value1"),
					},
				},
			},
		}

		require.NoError(t, env.txProcessor.submitTransaction(context.Background(), tx))

		assertTestKey1InDB := func() bool {
			val, metadata, err := env.db.Get(worldstate.DefaultDBName, "test-key1")
			if err != nil {
				return false
			}
			return bytes.Equal([]byte("test-value1"), val) &&
				proto.Equal(
					&types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    0,
						},
					},
					metadata,
				)
		}
		require.Eventually(
			t,
			assertTestKey1InDB,
			2*time.Second,
			100*time.Millisecond,
		)

		height, err := env.blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(1), height)

		expectedBlock := &types.Block{
			Header: &types.BlockHeader{
				Number:                  1,
				PreviousBlockHeaderHash: nil,
				TransactionsHash:        nil,
			},
			TransactionEnvelopes: []*types.TransactionEnvelope{
				tx,
			},
		}

		block, err := env.blockStore.Get(1)
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedBlock, block))
	})
}
