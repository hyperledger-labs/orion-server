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
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type txProcessorTestEnv struct {
	dbPath      string
	db          *leveldb.LevelDB
	txProcessor *transactionProcessor
	cleanup     func()
}

func newTxProcessorTestEnv(t *testing.T) *txProcessorTestEnv {
	dbPath, err := ioutil.TempDir("/tmp", "transaction-processor")
	if err != nil {
		t.Fatalf("Failed to create a temp directory: %v", err)
	}

	cleanup := func() {
		if err := os.RemoveAll(dbPath); err != nil {
			t.Fatalf("Failed to remove dbPath [%s]: %v", dbPath, err)
		}
	}

	db, err := leveldb.New(dbPath)
	if err != nil {
		cleanup()
		t.Fatalf("Failed to create new leveldb instance: %v", err)
	}

	txProcConf := &txProcessorConfig{
		db:                 db,
		txQueueLength:      100,
		txBatchQueueLength: 100,
		blockQueueLength:   100,
		MaxTxCountPerBatch: 1,
		batchTimeout:       50 * time.Millisecond,
	}
	txProcessor := newTransactionProcessor(txProcConf)

	return &txProcessorTestEnv{
		dbPath:      dbPath,
		db:          db,
		txProcessor: txProcessor,
		cleanup:     cleanup,
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

		require.NoError(t, env.txProcessor.SubmitTransaction(context.Background(), tx))

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
	})
}
