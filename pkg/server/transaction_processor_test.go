package server

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
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

	db, err := leveldb.NewLevelDB(dbPath)
	if err != nil {
		cleanup()
		t.Fatalf("Failed to create new leveldb instance: %v", err)
	}

	txProcessor := newTransactionProcessor(db)

	return &txProcessorTestEnv{
		dbPath:      dbPath,
		db:          db,
		txProcessor: txProcessor,
		cleanup:     cleanup,
	}
}

func TestTransactionProcessor(t *testing.T) {
	t.Run("commit a simple transaction", func(t *testing.T) {
		env := newTxProcessorTestEnv(t)
		defer env.cleanup()

		tx := &types.TransactionEnvelope{
			Payload: &types.Transaction{
				DBName:    "test",
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

		require.NoError(t, env.txProcessor.SubmitTransaction(context.TODO(), tx))

		assertTestKey1InDB := func() bool {
			val, err := env.db.Get("test", "test-key1")
			if err != nil {
				return false
			}
			return bytes.Equal([]byte("test-value1"), val.Value)
		}
		require.Eventually(
			t,
			assertTestKey1InDB,
			2*time.Second,
			100*time.Millisecond,
		)
	})
}
