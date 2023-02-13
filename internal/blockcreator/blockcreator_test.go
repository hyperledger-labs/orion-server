// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockcreator_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/blockcreator"
	"github.com/hyperledger-labs/orion-server/internal/blockcreator/mocks"
	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/queue"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

type testEnv struct {
	creator        *blockcreator.BlockCreator
	txBatchQueue   *queue.Queue
	pendingTxs     *queue.PendingTxs //TODO test the release of txs
	mockReplicator *mocks.Replicator
	blockQueue     *queue.Queue

	cleanup func()
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

	dir := t.TempDir()

	dbPath := filepath.Join(dir, "leveldb")
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    logger,
		},
	)
	if err != nil {
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
		t.Fatalf("error while creating the block store, %v", err)
	}

	txBatchQ := queue.New(10)
	pendingTxs := queue.NewPendingTxs(logger)
	b, err := blockcreator.New(&blockcreator.Config{
		TxBatchQueue: txBatchQ,
		PendingTxs:   pendingTxs,
		Logger:       logger,
		BlockStore:   blockStore,
	})
	require.NoError(t, err)

	blockQueue := queue.New(10) // Output: accumulates the blocks that are submitted to the Replicator.
	mockReplicator := &mocks.Replicator{}
	b.RegisterReplicator(mockReplicator)
	go b.Start()
	b.WaitTillStart()

	cleanup := func() {
		b.Stop()

		if err := db.Close(); err != nil {
			t.Errorf("failed to close the db instance, %v", err)
		}

		if err := blockStore.Close(); err != nil {
			t.Errorf("failed to close the blockstore, %v", err)
		}
	}

	return &testEnv{
		creator:        b,
		txBatchQueue:   txBatchQ,       // Input
		mockReplicator: mockReplicator, // Define behavior
		blockQueue:     blockQueue,     // Output
		pendingTxs:     pendingTxs,     // Output
		cleanup:        cleanup,
	}
}

var genesisBlock = &types.Block{
	Header: &types.BlockHeader{
		BaseHeader: &types.BlockHeaderBase{
			Number: 1,
		},
		ValidationInfo: []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		},
	},
	Payload: &types.Block_ConfigTxEnvelope{
		ConfigTxEnvelope: configTx,
	},
}

var userAdminTx = &types.UserAdministrationTxEnvelope{
	Payload: &types.UserAdministrationTx{
		TxId:   "txid:1",
		UserId: "user1",
		UserReads: []*types.UserRead{
			{
				UserId: "user1",
			},
		},
		UserWrites: []*types.UserWrite{
			{
				User: &types.User{
					Id:          "user2",
					Certificate: []byte("certificate"),
				},
			},
		},
	},
}

var dbAdminTx = &types.DBAdministrationTxEnvelope{
	Payload: &types.DBAdministrationTx{
		TxId:      "txid:2",
		UserId:    "user1",
		CreateDbs: []string{"db1", "db2"},
		DeleteDbs: []string{"db3", "db4"},
	},
}

var dataTx1 = &types.DataTxEnvelope{
	Payload: &types.DataTx{
		TxId:            "txid:3",
		MustSignUserIds: []string{"user1"},
		DbOperations: []*types.DBOperation{
			{
				DbName: "db1",
				DataDeletes: []*types.DataDelete{
					{
						Key: "key1",
					},
				},
			},
		},
	},
}

var dataTx2 = &types.DataTxEnvelope{
	Payload: &types.DataTx{
		TxId:            "txid:4",
		MustSignUserIds: []string{"user2"},
		DbOperations: []*types.DBOperation{
			{
				DbName: "db2",
				DataDeletes: []*types.DataDelete{
					{
						Key: "key2",
					},
				},
			},
		},
	},
}

var configTx = &types.ConfigTxEnvelope{
	Payload: &types.ConfigTx{
		TxId:   "txid:5",
		UserId: "user1",
		NewConfig: &types.ClusterConfig{
			Nodes: []*types.NodeConfig{
				{
					Id: "node1",
				},
			},
			Admins: []*types.Admin{
				{
					Id: "admin1",
				},
			},
			CertAuthConfig: &types.CAConfig{
				Roots: [][]byte{[]byte("root-ca")},
			},
		},
	},
}

var txBatches = []interface{}{
	&types.Block_UserAdministrationTxEnvelope{
		UserAdministrationTxEnvelope: userAdminTx,
	},
	&types.Block_DbAdministrationTxEnvelope{
		DbAdministrationTxEnvelope: dbAdminTx,
	},
	&types.Block_DataTxEnvelopes{
		DataTxEnvelopes: &types.DataTxEnvelopes{
			Envelopes: []*types.DataTxEnvelope{
				dataTx1,
				dataTx2,
			},
		},
	},
	&types.Block_ConfigTxEnvelope{
		ConfigTxEnvelope: configTx,
	},
}

func TestBlockCreator_EnqueueAllTypes(t *testing.T) {
	expectedBlocks := []*types.Block{
		{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 1,
				},
				ValidationInfo: []*types.ValidationInfo{
					{
						Flag: types.Flag_VALID,
					},
				},
			},
			Payload: &types.Block_UserAdministrationTxEnvelope{
				UserAdministrationTxEnvelope: userAdminTx,
			},
		},
		{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 2,
				},
				ValidationInfo: []*types.ValidationInfo{
					{
						Flag: types.Flag_VALID,
					},
				},
			},
			Payload: &types.Block_DbAdministrationTxEnvelope{
				DbAdministrationTxEnvelope: dbAdminTx,
			},
		},
		{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 3,
				},
				ValidationInfo: []*types.ValidationInfo{
					{
						Flag: types.Flag_VALID,
					},
					{
						Flag: types.Flag_VALID,
					},
				},
			},
			Payload: &types.Block_DataTxEnvelopes{
				DataTxEnvelopes: &types.DataTxEnvelopes{
					Envelopes: []*types.DataTxEnvelope{
						dataTx1,
						dataTx2,
					},
				},
			},
		},
		{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number: 4,
				},
				ValidationInfo: []*types.ValidationInfo{
					{
						Flag: types.Flag_VALID,
					},
				},
			},
			Payload: &types.Block_ConfigTxEnvelope{
				ConfigTxEnvelope: configTx,
			},
		},
	}

	testEnv := newTestEnv(t)
	defer testEnv.cleanup()

	testEnv.mockReplicator.SubmitCalls(
		func(block *types.Block) error {
			testEnv.blockQueue.Enqueue(block)
			return nil
		},
	)

	for _, txBatch := range txBatches {
		testEnv.txBatchQueue.Enqueue(txBatch)
	}

	hasBlockCountMatched := func() bool {
		return len(expectedBlocks) == testEnv.blockQueue.Size()
	}
	require.Eventually(t, hasBlockCountMatched, 2*time.Second, 10*time.Millisecond)

	for _, expectedBlock := range expectedBlocks {
		block := testEnv.blockQueue.Dequeue().(*types.Block)
		block.Header.ValidationInfo = expectedBlock.Header.ValidationInfo
		require.True(t, proto.Equal(expectedBlock, block), "Expected block  %v, received block %v", expectedBlock, block)
	}
}

func TestBlockCreator_ReleaseAsync(t *testing.T) {
	testEnv := newTestEnv(t)
	defer testEnv.cleanup()

	testEnv.mockReplicator.SubmitReturns(&ierrors.NotLeaderError{
		LeaderID:       1,
		LeaderHostPort: "10.10.10.10:1111",
	})

	for i := 1; i < 6; i++ {
		require.False(t, testEnv.pendingTxs.Add(fmt.Sprintf("txid:%d", i), nil))
	}

	for _, txBatch := range txBatches {
		testEnv.txBatchQueue.Enqueue(txBatch)
	}

	allReleased := func() bool {
		return testEnv.pendingTxs.Empty()
	}
	require.Eventually(t, allReleased, 2*time.Second, 10*time.Millisecond)
}

func TestBlockCreator_ReleaseSync(t *testing.T) {
	testEnv := newTestEnv(t)
	defer testEnv.cleanup()

	testEnv.mockReplicator.SubmitReturns(&ierrors.NotLeaderError{
		LeaderID:       1,
		LeaderHostPort: "10.10.10.10:1111",
	})

	wg := sync.WaitGroup{}
	wg.Add(5)
	for i := 1; i < 6; i++ {
		promise := queue.NewCompletionPromise(5 * time.Second)
		require.False(t, testEnv.pendingTxs.Add(fmt.Sprintf("txid:%d", i), promise))
		go func() {
			receipt, err := promise.Wait()
			require.Nil(t, receipt)
			require.EqualError(t, err, "not a leader, leader is RaftID: 1, with HostPort: 10.10.10.10:1111")
			wg.Done()
		}()
	}

	for _, txBatch := range txBatches {
		testEnv.txBatchQueue.Enqueue(txBatch)
	}

	allReleased := func() bool {
		return testEnv.pendingTxs.Empty()
	}
	require.Eventually(t, allReleased, 2*time.Second, 10*time.Millisecond)
	wg.Wait()
}
