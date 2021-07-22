// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockcreator_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/internal/blockcreator"
	"github.com/IBM-Blockchain/bcdb-server/internal/blockcreator/mocks"
	"github.com/IBM-Blockchain/bcdb-server/internal/blockstore"
	"github.com/IBM-Blockchain/bcdb-server/internal/queue"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate/leveldb"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

type testEnv struct {
	creator        *blockcreator.BlockCreator
	txBatchQueue   *queue.Queue
	blockQueue     *queue.Queue
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

	dir, err := ioutil.TempDir("/tmp", "creator")
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

	txBatchQ := queue.New(10) // Input: transactions
	b, err := blockcreator.New(&blockcreator.Config{
		TxBatchQueue: txBatchQ,
		Logger:       logger,
		BlockStore:   blockStore,
	})
	require.NoError(t, err)

	blockQueue := queue.New(10) // Output: accumulates the blocks that are submitted to the Replicator.
	mockReplicator := &mocks.Replicator{}
	mockReplicator.SubmitCalls(
		func(block *types.Block) error {
			blockQueue.Enqueue(block)
			return nil
		},
	)
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

		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove directory %s, %v", dir, err)
		}
	}

	return &testEnv{
		creator:        b,
		txBatchQueue:   txBatchQ,   // Input
		blockQueue:     blockQueue, // Output
		db:             db,
		dbPath:         dir,
		blockStore:     blockStore,
		blockStorePath: blockStorePath,
		cleanup:        cleanup,
	}
}

func TestBatchCreator(t *testing.T) {
	dataTx1 := &types.DataTxEnvelope{
		Payload: &types.DataTx{
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

	dataTx2 := &types.DataTxEnvelope{
		Payload: &types.DataTx{
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

	userAdminTx := &types.UserAdministrationTxEnvelope{
		Payload: &types.UserAdministrationTx{
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

	dbAdminTx := &types.DBAdministrationTxEnvelope{
		Payload: &types.DBAdministrationTx{
			UserId:    "user1",
			CreateDbs: []string{"db1", "db2"},
			DeleteDbs: []string{"db3", "db4"},
		},
	}

	configTx := &types.ConfigTxEnvelope{
		Payload: &types.ConfigTx{
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

	testCases := []struct {
		name           string
		txBatches      []interface{}
		expectedBlocks []*types.Block
	}{
		{
			name: "enqueue all types of transactions",
			txBatches: []interface{}{
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
			},
			expectedBlocks: []*types.Block{
				{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number:                1,
							LastCommittedBlockNum: 0,
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
			},
		},
	}
	for _, tt := range testCases {
		// updating test case blocks PrevCommitted block. We make it block 0, to keep thing simple
		genesisBlockHash, err := blockstore.ComputeBlockHash(tt.expectedBlocks[0])
		require.NoError(t, err)
		for _, expectedBlock := range tt.expectedBlocks[1:] {
			expectedBlock.Header.BaseHeader.LastCommittedBlockNum = 1
			expectedBlock.Header.BaseHeader.LastCommittedBlockHash = genesisBlockHash
		}
		// Update test case blocks prev hashes
		for index, expectedBlock := range tt.expectedBlocks {
			var prevBlockHash []byte
			prevBlockHash = nil
			if index > 0 {
				var err error
				prevBlockHash, err = blockstore.ComputeBlockBaseHash(tt.expectedBlocks[index-1])
				require.NoError(t, err)
			}
			expectedBlock.Header.BaseHeader.PreviousBaseHeaderHash = prevBlockHash
		}
	}

	enqueueTxBatchesAndAssertBlocks := func(t *testing.T, testEnv *testEnv, txBatches []interface{}, expectedBlocks []*types.Block) {
		for _, txBatch := range txBatches {
			testEnv.txBatchQueue.Enqueue(txBatch)
		}

		hasBlockCountMatched := func() bool {
			return len(expectedBlocks) == testEnv.blockQueue.Size()
		}
		require.Eventually(t, hasBlockCountMatched, 2*time.Second, 1000*time.Millisecond)

		for _, expectedBlock := range expectedBlocks {
			block := testEnv.blockQueue.Dequeue().(*types.Block)
			block.Header.ValidationInfo = expectedBlock.Header.ValidationInfo
			require.True(t, proto.Equal(expectedBlock, block), "Expected block  %v, received block %v", expectedBlock, block)
		}
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			testEnv := newTestEnv(t)
			defer testEnv.cleanup()

			// storing only first block in block store, to simulate last committed block
			require.NoError(t, testEnv.blockStore.Commit(tt.expectedBlocks[0]))

			enqueueTxBatchesAndAssertBlocks(t, testEnv, tt.txBatches, tt.expectedBlocks)
		})
	}
}
