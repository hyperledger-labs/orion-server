package blockcreator

import (
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

func TestBatchCreator(t *testing.T) {
	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	b := New(&Config{
		TxBatchQueue:    queue.New(10),
		BlockQueue:      queue.New(10),
		NextBlockNumber: 1,
		Logger:          logger,
	})
	go b.Run()

	dataTx1 := &types.DataTxEnvelope{
		Payload: &types.DataTx{
			UserID: "user1",
			DBName: "db1",
			DataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
		},
	}

	dataTx2 := &types.DataTxEnvelope{
		Payload: &types.DataTx{
			UserID: "user2",
			DBName: "db2",
			DataDeletes: []*types.DataDelete{
				{
					Key: "key2",
				},
			},
		},
	}

	userAdminTx := &types.UserAdministrationTxEnvelope{
		Payload: &types.UserAdministrationTx{
			UserID: "user1",
			UserReads: []*types.UserRead{
				{
					UserID: "user1",
				},
			},
			UserWrites: []*types.UserWrite{
				{
					User: &types.User{
						ID:          "user2",
						Certificate: []byte("certificate"),
					},
				},
			},
		},
	}

	dbAdminTx := &types.DBAdministrationTxEnvelope{
		Payload: &types.DBAdministrationTx{
			UserID:    "user1",
			CreateDBs: []string{"db1", "db2"},
			DeleteDBs: []string{"db3", "db4"},
		},
	}

	configTx := &types.ConfigTxEnvelope{
		Payload: &types.ConfigTx{
			UserID: "user1",
			NewConfig: &types.ClusterConfig{
				Nodes: []*types.NodeConfig{
					{
						ID: "node1",
					},
				},
				Admins: []*types.Admin{
					{
						ID: "admin1",
					},
				},
				RootCACertificate: []byte("root-ca"),
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
				&types.Block_DBAdministrationTxEnvelope{
					DBAdministrationTxEnvelope: dbAdminTx,
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
						Number: 1,
					},
					Payload: &types.Block_UserAdministrationTxEnvelope{
						UserAdministrationTxEnvelope: userAdminTx,
					},
				},
				{
					Header: &types.BlockHeader{
						Number: 2,
					},
					Payload: &types.Block_DBAdministrationTxEnvelope{
						DBAdministrationTxEnvelope: dbAdminTx,
					},
				},
				{
					Header: &types.BlockHeader{
						Number: 3,
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
						Number: 4,
					},
					Payload: &types.Block_ConfigTxEnvelope{
						ConfigTxEnvelope: configTx,
					},
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, txBatch := range tt.txBatches {
				b.txBatchQueue.Enqueue(txBatch)
			}

			hasBlockCountMatched := func() bool {
				if len(tt.expectedBlocks) == b.blockQueue.Size() {
					return true
				}
				return false
			}
			require.Eventually(t, hasBlockCountMatched, 2*time.Second, 100*time.Millisecond)

			for _, expectedBlock := range tt.expectedBlocks {
				block := b.blockQueue.Dequeue().(*types.Block)
				require.True(t, proto.Equal(expectedBlock, block))
			}
		})
	}
}
