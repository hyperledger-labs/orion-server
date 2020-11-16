package txreorderer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/common/logger"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

func TestTxReorderer(t *testing.T) {
	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	b := New(&Config{
		TxQueue:            queue.New(10),
		TxBatchQueue:       queue.New(10),
		MaxTxCountPerBatch: 1,
		BatchTimeout:       50 * time.Millisecond,
		Logger:             logger,
	})
	go b.Run()

	dataTx1 := &types.DataTxEnvelope{
		Payload: &types.DataTx{
			UserID: "user1",
			DBName: "db1",
			DataReads: []*types.DataRead{
				{
					Key: "key1",
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    1,
					},
				},
			},
			DataWrites: []*types.DataWrite{
				{
					Key:   "key2",
					Value: []byte("value2"),
				},
			},
		},
	}

	dataTx2 := &types.DataTxEnvelope{
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

	dataTx3 := &types.DataTxEnvelope{
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

	dataTx4 := &types.DataTxEnvelope{
		Payload: &types.DataTx{
			UserID: "user2",
			DBName: "db2",
			DataDeletes: []*types.DataDelete{
				{
					Key: "key3",
				},
			},
		},
	}

	dataTx5 := &types.DataTxEnvelope{
		Payload: &types.DataTx{
			UserID: "user2",
			DBName: "db2",
			DataDeletes: []*types.DataDelete{
				{
					Key: "key4",
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

	tests := []struct {
		name               string
		maxTxCountPerBatch uint32
		txs                []interface{}
		expectedTxBatches  []interface{}
	}{
		{
			name:               "mix of data and admin transactions",
			maxTxCountPerBatch: 2,
			txs: []interface{}{
				dataTx1,
				userAdminTx,
				dataTx2,
				dbAdminTx,
				dataTx3,
				dataTx4,
				dataTx5,
				configTx,
			},
			expectedTxBatches: []interface{}{
				&types.Block_DataTxEnvelopes{
					DataTxEnvelopes: &types.DataTxEnvelopes{
						Envelopes: []*types.DataTxEnvelope{
							dataTx1,
						},
					},
				},
				&types.Block_UserAdministrationTxEnvelope{
					UserAdministrationTxEnvelope: userAdminTx,
				},
				&types.Block_DataTxEnvelopes{
					DataTxEnvelopes: &types.DataTxEnvelopes{
						Envelopes: []*types.DataTxEnvelope{
							dataTx2,
						},
					},
				},
				&types.Block_DBAdministrationTxEnvelope{
					DBAdministrationTxEnvelope: dbAdminTx,
				},
				&types.Block_DataTxEnvelopes{
					DataTxEnvelopes: &types.DataTxEnvelopes{
						Envelopes: []*types.DataTxEnvelope{
							dataTx3,
							dataTx4,
						},
					},
				},
				&types.Block_DataTxEnvelopes{
					DataTxEnvelopes: &types.DataTxEnvelopes{
						Envelopes: []*types.DataTxEnvelope{
							dataTx5,
						},
					},
				},
				&types.Block_ConfigTxEnvelope{
					ConfigTxEnvelope: configTx,
				},
			},
		},
	}

	for _, tt := range tests {
		b.maxTxCountPerBatch = tt.maxTxCountPerBatch
		for _, tx := range tt.txs {
			b.txQueue.Enqueue(tx)
		}

		hasBatchSizeMatched := func() bool {
			if len(tt.expectedTxBatches) == b.txBatchQueue.Size() {
				return true
			}
			return false
		}
		require.Eventually(t, hasBatchSizeMatched, 2*time.Second, 100*time.Millisecond)

		for _, expectedTxBatch := range tt.expectedTxBatches {
			txBatch := b.txBatchQueue.Dequeue()
			// switch txBatch.(type) {
			// case *types.Block_DataTxEnvelopes:
			require.Equal(t, expectedTxBatch, txBatch)
			// case *types.Block_UserAdministrationTxEnvelope:
			// case *types.Block_DBAdministrationTxEnvelope:
			// case *types.Block_ConfigTxEnvelope:
			// }
			// require.Equal(t, expectedTxBatch, txBatch)
		}
	}
}
