// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package txreorderer

import (
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/internal/queue"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func newTxReordererForTest(t *testing.T, maxTxCountPerBatch uint32, blockTimeout time.Duration) *TxReorderer {
	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	r := New(&Config{
		TxQueue:            queue.New(10),
		TxBatchQueue:       queue.New(10),
		MaxTxCountPerBatch: maxTxCountPerBatch,
		BatchTimeout:       blockTimeout,
		Logger:             logger,
		Metrics:            utils.NewTxProcessingMetrics(nil),
	})
	go r.Start()
	r.WaitTillStart()

	return r
}

func TestTxReorderer(t *testing.T) {
	dataTx1 := &types.DataTxEnvelope{
		Payload: &types.DataTx{
			MustSignUserIds: []string{"user1"},
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
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
			},
		},
	}

	dataTx2 := &types.DataTxEnvelope{
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

	dataTx3 := &types.DataTxEnvelope{
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

	dataTx4 := &types.DataTxEnvelope{
		Payload: &types.DataTx{
			MustSignUserIds: []string{"user2"},
			DbOperations: []*types.DBOperation{
				{
					DbName: "db2",
					DataDeletes: []*types.DataDelete{
						{
							Key: "key3",
						},
					},
				},
			},
		},
	}

	dataTx5 := &types.DataTxEnvelope{
		Payload: &types.DataTx{
			MustSignUserIds: []string{"user2"},
			DbOperations: []*types.DBOperation{
				{
					DbName: "db2",
					DataDeletes: []*types.DataDelete{
						{
							Key: "key4",
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

	tests := []struct {
		name               string
		maxTxCountPerBatch uint32
		timeout            time.Duration
		txs                []interface{}
		expectedTxBatches  []interface{}
	}{
		{
			name:               "tx count reached",
			maxTxCountPerBatch: 2,
			timeout:            50 * time.Second,
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
				&types.Block_DbAdministrationTxEnvelope{
					DbAdministrationTxEnvelope: dbAdminTx,
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
		{
			name:               "batch timeout reached",
			maxTxCountPerBatch: 1000,
			timeout:            500 * time.Millisecond,
			txs: []interface{}{
				dataTx1,
				userAdminTx,
				dataTx2,
				dataTx3,
				dataTx4,
				dataTx5,
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
							dataTx3,
							dataTx4,
							dataTx5,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := newTxReordererForTest(t, tt.maxTxCountPerBatch, tt.timeout)
			defer r.Stop()

			r.maxTxCountPerBatch = tt.maxTxCountPerBatch
			for _, tx := range tt.txs {
				r.txQueue.Enqueue(tx)
			}

			hasBatchSizeMatched := func() bool {
				return len(tt.expectedTxBatches) == r.txBatchQueue.Size()
			}
			require.Eventually(t, hasBatchSizeMatched, 2*time.Second, 100*time.Millisecond)

			for _, expectedTxBatch := range tt.expectedTxBatches {
				txBatch := r.txBatchQueue.Dequeue()
				require.Equal(t, expectedTxBatch, txBatch)
			}
		})
	}
}
