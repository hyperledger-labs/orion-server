// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package utils_test

import (
	"fmt"
	"testing"

	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBlockPayloadToTxIDs_Config(t *testing.T) {
	userAdmin := &types.Block_UserAdministrationTxEnvelope{
		UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
			Payload: &types.UserAdministrationTx{
				TxId: "txid:1",
			},
		},
	}

	dbAdmin := &types.Block_DbAdministrationTxEnvelope{
		DbAdministrationTxEnvelope: &types.DBAdministrationTxEnvelope{
			Payload: &types.DBAdministrationTx{

				TxId: "txid:2",
			},
		},
	}

	config := &types.Block_ConfigTxEnvelope{
		ConfigTxEnvelope: &types.ConfigTxEnvelope{
			Payload: &types.ConfigTx{
				TxId: "txid:3",
			},
		},
	}

	for i, p := range []interface{}{userAdmin, dbAdmin, config} {
		txIDs, err := utils.BlockPayloadToTxIDs(p)
		require.NoError(t, err)
		require.Len(t, txIDs, 1)
		require.Equal(t, fmt.Sprintf("txid:%d", i+1), txIDs[0])
	}
}

func TestBlockPayloadToTxIDs_Data(t *testing.T) {
	dataEnv := &types.Block_DataTxEnvelopes{DataTxEnvelopes: &types.DataTxEnvelopes{
		Envelopes: []*types.DataTxEnvelope{
			{
				Payload: &types.DataTx{TxId: "txid:1"},
			},
			{
				Payload: &types.DataTx{TxId: "txid:2"},
			},
			{
				Payload: &types.DataTx{TxId: "txid:3"},
			},
		},
	}}

	txIDs, err := utils.BlockPayloadToTxIDs(dataEnv)
	require.NoError(t, err)
	require.Len(t, txIDs, 3)
	for i, id := range txIDs {
		require.Equal(t, fmt.Sprintf("txid:%d", i+1), id)
	}
}

func TestBlockPayloadToTxIDs_Errors(t *testing.T) {
	t.Run("user admin", func(t *testing.T) {
		userAdmin := &types.Block_UserAdministrationTxEnvelope{}

		txIDs, err := utils.BlockPayloadToTxIDs(userAdmin)
		require.EqualError(t, err, "empty payload in: &{UserAdministrationTxEnvelope:<nil>}")
		require.Nil(t, txIDs)

		userAdmin = &types.Block_UserAdministrationTxEnvelope{
			UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{},
		}

		txIDs, err = utils.BlockPayloadToTxIDs(userAdmin)
		require.EqualError(t, err, "empty payload in: &{UserAdministrationTxEnvelope:}")
		require.Nil(t, txIDs)

		userAdmin = &types.Block_UserAdministrationTxEnvelope{
			UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
				Payload: &types.UserAdministrationTx{},
			},
		}

		txIDs, err = utils.BlockPayloadToTxIDs(userAdmin)
		require.EqualError(t, err, "missing TxId in: &{UserAdministrationTxEnvelope:payload:{}}")
		require.Nil(t, txIDs)
	})

	t.Run("db admin", func(t *testing.T) {
		dbAdmin := &types.Block_DbAdministrationTxEnvelope{}

		txIDs, err := utils.BlockPayloadToTxIDs(dbAdmin)
		require.EqualError(t, err, "empty payload in: &{DbAdministrationTxEnvelope:<nil>}")
		require.Nil(t, txIDs)

		dbAdmin = &types.Block_DbAdministrationTxEnvelope{
			DbAdministrationTxEnvelope: &types.DBAdministrationTxEnvelope{},
		}

		txIDs, err = utils.BlockPayloadToTxIDs(dbAdmin)
		require.EqualError(t, err, "empty payload in: &{DbAdministrationTxEnvelope:}")
		require.Nil(t, txIDs)

		dbAdmin = &types.Block_DbAdministrationTxEnvelope{
			DbAdministrationTxEnvelope: &types.DBAdministrationTxEnvelope{
				Payload: &types.DBAdministrationTx{},
			},
		}

		txIDs, err = utils.BlockPayloadToTxIDs(dbAdmin)
		require.EqualError(t, err, "missing TxId in: &{DbAdministrationTxEnvelope:payload:{}}")
		require.Nil(t, txIDs)
	})

	t.Run("config", func(t *testing.T) {
		config := &types.Block_ConfigTxEnvelope{}

		txIDs, err := utils.BlockPayloadToTxIDs(config)
		require.EqualError(t, err, "empty payload in: &{ConfigTxEnvelope:<nil>}")
		require.Nil(t, txIDs)

		config = &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{},
		}

		txIDs, err = utils.BlockPayloadToTxIDs(config)
		require.EqualError(t, err, "empty payload in: &{ConfigTxEnvelope:}")
		require.Nil(t, txIDs)

		config = &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{},
			},
		}

		txIDs, err = utils.BlockPayloadToTxIDs(config)
		require.EqualError(t, err, "missing TxId in: &{ConfigTxEnvelope:payload:{}}")
		require.Nil(t, txIDs)
	})

	t.Run("data", func(t *testing.T) {
		data := &types.Block_DataTxEnvelopes{}

		txIDs, err := utils.BlockPayloadToTxIDs(data)
		require.EqualError(t, err, "empty payload in: &{DataTxEnvelopes:<nil>}")
		require.Nil(t, txIDs)

		data = &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{},
		}

		txIDs, err = utils.BlockPayloadToTxIDs(data)
		require.EqualError(t, err, "empty payload in: &{DataTxEnvelopes:}")
		require.Nil(t, txIDs)

		data = &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{
				Envelopes: []*types.DataTxEnvelope{},
			},
		}
		txIDs, err = utils.BlockPayloadToTxIDs(data)
		require.EqualError(t, err, "empty payload in: &{DataTxEnvelopes:}")
		require.Nil(t, txIDs)

		data = &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{
				Envelopes: []*types.DataTxEnvelope{
					{
						Payload: nil,
					},
				},
			},
		}
		txIDs, err = utils.BlockPayloadToTxIDs(data)
		require.EqualError(t, err, "empty payload in index [0]: &{DataTxEnvelopes:envelopes:{}}")
		require.Nil(t, txIDs)

		data = &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{
				Envelopes: []*types.DataTxEnvelope{
					{
						Payload: &types.DataTx{
							TxId:            "txid:1",
							MustSignUserIds: []string{"alice"},
						},
					},
					{
						Payload: &types.DataTx{
							MustSignUserIds: []string{"bob"},
						},
					},
				},
			},
		}
		txIDs, err = utils.BlockPayloadToTxIDs(data)
		require.EqualError(t, err, "missing TxId in index [1]: payload:{must_sign_user_ids:\"bob\"}")
		require.Nil(t, txIDs)
	})
}

func TestIsConfigBlock(t *testing.T) {
	type testCase struct {
		name     string
		block    *types.Block
		expected bool
	}

	testCases := []*testCase{
		{
			name:     "nil block",
			block:    nil,
			expected: false,
		},
		{
			name:     "empty block",
			block:    &types.Block{},
			expected: false,
		},
		{
			name:     "data block",
			block:    &types.Block{Payload: &types.Block_DataTxEnvelopes{}},
			expected: false,
		},
		{
			name:     "user admin block",
			block:    &types.Block{Payload: &types.Block_UserAdministrationTxEnvelope{}},
			expected: false,
		},
		{
			name:     "db admin block",
			block:    &types.Block{Payload: &types.Block_DbAdministrationTxEnvelope{}},
			expected: false,
		},
		{
			name:     "config block",
			block:    &types.Block{Payload: &types.Block_ConfigTxEnvelope{}},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			isConfig := utils.IsConfigBlock(tc.block)
			require.Equal(t, tc.expected, isConfig)
		})
	}
}
