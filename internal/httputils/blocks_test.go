// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package httputils_test

import (
	"fmt"
	"testing"

	"github.com/hyperledger-labs/orion-server/internal/httputils"
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
		txIDs, err := httputils.BlockPayloadToTxIDs(p)
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

	txIDs, err := httputils.BlockPayloadToTxIDs(dataEnv)
	require.NoError(t, err)
	require.Len(t, txIDs, 3)
	for i, id := range txIDs {
		require.Equal(t, fmt.Sprintf("txid:%d", i+1), id)
	}
}

func TestBlockPayloadToTxIDs_Errors(t *testing.T) {
	t.Run("user admin", func(t *testing.T) {
		userAdmin := &types.Block_UserAdministrationTxEnvelope{}

		txIDs, err := httputils.BlockPayloadToTxIDs(userAdmin)
		require.EqualError(t, err, "empty payload in: &{UserAdministrationTxEnvelope:<nil>}")
		require.Nil(t, txIDs)

		userAdmin = &types.Block_UserAdministrationTxEnvelope{
			UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{},
		}

		txIDs, err = httputils.BlockPayloadToTxIDs(userAdmin)
		require.EqualError(t, err, "empty payload in: &{UserAdministrationTxEnvelope:}")
		require.Nil(t, txIDs)

		userAdmin = &types.Block_UserAdministrationTxEnvelope{
			UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
				Payload: &types.UserAdministrationTx{},
			},
		}

		txIDs, err = httputils.BlockPayloadToTxIDs(userAdmin)
		require.EqualError(t, err, "missing TxId in: &{UserAdministrationTxEnvelope:payload:<> }")
		require.Nil(t, txIDs)
	})

	t.Run("db admin", func(t *testing.T) {
		dbAdmin := &types.Block_DbAdministrationTxEnvelope{}

		txIDs, err := httputils.BlockPayloadToTxIDs(dbAdmin)
		require.EqualError(t, err, "empty payload in: &{DbAdministrationTxEnvelope:<nil>}")
		require.Nil(t, txIDs)

		dbAdmin = &types.Block_DbAdministrationTxEnvelope{
			DbAdministrationTxEnvelope: &types.DBAdministrationTxEnvelope{},
		}

		txIDs, err = httputils.BlockPayloadToTxIDs(dbAdmin)
		require.EqualError(t, err, "empty payload in: &{DbAdministrationTxEnvelope:}")
		require.Nil(t, txIDs)

		dbAdmin = &types.Block_DbAdministrationTxEnvelope{
			DbAdministrationTxEnvelope: &types.DBAdministrationTxEnvelope{
				Payload: &types.DBAdministrationTx{},
			},
		}

		txIDs, err = httputils.BlockPayloadToTxIDs(dbAdmin)
		require.EqualError(t, err, "missing TxId in: &{DbAdministrationTxEnvelope:payload:<> }")
		require.Nil(t, txIDs)
	})

	t.Run("config", func(t *testing.T) {
		config := &types.Block_ConfigTxEnvelope{}

		txIDs, err := httputils.BlockPayloadToTxIDs(config)
		require.EqualError(t, err, "empty payload in: &{ConfigTxEnvelope:<nil>}")
		require.Nil(t, txIDs)

		config = &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{},
		}

		txIDs, err = httputils.BlockPayloadToTxIDs(config)
		require.EqualError(t, err, "empty payload in: &{ConfigTxEnvelope:}")
		require.Nil(t, txIDs)

		config = &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{},
			},
		}

		txIDs, err = httputils.BlockPayloadToTxIDs(config)
		require.EqualError(t, err, "missing TxId in: &{ConfigTxEnvelope:payload:<> }")
		require.Nil(t, txIDs)
	})

	t.Run("data", func(t *testing.T) {
		data := &types.Block_DataTxEnvelopes{}

		txIDs, err := httputils.BlockPayloadToTxIDs(data)
		require.EqualError(t, err, "empty payload in: &{DataTxEnvelopes:<nil>}")
		require.Nil(t, txIDs)

		data = &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{},
		}

		txIDs, err = httputils.BlockPayloadToTxIDs(data)
		require.EqualError(t, err, "empty payload in: &{DataTxEnvelopes:}")
		require.Nil(t, txIDs)

		data = &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{
				Envelopes: []*types.DataTxEnvelope{},
			},
		}
		txIDs, err = httputils.BlockPayloadToTxIDs(data)
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
		txIDs, err = httputils.BlockPayloadToTxIDs(data)
		require.EqualError(t, err, "empty payload in index [0]: &{DataTxEnvelopes:envelopes:<> }")
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
		txIDs, err = httputils.BlockPayloadToTxIDs(data)
		require.EqualError(t, err, "missing TxId in index [1]: &{DataTxEnvelopes:envelopes:<payload:<must_sign_user_ids:\"alice\" tx_id:\"txid:1\" > > envelopes:<payload:<must_sign_user_ids:\"bob\" > > }")
		require.Nil(t, txIDs)
	})
}
