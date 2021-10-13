// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mtree

import (
	"encoding/json"
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func Test_calculateBlockTxHashes(t *testing.T) {
	tests := []struct {
		name    string
		block   *types.Block
		wantErr bool
	}{
		{
			name:    "data block",
			block:   generateDataBlock(t, 10),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := calculateBlockTxHashes(tt.block)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				hashes := make([][]byte, 0)
				for i, tx := range tt.block.GetDataTxEnvelopes().GetEnvelopes() {
					h, err := calculateTxHash(tx, tt.block.GetHeader().GetValidationInfo()[i])
					require.NoError(t, err)
					hashes = append(hashes, h)
				}
				require.EqualValues(t, hashes, got)
			}
		})
	}
}

func Test_calculateTxHash(t *testing.T) {
	tests := []struct {
		name    string
		tx      proto.Message
		valInfo proto.Message
		wantErr bool
	}{
		{
			name: "Data tx",
			tx: &types.DataTxEnvelope{
				Payload: &types.DataTx{
					MustSignUserIds: []string{"testUser"},
					TxId:            "DataTx1",
					DbOperations: []*types.DBOperation{
						{
							DbName:      "testDB",
							DataReads:   nil,
							DataWrites:  nil,
							DataDeletes: nil,
						},
					},
				},
				Signatures: map[string][]byte{
					"testUser": []byte("signature"),
				},
			},
			valInfo: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
			wantErr: false,
		},
		{
			name: "Data tx, no validation info",
			tx: &types.DataTxEnvelope{
				Payload: &types.DataTx{
					MustSignUserIds: []string{"testUser"},
					TxId:            "DataTx1",
					DbOperations: []*types.DBOperation{
						{

							DbName:      "testDB",
							DataReads:   nil,
							DataWrites:  nil,
							DataDeletes: nil,
						},
					},
				},
				Signatures: map[string][]byte{
					"testUser": []byte("signature"),
				},
			},
			valInfo: nil,
			wantErr: false,
		},
		{
			name:    "Nil tx, no validation info",
			tx:      nil,
			valInfo: nil,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := calculateTxHash(tt.tx, tt.valInfo)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				payloadBytes, err := json.Marshal(tt.tx)
				require.NoError(t, err)
				valBytes, err := json.Marshal(tt.valInfo)
				require.NoError(t, err)
				h, err := crypto.ComputeSHA256Hash(append(payloadBytes, valBytes...))
				require.NoError(t, err)
				require.Equal(t, h, got)
			}
		})
	}
}
