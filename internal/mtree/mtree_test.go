// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mtree

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestBuildBlockTree(t *testing.T) {
	tests := []struct {
		name       string
		block      *types.Block
		leftIdx    int
		rightIdx   int
		emptyBlock bool
		wantErr    bool
	}{
		{
			name:     "Data block full tree",
			block:    generateDataBlock(t, 64),
			leftIdx:  0,
			rightIdx: 63,
		},
		{
			name:     "Data block not full tree - almost full",
			block:    generateDataBlock(t, 63),
			leftIdx:  0,
			rightIdx: 62,
		},
		{
			name:     "Data block not full tree, right tree almost empty",
			block:    generateDataBlock(t, 35),
			leftIdx:  0,
			rightIdx: 34,
		},
		{
			name:     "Data block not full tree, right tree almost empty",
			block:    generateDataBlock(t, 10),
			leftIdx:  0,
			rightIdx: 9,
		},
		{
			name:     "Config block",
			block:    generateConfigBlock(t),
			leftIdx:  0,
			rightIdx: 0,
		},
		{
			name:     "UserAdmin block",
			block:    generateUserAdminBlock(t),
			leftIdx:  0,
			rightIdx: 0,
		},
		{
			name:     "DBAdmin block",
			block:    generateDBAdminBlock(t),
			leftIdx:  0,
			rightIdx: 0,
		},
		{
			name:       "Data block no tx",
			block:      generateDataBlock(t, 0),
			leftIdx:    0,
			rightIdx:   0,
			emptyBlock: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, err := BuildTreeForBlockTx(tt.block)
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildTreeForBlockTx() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.emptyBlock {
				require.Nil(t, root)
				return
			}
			require.NotNil(t, tt.block.Header.TxMerkleTreeRootHash)
			require.Equal(t, tt.block.Header.TxMerkleTreeRootHash, root.Hash())
			validateTreeRoot(t, root, tt.leftIdx, tt.rightIdx)
		})
	}
}

func getTxs(block *types.Block) []proto.Message {
	switch block.Payload.(type) {
	case *types.Block_DataTxEnvelopes:
		res := make([]proto.Message, 0)
		for _, tx := range block.Payload.(*types.Block_DataTxEnvelopes).DataTxEnvelopes.Envelopes {
			res = append(res, tx)
		}
		return res
	case *types.Block_UserAdministrationTxEnvelope:
		return []proto.Message{block.Payload.(*types.Block_UserAdministrationTxEnvelope).UserAdministrationTxEnvelope}
	case *types.Block_DbAdministrationTxEnvelope:
		return []proto.Message{block.Payload.(*types.Block_DbAdministrationTxEnvelope).DbAdministrationTxEnvelope}
	case *types.Block_ConfigTxEnvelope:
		return []proto.Message{block.Payload.(*types.Block_ConfigTxEnvelope).ConfigTxEnvelope}
	}
	return nil
}

func generateDataBlock(t *testing.T, txNum int) *types.Block {
	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader:           &types.BlockHeaderBase{},
			TxMerkleTreeRootHash: nil,
			ValidationInfo:       []*types.ValidationInfo{},
		},
		Payload: &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{
				Envelopes: []*types.DataTxEnvelope{},
			},
		},
	}

	txEnvelopes := make([]*types.DataTxEnvelope, 0)
	hashes := make([][]byte, 0)

	for i := 0; i < txNum; i++ {
		txPayload := &types.DataTx{
			MustSignUserIds: []string{"testUser"},
			TxId:            fmt.Sprintf("%d", i),
			DbOperations: []*types.DBOperation{
				{
					DbName:      "testDB",
					DataReads:   nil,
					DataWrites:  nil,
					DataDeletes: nil,
				},
			},
		}
		envelope := &types.DataTxEnvelope{
			Payload: txPayload,
			Signatures: map[string][]byte{
				"testUser": []byte("singature"),
			},
		}
		txEnvelopes = append(txEnvelopes, envelope)
		validationInfo := &types.ValidationInfo{
			Flag: types.Flag_VALID,
		}
		block.Header.ValidationInfo = append(block.Header.ValidationInfo, validationInfo)
		txBytes, err := json.Marshal(envelope)
		require.NoError(t, err)
		vBytes, err := json.Marshal(validationInfo)
		require.NoError(t, err)
		hash, err := crypto.ComputeSHA256Hash(append(txBytes, vBytes...))
		require.NoError(t, err)
		hashes = append(hashes, hash)
	}
	block.Header.TxMerkleTreeRootHash = prepareAndBuildTree(hashes).Hash()
	block.Payload.(*types.Block_DataTxEnvelopes).DataTxEnvelopes.Envelopes = txEnvelopes
	return block
}

func generateConfigBlock(t *testing.T) *types.Block {
	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{},
		},
		Payload: &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					UserId: "adminUsr",
					TxId:   "1",
					ReadOldConfigVersion: &types.Version{
						BlockNum: 1,
						TxNum:    2,
					},
					NewConfig: &types.ClusterConfig{
						Nodes: nil,
						Admins: []*types.Admin{
							{
								Id:          "admin1",
								Certificate: []byte("cert1"),
							},
						},
						CertAuthConfig: &types.CAConfig{
							Roots: [][]byte{[]byte("cert")},
						},
					},
				},
				Signature: []byte("signature"),
			},
		},
	}

	validationInfo := &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
	block.Header.ValidationInfo = append(block.Header.ValidationInfo, validationInfo)
	txBytes, err := json.Marshal(block.Payload.(*types.Block_ConfigTxEnvelope).ConfigTxEnvelope)
	require.NoError(t, err)
	vBytes, err := json.Marshal(validationInfo)
	require.NoError(t, err)
	hash, err := crypto.ComputeSHA256Hash(append(txBytes, vBytes...))
	require.NoError(t, err)

	block.Header.TxMerkleTreeRootHash = hash
	return block
}

func generateUserAdminBlock(t *testing.T) *types.Block {
	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{},
		},
		Payload: &types.Block_UserAdministrationTxEnvelope{
			UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
				Payload: &types.UserAdministrationTx{
					UserId:      "dbAdmin",
					TxId:        "100",
					UserReads:   nil,
					UserWrites:  nil,
					UserDeletes: nil,
				},
				Signature: []byte("signature"),
			},
		},
	}

	validationInfo := &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
	block.Header.ValidationInfo = append(block.Header.ValidationInfo, validationInfo)
	txBytes, err := json.Marshal(block.Payload.(*types.Block_UserAdministrationTxEnvelope).UserAdministrationTxEnvelope)
	require.NoError(t, err)
	vBytes, err := json.Marshal(validationInfo)
	require.NoError(t, err)
	hash, err := crypto.ComputeSHA256Hash(append(txBytes, vBytes...))
	require.NoError(t, err)

	block.Header.TxMerkleTreeRootHash = hash
	return block
}

func generateDBAdminBlock(t *testing.T) *types.Block {
	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{},
		},
		Payload: &types.Block_DbAdministrationTxEnvelope{
			DbAdministrationTxEnvelope: &types.DBAdministrationTxEnvelope{
				Payload: &types.DBAdministrationTx{
					UserId:    "DBManager",
					TxId:      "1000",
					CreateDbs: nil,
					DeleteDbs: nil,
				},
				Signature: []byte("signature"),
			},
		},
	}

	validationInfo := &types.ValidationInfo{
		Flag: types.Flag_VALID,
	}
	block.Header.ValidationInfo = append(block.Header.ValidationInfo, validationInfo)
	txBytes, err := json.Marshal(block.Payload.(*types.Block_DbAdministrationTxEnvelope).DbAdministrationTxEnvelope)
	require.NoError(t, err)
	vBytes, err := json.Marshal(validationInfo)
	require.NoError(t, err)
	hash, err := crypto.ComputeSHA256Hash(append(txBytes, vBytes...))
	require.NoError(t, err)
	hashes := [][]byte{hash}

	block.Header.TxMerkleTreeRootHash = prepareAndBuildTree(hashes).hash
	return block
}

func validateTreeRoot(t *testing.T, root *Node, startIdx, endIdx int) {
	if root.left == nil && root.right == nil {
		require.Equal(t, root.leafRange.startIndex, root.leafRange.endIndex)
		return
	}

	closestTwoPower := closestTwoPowerLeft(root.leafRange.endIndex - root.leafRange.startIndex)
	if root.left != nil {
		leftTreeStartIdx := root.leafRange.startIndex
		leftTreeEndIdx := root.leafRange.startIndex + closestTwoPower - 1
		if root.right == nil {
			leftTreeEndIdx = endIdx
		}
		validateTreeRoot(t, root.left, leftTreeStartIdx, leftTreeEndIdx)
	}
	if root.right != nil {
		rightTreeStartIdx := root.leafRange.startIndex + closestTwoPower
		rightTreeEndIdx := root.leafRange.endIndex
		validateTreeRoot(t, root.right, rightTreeStartIdx, rightTreeEndIdx)
	}

	h, err := crypto.ConcatenateHashes(root.Left().Hash(), root.Right().Hash())
	require.NoError(t, err)
	require.Equal(t, h, root.Hash())
	require.Equal(t, startIdx, root.leafRange.startIndex)
	require.Equal(t, endIdx, root.leafRange.endIndex)
}

func closestTwoPowerLeft(num int) int {
	res := 1
	for ; num > 1; num /= 2 {
		res *= 2
	}
	return res
}
