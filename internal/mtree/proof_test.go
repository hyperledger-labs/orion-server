// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mtree

import (
	"encoding/json"
	"testing"

	interrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestNodeProof(t *testing.T) {
	tests := []struct {
		name        string
		block       *types.Block
		idx         int
		pathLen     int
		root        []byte
		expectedErr string
	}{
		{
			name:    "Data block full tree",
			block:   generateDataBlock(t, 32),
			idx:     0,
			pathLen: 6,
		},
		{
			name:    "Data block half tree plus one",
			block:   generateDataBlock(t, 33),
			idx:     32,
			pathLen: 2,
		},
		{
			name:    "Data block half tree plus two right",
			block:   generateDataBlock(t, 34),
			idx:     33,
			pathLen: 3,
		},
		{
			name:    "Data block half tree plus two left",
			block:   generateDataBlock(t, 34),
			idx:     32,
			pathLen: 3,
		},
		{
			name:    "Data block half tree plus two left",
			block:   generateDataBlock(t, 34),
			idx:     31,
			pathLen: 7,
		},
		{
			name:        "Data block half tree plus two index out of bounds",
			block:       generateDataBlock(t, 34),
			idx:         34,
			pathLen:     0,
			expectedErr: "node with index 34 is not part of merkle tree (0, 33)",
		},
		{
			name:    "Config block, no intermediate hashes",
			block:   generateConfigBlock(t),
			idx:     0,
			pathLen: 1,
		},
	}
	for i := 0; i < len(tests); i++ {
		root, err := BuildTreeForBlockTx(tests[i].block)
		require.NoError(t, err)
		tests[i].root = root.Hash()
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root, err := BuildTreeForBlockTx(tt.block)
			require.NoError(t, err)
			txs := getTxs(tt.block)
			intermediateHashes, err := root.Proof(tt.idx)
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				require.IsType(t, &interrors.NotFoundErr{}, err)
			} else {
				require.NoError(t, err)
				require.Len(t, intermediateHashes, tt.pathLen)
				if tt.idx < len(txs) {

					tx := txs[tt.idx]
					txBytes, err := json.Marshal(tx)
					require.NoError(t, err)
					vi := tt.block.Header.ValidationInfo[tt.idx]
					viBytes, err := json.Marshal(vi)
					require.NoError(t, err)
					txHash, err := crypto.ComputeSHA256Hash(append(txBytes, viBytes...))
					require.NoError(t, err)

					var rootHash []byte
					for i, h := range intermediateHashes {
						if i == 0 {
							require.Equal(t, txHash, h)
							rootHash = h
						} else {
							rootHash, err = crypto.ConcatenateHashes(rootHash, h)
							require.NoError(t, err)
						}
					}

					require.Equal(t, tt.root, rootHash)
				} else {
					require.Fail(t, "Transaction index bigger that amount of tx in block")
				}
			}
		})
	}
}
