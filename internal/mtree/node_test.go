// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mtree

import (
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/stretchr/testify/require"
)

func TestNode_calcHash(t *testing.T) {
	tests := []struct {
		name string
		node *Node
	}{
		{
			name: "no children no hash",
			node: &Node{},
		},
		{
			name: "left child only",
			node: &Node{
				sibling: nil,
				left: &Node{
					hash: []byte("hash"),
				},
			},
		},
		{
			name: "right child only",
			node: &Node{
				sibling: nil,
				right: &Node{
					hash: []byte("hash"),
				},
			},
		},
		{
			name: "both children",
			node: &Node{
				sibling: nil,
				left: &Node{
					hash: []byte("hash1"),
				},
				right: &Node{
					hash: []byte("hash2"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := tt.node
			n.calcHash()
			if n.Left() == nil {
				require.Equal(t, n.Right().Hash(), n.Hash())
			} else if n.Right() == nil {
				require.Equal(t, n.Left().Hash(), n.Hash())
			} else {
				h, err := crypto.ConcatenateHashes(n.Left().Hash(), n.Right().Hash())
				require.NoError(t, err)
				require.Equal(t, h, n.Hash())
			}
		})
	}
}
