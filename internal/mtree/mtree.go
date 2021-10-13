// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mtree

import (
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

// BuildTreeForBlockTx builds Merkle tree from block transactions.
func BuildTreeForBlockTx(block *types.Block) (*Node, error) {
	h, err := calculateBlockTxHashes(block)
	if err != nil {
		return nil, err
	}
	return prepareAndBuildTree(h), nil
}

func prepareAndBuildTree(hashes [][]byte) (root *Node) {
	srcNodes := make([]*Node, 0)

	for i, h := range hashes {
		n := &Node{
			hash: h,
			leafRange: &leafRange{
				startIndex: i,
				endIndex:   i,
			},
		}
		srcNodes = append(srcNodes, n)
	}
	root = buildNextTreeLevel(srcNodes)
	return root
}

func buildNextTreeLevel(srcNodes []*Node) *Node {
	dstNodes := make([]*Node, 0)
	for i := 0; i < len(srcNodes); i += 2 {
		n := &Node{
			leafRange: &leafRange{},
		}
		n.left = srcNodes[i]
		n.leafRange.startIndex = srcNodes[i].leafRange.startIndex
		if i+1 < len(srcNodes) {
			n.right = srcNodes[i+1]
			n.leafRange.endIndex = srcNodes[i+1].leafRange.endIndex
			srcNodes[i].sibling = srcNodes[i+1]
			srcNodes[i+1].sibling = srcNodes[i]
		} else {
			n.leafRange.endIndex = srcNodes[i].leafRange.endIndex
		}
		n.calcHash()
		dstNodes = append(dstNodes, n)
	}
	if len(dstNodes) == 1 {
		return dstNodes[0]
	}
	if len(dstNodes) == 0 {
		return nil
	}
	return buildNextTreeLevel(dstNodes)
}
