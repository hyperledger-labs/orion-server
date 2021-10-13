// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mtree

import "github.com/hyperledger-labs/orion-server/pkg/crypto"

// Node struct keep data for Merkle tree node. For now, it is binary Merkle tree
type Node struct {
	sibling   *Node
	left      *Node
	right     *Node
	hash      []byte
	leafRange *leafRange
}

type leafRange struct {
	startIndex int
	endIndex   int
}

// Left returns left child node for node, if exists
func (n *Node) Left() *Node {
	if n == nil {
		return nil
	}
	return n.left
}

// Right returns right child node for node, if exists
func (n *Node) Right() *Node {
	if n == nil {
		return nil
	}
	return n.right
}

// Hash returns node hash, for leaf node it is hash of associated data object, for not leaf - it calculated for children hashes
func (n *Node) Hash() []byte {
	if n == nil {
		return nil
	}
	return n.hash
}

func (n *Node) calcHash() {
	n.hash, _ = crypto.ConcatenateHashes(n.Left().Hash(), n.Right().Hash())
}

// Sibling returns node sibling - node that has same parent as caller, if exists.
func (n *Node) Sibling() *Node {
	if n == nil {
		return nil
	}
	return n.sibling
}
