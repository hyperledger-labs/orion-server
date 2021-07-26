// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mptrie

import (
	"bytes"
	"sync"

	"github.com/IBM-Blockchain/bcdb-server/pkg/crypto"
	"github.com/pkg/errors"
)

// Store stores Trie nodes and values in way Hash(node)->node bytes Hash(value)->value bytes
type Store interface {
	// GetNode returns TrieNode associated with key/ptr. It may be temporal node
	// created by PutNode, node market to persist after PersistNode or after executing
	// CommitPersistChanges actually stored in backend storage node
	GetNode(nodePtr []byte) (TrieNode, error)
	// GetValue return value bytes associated with value ptr. Same logic as in GetNode applies.
	GetValue(valuePtr []byte) ([]byte, error)
	// PutNode store node data it temporal way - it my be accessed by GetNode, but will not stored in backend store.
	PutNode(nodePtr []byte, node TrieNode) error
	// PutValue do the same as PutNode, but for value
	PutValue(valuePtr, value []byte) error
	// PersistNode mark temporal node to be persisted to backend storage in next call to CommitPersistChanges
	PersistNode(nodePtr []byte) (bool, error)
	// PersistValue do same as PersistNode, but for value
	PersistValue(valuePtr []byte) (bool, error)
	// Height returns number of last block trie was persist for
	Height() (uint64, error)
	// CommitChanges frees all inMemory nodes and actually stores nodes and value marked to be persist by
	// PersistNode and PersistValue in single backend store update - usually used with block number
	CommitChanges(blockNum uint64) error
	// RollbackChanges free all in memory nodes and nodes marked to be persist, without storing anything in
	// underlying database. Operation can cause to current MPTrie become invalid, so always reload trie
	// after the call
	RollbackChanges() error
}

// Merkle-Patricia Trie implementation. No node/value data stored inside trie, but in associated TrieStore
type MPTrie struct {
	root  TrieNode
	store Store
	lock  sync.RWMutex
}

// NewTrie creates new Merkle-Patricia Trie, with backend store.
// If root node Hash is not nil, root node loaded from store, otherwise, empty trie is created
func NewTrie(rootHash []byte, store Store) (*MPTrie, error) {
	res := &MPTrie{
		store: store,
	}
	var err error
	if rootHash == nil {
		// To simplify things, root node is always full branch node, even if trie doesn't contains any data. Its valuePrt is always nil.
		res.root = &BranchNode{Children: make([][]byte, 16)}
		_, err = res.saveNode(res.root)
		if err != nil {
			return nil, err
		}
	} else {
		res.root, err = store.GetNode(rootHash)
		if err != nil {
			return nil, err
		}
		if res.root == nil {
			return nil, errors.New("invalid root Hash provided, not found in store")
		}
	}
	return res, nil
}

func (t *MPTrie) Hash() ([]byte, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()
	return t.root.hash()
}

func (t *MPTrie) Get(key []byte) ([]byte, error) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	hexKey := convertByteToHex(key)
	node, err := t.getNode(hexKey)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, nil
	}

	if node.isDeleted() {
		return nil, nil
	}
	valPtr := node.getValuePtr()
	return t.store.GetValue(valPtr)
}

func (t *MPTrie) Update(key, value []byte) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if len(key) == 0 {
		return errors.New("can't update element with empty key")
	}
	valuePtr, err := CalculateKeyValueHash(key, value)
	if err != nil {
		return err
	}
	node := t.root
	hexKey := convertByteToHex(key)
	t.root, _, err = t.updateBranchNode(node.(*BranchNode), hexKey, valuePtr, false)
	if err != nil {
		return err
	}
	return t.store.PutValue(valuePtr, value)
}

func (t *MPTrie) update(node TrieNode, hexKey, valuePtr []byte, isDeleted bool) (TrieNode, []byte, error) {
	switch node := node.(type) {
	case *BranchNode:
		return t.updateBranchNode(node, hexKey, valuePtr, isDeleted)
	case *ExtensionNode:
		return t.updateExtensionNode(node, hexKey, valuePtr, isDeleted)
	case *ValueNode:
		return t.updateValueNode(node, hexKey, valuePtr, isDeleted)
	case *EmptyNode:
		return t.updateEmptyNode(hexKey, valuePtr)
	default:
		return nil, nil, errors.New("unknown trie node type")
	}
}

func (t *MPTrie) updateBranchNode(node *BranchNode, hexKey, valuePtr []byte, isDeleted bool) (TrieNode, []byte, error) {
	var err error
	if len(hexKey) == 0 {
		node.setValuePtr(valuePtr)
		node.Deleted = isDeleted
	} else {
		childPtr := node.Children[hexKey[0]]
		var childNode TrieNode
		childNode = &EmptyNode{}
		if childPtr != nil {
			childNode, err = t.store.GetNode(childPtr)
			if err != nil {
				return nil, nil, err
			}
		}
		_, updateChildNodePtr, err := t.update(childNode, hexKey[1:], valuePtr, isDeleted)
		if err != nil {
			return nil, nil, err
		}
		node.Children[hexKey[0]] = updateChildNodePtr
	}

	nodePtr, err := t.saveNode(node)
	if err != nil {
		return nil, nil, err
	}

	return node, nodePtr, nil
}

func (t *MPTrie) updateEmptyNode(hexKey, valuePtr []byte) (TrieNode, []byte, error) {
	newNode := &ValueNode{
		Key:      hexKey,
		ValuePtr: valuePtr,
	}

	newNodePtr, err := t.saveNode(newNode)
	if err != nil {
		return nil, nil, err
	}

	return newNode, newNodePtr, nil
}

func (t *MPTrie) updateValueNode(node *ValueNode, hexKey, valuePtr []byte, isDeleted bool) (TrieNode, []byte, error) {
	var newNode TrieNode
	commonPrefix := findCommonPrefix(hexKey, node.Key)
	nodeKeyLeft := node.Key[len(commonPrefix):]
	valueKeyLeft := hexKey[len(commonPrefix):]
	if len(valueKeyLeft) == 0 && len(nodeKeyLeft) == 0 {
		// Exactly same node, just update value
		node.setValuePtr(valuePtr)
		node.Deleted = isDeleted
		newNode = node
	} else if isDeleted {
		return nil, nil, errors.New("can't delete non existing node")
	} else {
		if len(valueKeyLeft) == 0 && len(nodeKeyLeft) > 0 {
			// Value(OriginalValue) -> Extension(CommonPrefix)
			//                             |
			//                      Branch(NewValue)
			//                             |
			//                    Value(OriginalValue)
			var err error
			newNode, err = t.splitPathToValueNode(nodeKeyLeft, valuePtr, node.ValuePtr, commonPrefix)
			if err != nil {
				return nil, nil, err
			}
		} else if len(nodeKeyLeft) == 0 && len(valueKeyLeft) > 0 {
			// Value(OriginalValue) -> Extension(commonPrefix)
			//                             |
			//                   Branch(OriginalValue)
			//                             |
			//                      Value(NewValue)
			var err error
			newNode, err = t.splitPathToValueNode(valueKeyLeft, node.ValuePtr, valuePtr, commonPrefix)
			if err != nil {
				return nil, nil, err
			}
		} else {
			// new extension node with common prefix followed by branch node without value should be added
			// and two value nodes (old and new) should be added below it
			// Value(OriginalValue) -> Extension(commonPrefix)
			//                             |
			//                          Branch
			//                           /   \
			//        Value(OriginalValue)   Value(NewValue)
			newValueNode1 := &ValueNode{
				Key:      nodeKeyLeft[1:],
				ValuePtr: node.ValuePtr,
			}
			newValueNodePtr1, err := t.saveNode(newValueNode1)
			if err != nil {
				return nil, nil, err
			}

			// new value node from value
			newValueNode2 := &ValueNode{
				Key:      valueKeyLeft[1:],
				ValuePtr: valuePtr,
			}
			newValueNodePtr2, err := t.saveNode(newValueNode2)
			if err != nil {
				return nil, nil, err
			}

			// branch node for both value nodes
			newBranchNode := &BranchNode{
				Children: make([][]byte, 16),
				ValuePtr: nil,
			}

			newBranchNode.Children[nodeKeyLeft[0]] = newValueNodePtr1
			newBranchNode.Children[valueKeyLeft[0]] = newValueNodePtr2
			newNode = newBranchNode
			if len(commonPrefix) > 0 {
				// before new branch node should come extension node with common prefix
				newBranchNodePtr, err := t.saveNode(newBranchNode)
				if err != nil {
					return nil, nil, err
				}
				newExtensionNode := &ExtensionNode{
					Key:   commonPrefix,
					Child: newBranchNodePtr,
				}
				newNode = newExtensionNode
			}
		}
	}
	newNodePtr, err := t.saveNode(newNode)
	if err != nil {
		return nil, nil, err
	}

	return newNode, newNodePtr, nil
}

func (t *MPTrie) splitPathToValueNode(keyLeft []byte, newValuePtr, orgValuePtr []byte, commonPrefix []byte) (TrieNode, error) {

	var newNode TrieNode
	newValueNode := &ValueNode{
		Key:      keyLeft[1:],
		ValuePtr: orgValuePtr,
	}

	newValueNodePtr, err := t.saveNode(newValueNode)
	if err != nil {
		return nil, err
	}

	// Create new branch (and extension if needed) node for new value, update it with new value node and valuePtr
	newBranchNode := &BranchNode{
		Children: make([][]byte, 16),
		ValuePtr: newValuePtr,
	}
	newBranchNode.Children[keyLeft[0]] = newValueNodePtr
	newNode = newBranchNode
	if len(commonPrefix) > 0 {
		newBranchNodePtr, err := t.saveNode(newBranchNode)
		if err != nil {
			return nil, err
		}
		newExtensionNode := &ExtensionNode{
			Key:   commonPrefix,
			Child: newBranchNodePtr,
		}
		newNode = newExtensionNode
	}
	return newNode, nil

}

func (t *MPTrie) updateExtensionNode(node *ExtensionNode, hexKey, valuePtr []byte, isDeleted bool) (TrieNode, []byte, error) {
	var newNode TrieNode
	commonPrefix := findCommonPrefix(hexKey, node.Key)
	nodeKeyLeft := node.Key[len(commonPrefix):]
	valueKeyLeft := hexKey[len(commonPrefix):]

	if len(valueKeyLeft) == 0 && len(nodeKeyLeft) == 0 {
		// Exactly same node, just update value in following branch node
		branchNode, err := t.store.GetNode(node.Child)
		if err != nil {
			return nil, nil, err
		}
		_, childPtr, err := t.update(branchNode, valueKeyLeft, valuePtr, isDeleted)
		if err != nil {
			return nil, nil, err
		}
		node.Child = childPtr
		newNode = node
	} else {
		if len(valueKeyLeft) == 0 && len(nodeKeyLeft) > 0 {
			// new value split path to node to two parts, new extension node should be added in the middle with commonPrefix key, followed
			// by branch node that holds value

			// create new branch to hold value
			newBranchNode := &BranchNode{
				Children: make([][]byte, 16),
				ValuePtr: valuePtr,
			}
			if len(nodeKeyLeft) > 1 {
				// create new extension that follows new branch with key = nodeKeyLeft[1:] because len(nodeKeyLeft) > 1
				// it points to original node child
				newExtensionNode := &ExtensionNode{
					Key:   nodeKeyLeft[1:],
					Child: node.Child,
				}
				newExtensionNodePtr, err := t.saveNode(newExtensionNode)
				if err != nil {
					return nil, nil, err
				}
				newBranchNode.Children[nodeKeyLeft[0]] = newExtensionNodePtr
			} else {
				// new branch points directly to original extension node child
				newBranchNode.Children[nodeKeyLeft[0]] = node.Child
			}
			newNode = newBranchNode
			if len(commonPrefix) > 0 {
				// if commonPrefix not empty, update existing extension with commonPrefix as key and new branch as child
				node.Key = commonPrefix
				newBranchNodePtr, err := t.saveNode(newBranchNode)
				if err != nil {
					return nil, nil, err
				}
				node.Child = newBranchNodePtr
				newNode = node
			}
		} else if len(nodeKeyLeft) == 0 && len(valueKeyLeft) > 0 {
			// pass value to child node update, just update valuePtr for current node
			childNode, err := t.store.GetNode(node.Child)
			if err != nil {
				return nil, nil, err
			}
			_, newChildPtr, err := t.update(childNode, hexKey[len(commonPrefix):], valuePtr, isDeleted)
			node.Child = newChildPtr
			newNode = node
		} else {
			// keys split at some point, we should create branch node at split point, add extension node as one child and new value node as another
			newValueNode := &ValueNode{
				Key:      valueKeyLeft[1:],
				ValuePtr: valuePtr,
			}
			newValueNodePtr, err := t.saveNode(newValueNode)
			if err != nil {
				return nil, nil, err
			}

			newBranchNode := &BranchNode{
				Children: make([][]byte, 16),
			}

			newBranchNode.Children[valueKeyLeft[0]] = newValueNodePtr
			var secondChildPtr []byte
			if len(nodeKeyLeft) == 1 {
				// existing extension node not needed anymore
				secondChildPtr = node.Child
			} else {
				// we need update existing extension node and add pointer to it to new branch node
				node.Key = nodeKeyLeft[1:]
				secondChildPtr, err = t.saveNode(node)
				if err != nil {
					return nil, nil, err
				}
			}
			newBranchNode.Children[nodeKeyLeft[0]] = secondChildPtr
			newNode = newBranchNode
			if len(commonPrefix) > 0 {
				// if commonPrefix not empty, update existing extension with commonPrefix as key and new branch as child
				node.Key = commonPrefix
				newBranchNodePtr, err := t.saveNode(newBranchNode)
				if err != nil {
					return nil, nil, err
				}
				node.Child = newBranchNodePtr
				newNode = node
			}
		}
	}

	newNodePtr, err := t.saveNode(newNode)
	if err != nil {
		return nil, nil, err
	}

	return newNode, newNodePtr, nil
}

func (t *MPTrie) Delete(key []byte) ([]byte, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if len(key) == 0 {
		return nil, errors.New("can't delete element with empty key")
	}
	hexKey := convertByteToHex(key)

	node, err := t.getNode(hexKey)
	if err != nil {
		return nil, err
	}

	if node == nil {
		return nil, nil
	}

	valuePtr := node.getValuePtr()
	value, err := t.store.GetValue(valuePtr)
	if err != nil {
		return nil, err
	}

	t.root, _, err = t.updateBranchNode(t.root.(*BranchNode), hexKey, valuePtr, true)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (t *MPTrie) Commit(blockNum uint64) error {
	t.lock.RLock()
	defer t.lock.RUnlock()

	rootHash, err := t.root.hash()
	if err != nil {
		return err
	}

	if err := t.persistSubtrie(rootHash); err != nil {
		return err
	}
	return t.store.CommitChanges(blockNum)
}

func (t *MPTrie) persistSubtrie(nodePtr []byte) error {
	wasChanged, err := t.persistNode(nodePtr)
	if err != nil {
		return err
	}

	// If node wasn't changed, no need to persist iт child nodes and can return
	if !wasChanged {
		return nil
	}
	node, err := t.store.GetNode(nodePtr)
	if err != nil {
		return err
	}
	switch n := node.(type) {
	case *BranchNode:
		for _, childPtr := range n.Children {
			if childPtr == nil {
				continue
			}
			if err = t.persistSubtrie(childPtr); err != nil {
				return err
			}
		}
		if n.ValuePtr != nil {
			if _, err := t.store.PersistValue(n.ValuePtr); err != nil {
				return err
			}
		}
	case *ExtensionNode:
		if err = t.persistSubtrie(n.Child); err != nil {
			return err
		}
	case *ValueNode:
		if _, err := t.store.PersistValue(n.ValuePtr); err != nil {
			return err
		}
	default:
		return errors.New("unrecognized node type in trie")
	}

	return err
}

func (t *MPTrie) getNode(hexKey []byte) (TrieNodeWithValue, error) {
	_, node, err := t.getPath(hexKey)
	return node, err
}

func (t *MPTrie) getPath(hexKey []byte) ([]TrieNode, TrieNodeWithValue, error) {
	res := make([]TrieNode, 0)
	node := t.root
	for {
		res = append(res, node)
		if len(hexKey) == 0 {
			if _, ok := node.(*ExtensionNode); ok {
				return nil, nil, errors.New("impossible state - path can't end at extension node, it means extension node with nil key")
			}
			return res, node.(TrieNodeWithValue), nil
		}
		switch node.(type) {
		case *BranchNode:
			childPtr := node.(*BranchNode).GetChildren()[hexKey[0]]
			if childPtr == nil {
				return res, nil, nil
			}
			hexKey = hexKey[1:]
			var err error
			node, err = t.store.GetNode(childPtr)
			if err != nil {
				return nil, nil, err
			}
		case *ExtensionNode:
			nodeHexKey := node.(*ExtensionNode).GetKey()
			if len(nodeHexKey) > len(hexKey) {
				return res, nil, nil
			}
			commonKey := findCommonPrefix(hexKey, nodeHexKey)
			if len(commonKey) < len(nodeHexKey) {
				return res, nil, nil
			}
			hexKey = hexKey[len(commonKey):]
			childPtr := node.(*ExtensionNode).GetChild()
			if childPtr == nil {
				return nil, nil, errors.New("impossible state - extension node can't exist without following branch node")
			}
			var err error
			node, err = t.store.GetNode(childPtr)
			if err != nil {
				return nil, nil, err
			}
		case *ValueNode:
			nodeHexKey := node.(*ValueNode).GetKey()
			if bytes.Equal(hexKey, nodeHexKey) {
				return res, node.(*ValueNode), nil
			}
			return res, nil, nil
		default:
			return nil, nil, errors.New("Unknown trie node type")
		}
	}
}

func (t *MPTrie) saveNode(node TrieNode) ([]byte, error) {
	nodePtr, err := node.hash()
	if err != nil {
		return nil, err
	}

	if err = t.store.PutNode(nodePtr, node); err != nil {
		return nil, err
	}
	return nodePtr, nil
}

func (t *MPTrie) persistNode(nodePtr []byte) (bool, error) {
	isChanged, err := t.store.PersistNode(nodePtr)
	if err != nil {
		return false, err
	}
	return isChanged, nil
}

func convertByteToHex(b []byte) []byte {
	res := make([]byte, len(b)*2)
	i := 0
	for _, v := range b {
		res[i] = v >> 4
		res[i+1] = v & 0x0f
		i += 2
	}
	return res
}

func findCommonPrefix(hexKey1, hexKey2 []byte) []byte {
	maxPrefix := min(len(hexKey1), len(hexKey2))
	res := make([]byte, 0)
	for i := 0; i < maxPrefix; i++ {
		if hexKey1[i] != hexKey2[i] {
			return res
		}
		res = append(res, hexKey1[i])
	}
	return res
}

func CalculateKeyValueHash(key, value []byte) ([]byte, error) {
	bytesToHash := make([]byte, 0)
	if len(key) > 0 {
		bytesToHash = append(bytesToHash, key...)
	}
	if len(value) > 0 {
		valHash, err := crypto.ComputeSHA256Hash(value)
		if err != nil {
			return nil, err
		}
		bytesToHash = append(bytesToHash, valHash...)
	}
	return crypto.ComputeSHA256Hash(bytesToHash)
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func ConstructCompositeKey(dbName, key string) ([]byte, error) {
	bytesToHash := make([]byte, 0)
	if len(dbName) > 0 {
		dbNameHash, err := crypto.ComputeSHA256Hash([]byte(dbName))
		if err != nil {
			return nil, err
		}
		bytesToHash = append(bytesToHash, dbNameHash...)
	}
	if len(key) > 0 {
		keyHash, err := crypto.ComputeSHA256Hash([]byte(key))
		if err != nil {
			return nil, err
		}
		bytesToHash = append(bytesToHash, keyHash...)
	}
	return crypto.ComputeSHA256Hash(bytesToHash)
}
