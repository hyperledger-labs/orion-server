// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mptrie

import "github.com/IBM-Blockchain/bcdb-server/pkg/crypto"

type TrieNode interface {
	hash() ([]byte, error)
	bytes() [][]byte
}

type TrieNodeWithValue interface {
	TrieNode
	getValuePtr() []byte
	setValuePtr([]byte)
	isDeleted() bool
}

var deleteBytes = []byte{1}

func (m *BranchNode) hash() ([]byte, error) {
	return calcHash(m.bytes())
}

func (m *BranchNode) bytes() [][]byte {
	bytes := make([][]byte, 0)
	for _, child := range m.Children {
		bytes = append(bytes, child)
	}
	if len(m.ValuePtr) > 0 {
		bytes = append(bytes, m.ValuePtr)
	}
	if m.isDeleted() {
		bytes = append(bytes, deleteBytes)
	}
	return bytes
}

func (m *BranchNode) getValuePtr() []byte {
	return m.GetValuePtr()
}

func (m *BranchNode) setValuePtr(v []byte) {
	m.ValuePtr = v
}

func (m *BranchNode) isDeleted() bool {
	return m.GetDeleted()
}

func (m *ExtensionNode) hash() ([]byte, error) {
	return calcHash(m.bytes())
}

func (m *ExtensionNode) bytes() [][]byte {
	bytes := make([][]byte, 0)
	if len(m.Key) > 0 {
		bytes = append(bytes, m.Key)
	}
	if len(m.Child) > 0 {
		bytes = append(bytes, m.Child)
	}
	return bytes
}

func (m *ValueNode) hash() ([]byte, error) {
	return calcHash(m.bytes())
}

func (m *ValueNode) bytes() [][]byte {
	bytes := make([][]byte, 0)
	if len(m.Key) > 0 {
		bytes = append(bytes, m.Key)
	}
	if len(m.ValuePtr) > 0 {
		bytes = append(bytes, m.ValuePtr)
	}
	if m.isDeleted() {
		bytes = append(bytes, deleteBytes)
	}
	return bytes
}

func (m *ValueNode) getValuePtr() []byte {
	return m.GetValuePtr()
}

func (m *ValueNode) setValuePtr(v []byte) {
	m.ValuePtr = v
}

func (m *ValueNode) isDeleted() bool {
	return m.GetDeleted()
}

func (m *EmptyNode) hash() ([]byte, error) {
	panic("can't hash empty node")
}

func (m *EmptyNode) bytes() [][]byte {
	panic("can't hash empty node")
}

func calcHash(bytes [][]byte) ([]byte, error) {
	bytesToHash := make([]byte, 0)
	for _, b := range bytes {
		bytesToHash = append(bytesToHash, b...)
	}
	return crypto.ComputeSHA256Hash(bytesToHash)
}
