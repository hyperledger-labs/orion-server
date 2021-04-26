// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mptrie

import "github.ibm.com/blockchaindb/server/pkg/crypto"

type TrieNode interface {
	hash() ([]byte, error)
}

type TrieNodeWithValue interface {
	TrieNode
	getValuePtr() []byte
	setValuePtr([]byte)
	isDeleted() bool
}

func (m *BranchNode) hash() ([]byte, error) {
	bytesToHash := make([]byte, 0)
	for _, child := range m.Children {
		bytesToHash = append(bytesToHash, child...)
	}
	if len(m.ValuePtr) > 0 {
		bytesToHash = append(bytesToHash, m.ValuePtr...)
	}
	if m.isDeleted() {
		bytesToHash = append(bytesToHash, 1)
	}
	return crypto.ComputeSHA256Hash(bytesToHash)
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
	bytesToHash := make([]byte, 0)
	if len(m.Key) > 0 {
		bytesToHash = append(bytesToHash, m.Key...)
	}
	if len(m.Child) > 0 {
		bytesToHash = append(bytesToHash, m.Child...)
	}
	return crypto.ComputeSHA256Hash(bytesToHash)
}

func (m *ValueNode) hash() ([]byte, error) {
	bytesToHash := make([]byte, 0)
	if len(m.Key) > 0 {
		bytesToHash = append(bytesToHash, m.Key...)
	}
	if len(m.ValuePtr) > 0 {
		bytesToHash = append(bytesToHash, m.ValuePtr...)
	}
	if m.isDeleted() {
		bytesToHash = append(bytesToHash, 1)
	}
	return crypto.ComputeSHA256Hash(bytesToHash)
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
