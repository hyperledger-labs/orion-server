// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package mptrie

import (
	"github.com/hyperledger-labs/orion-server/pkg/state"
)

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

func (m *BranchNode) hash() ([]byte, error) {
	return state.CalcHash(m.bytes())
}

func (m *BranchNode) bytes() [][]byte {
	bytes := make([][]byte, 0)
	bytes = append(bytes, m.Children...)

	if len(m.ValuePtr) > 0 {
		bytes = append(bytes, m.ValuePtr)
	}
	if m.isDeleted() {
		bytes = append(bytes, state.KeyDeleteMarkerBytes)
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
	return state.CalcHash(m.bytes())
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
	return state.CalcHash(m.bytes())
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
		bytes = append(bytes, state.KeyDeleteMarkerBytes)
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
	panic("can't Hash empty node")
}

func (m *EmptyNode) bytes() [][]byte {
	panic("can't hash empty node")
}
