// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
// Code generated by mockery v2.5.1. DO NOT EDIT.

package mocks

import (
	mock "github.com/stretchr/testify/mock"
	types "github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

// BlockCommitListener is an autogenerated mock type for the BlockCommitListener type
type BlockCommitListener struct {
	mock.Mock
}

// PostBlockCommitProcessing provides a mock function with given fields: block
func (_m *BlockCommitListener) PostBlockCommitProcessing(block *types.Block) error {
	ret := _m.Called(block)

	var r0 error
	if rf, ok := ret.Get(0).(func(*types.Block) error); ok {
		r0 = rf(block)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
