// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package utils

import (
	"testing"

	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireGather(t *testing.T, reg *prometheus.Registry, expectedLen int) {
	gather, err := reg.Gather()
	require.NoError(t, err)
	assert.Len(t, gather, expectedLen)
	for i, g := range gather {
		t.Logf("[%d] %s: %s", i, g.GetName(), g.GetMetric())
	}
}

func observeAllTxProcessingMetrics(m *TxProcessingMetrics) {
	m.NewLatencyTimer("test1").Observe()
	m.NewLatencyTimer("test2").Observe()
	m.TxPerBlock(100)
	m.QueueSize("test1", 100)
	m.QueueSize("test2", 100)
	m.IncrementTxCount(types.Flag_VALID, "test1")
	m.IncrementTxCount(types.Flag_VALID, "test2")
	m.BlockSize(100)
}

func TestTxProcessingMetrics(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		m := NewTxProcessingMetrics(reg)
		observeAllTxProcessingMetrics(m)
		requireGather(t, reg, 5)
	})

	t.Run("disabled", func(t *testing.T) {
		m := NewTxProcessingMetrics(nil)
		// We just test that no panic occurs
		observeAllTxProcessingMetrics(m)
	})
}

func observeAllDataRequestHandlingMetrics(m *DataRequestHandlingMetrics) {
	m.NewLatencyTimer("test1").Observe()
	m.NewLatencyTimer("test2").Observe()
	m.TxSize(100)
}

func TestDataRequestHandlingMetrics(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		m := NewDataRequestHandlingMetrics(reg)
		observeAllDataRequestHandlingMetrics(m)
		requireGather(t, reg, 2)
	})

	t.Run("disabled", func(t *testing.T) {
		m := NewDataRequestHandlingMetrics(nil)
		// We just test that no panic occurs
		observeAllDataRequestHandlingMetrics(m)
	})
}
