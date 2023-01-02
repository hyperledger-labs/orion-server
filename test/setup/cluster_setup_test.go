// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package setup_test

import (
	"errors"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
)

func TestClusterSetup(t *testing.T) {
	dir := t.TempDir()
	setupConfig := &setup.Config{
		NumberOfServers:     3,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        64000,
		BasePeerPort:        64100,
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())
	testQueryOnServer(t, c)

	require.NoError(t, c.Restart())
	testQueryOnServer(t, c)

	require.NoError(t, c.RestartServer(c.Servers[0]))
	testQueryOnServer(t, c)

	require.NoError(t, c.ShutdownServer(c.Servers[0]))
	testConnectionRefused(t, c.Servers[0])

	require.NoError(t, c.StartServer(c.Servers[0]))
	testQueryOnServer(t, c)
}

func TestClusterErrorCases(t *testing.T) {
	tests := []struct {
		name        string
		setupConfig *setup.Config
		expected    error
	}{
		{
			name: "bdb executable not exist",
			setupConfig: &setup.Config{
				NumberOfServers:     3,
				TestDirAbsolutePath: "/tmp",
				BDBBinaryPath:       "../bdb",
				CmdTimeout:          10 * time.Second,
			},
			expected: errors.New("../bdb executable does not exist"),
		},
		{
			name: "cmd timeout is low",
			setupConfig: &setup.Config{
				NumberOfServers:     3,
				TestDirAbsolutePath: "/tmp",
				BDBBinaryPath:       "../../bin/bdb",
				CmdTimeout:          5 * time.Millisecond,
			},
			expected: errors.New("cmd timeout must be at least 1 second"),
		},
		{
			name: "set base ports",
			setupConfig: &setup.Config{
				NumberOfServers:     3,
				TestDirAbsolutePath: "/tmp",
				BDBBinaryPath:       "../../bin/bdb",
				CmdTimeout:          5 * time.Second,
			},
			expected: errors.New("set BaseNodePort >0 & BasePeerPort >0"),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := setup.NewCluster(tt.setupConfig)
			require.EqualError(t, err, tt.expected.Error())
			require.Nil(t, c)
		})
	}
}

func testQueryOnServer(t *testing.T, c *setup.Cluster) {
	for _, s := range c.Servers {
		client, err := s.NewRESTClient(nil)
		require.NoError(t, err)

		query := &types.GetConfigQuery{
			UserId: s.AdminID(),
		}
		response, err := client.GetConfig(
			&types.GetConfigQueryEnvelope{
				Payload:   query,
				Signature: testutils.SignatureFromQuery(t, s.AdminSigner(), query),
			},
		)
		require.NoError(t, err)
		require.NotNil(t, response)
	}
}

func testConnectionRefused(t *testing.T, s *setup.Server) {
	client, err := s.NewRESTClient(nil)
	require.NoError(t, err)
	_, err = client.GetDBStatus(&types.GetDBStatusQueryEnvelope{
		Payload: &types.GetDBStatusQuery{
			DbName: "abc",
		},
	})
	require.Contains(t, err.Error(), "connection refused")
}
