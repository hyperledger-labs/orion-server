package setup

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestCluster(t *testing.T) {
	setupConfig := &Config{
		NumberOfServers:     3,
		TestDirAbsolutePath: "/tmp",
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
	}
	c, err := NewCluster(setupConfig)
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
		setupConfig *Config
		expected    error
	}{
		{
			name: "bdb executable not exist",
			setupConfig: &Config{
				NumberOfServers:     3,
				TestDirAbsolutePath: "/tmp",
				BDBBinaryPath:       "../bdb",
				CmdTimeout:          10 * time.Second,
			},
			expected: errors.New("../bdb executable does not exist"),
		},
		{
			name: "cmd timeout is low",
			setupConfig: &Config{
				NumberOfServers:     3,
				TestDirAbsolutePath: "/tmp",
				BDBBinaryPath:       "../bdb",
				CmdTimeout:          5 * time.Millisecond,
			},
			expected: errors.New("cmd timeout must be at least 1 second"),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c, err := NewCluster(tt.setupConfig)
			require.EqualError(t, err, tt.expected.Error())
			require.Nil(t, c)
		})
	}
}

func testQueryOnServer(t *testing.T, c *Cluster) {
	for _, s := range c.Servers {
		client, err := s.NewRESTClient()
		require.NoError(t, err)

		query := &types.GetConfigQuery{
			UserID: s.adminID,
		}
		response, err := client.GetConfig(
			&types.GetConfigQueryEnvelope{
				Payload:   query,
				Signature: testutils.SignatureFromQuery(t, s.adminSigner, query),
			},
		)
		require.NoError(t, err)
		require.NotNil(t, response)
	}
}

func testConnectionRefused(t *testing.T, s *Server) {
	client, err := s.NewRESTClient()
	require.NoError(t, err)
	_, err = client.GetDBStatus(&types.GetDBStatusQueryEnvelope{
		Payload: &types.GetDBStatusQuery{
			DBName: "abc",
		},
	})
	require.Contains(t, err.Error(), "connection refused")
}
