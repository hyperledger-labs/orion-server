// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package main

import (
	"bytes"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// func TestBDBCmd(t *testing.T) {
// 	cmd := bdbCmd()
// 	require.NoError(t, cmd.Execute())

// 	expectedOutput := "To start and interact with a blockchain database\n\nUsage:\n  bdb [command]\n\nAvailable Commands:\n  help        Help about any command\n  start       Starts a blockchain database\n  version     Print the version of blockchain database\n\nFlags:\n  -h, --help   help for bdb\n\nUse \"bdb [command] --help\" for more information about a command.\n"
// 	require.Equal(t, expectedOutput, executeCaptureOutputs(cmd))

// 	cmd.SetArgs([]string{"start"})
// 	require.Contains(t, executeCaptureOutputs(cmd), "Starting a blockchain database")
// }

// func TestStartCmd(t *testing.T) {
// 	cmd := startCmd()
// 	require.NoError(t, cmd.Execute())

// 	require.Contains(t, executeCaptureOutputs(cmd), "Starting a blockchain database")

// 	cmd.SetArgs([]string{"arg1", "arg2"})
// 	require.EqualError(t, cmd.Execute(), "Trailing arguments detected")
// }

func TestVersionCmd(t *testing.T) {
	cmd := versionCmd()
	cmd.SetArgs([]string{})
	err, outStr, errStr := executeCaptureOutputs(cmd)
	assert.NoError(t, err)
	assert.Equal(t, "bdb 0.1\n", outStr)
	assert.Equal(t, "", errStr)

	cmd = versionCmd()
	cmd.SetArgs([]string{"--help"})
	err, outStr, errStr = executeCaptureOutputs(cmd)
	assert.NoError(t, err)
	assert.Equal(t, "Print the version of the blockchain database server.\n\nUsage:\n  version [flags]\n\nFlags:\n  -h, --help   help for version\n", outStr)
	assert.Equal(t, "", errStr)

	cmd = versionCmd()
	cmd.SetArgs([]string{"-h"})
	err, outStr, errStr = executeCaptureOutputs(cmd)
	assert.NoError(t, err)
	assert.Equal(t, "Print the version of the blockchain database server.\n\nUsage:\n  version [flags]\n\nFlags:\n  -h, --help   help for version\n", outStr)
	assert.Equal(t, "", errStr)

	cmd = versionCmd()
	cmd.SetArgs([]string{"arg1", "arg2"})
	err, outStr, errStr = executeCaptureOutputs(cmd)
	require.EqualError(t, err, "Trailing arguments detected")
	assert.Equal(t, "Error: Trailing arguments detected\nUsage:\n  version [flags]\n\nFlags:\n  -h, --help   help for version\n\n", outStr)
	assert.Equal(t, "", errStr)
}

func executeCaptureOutputs(cmd *cobra.Command) (error, string, string) {
	bufOut := bytes.Buffer{}
	bufErr := bytes.Buffer{}
	cmd.SetOut(&bufOut)
	cmd.SetErr(&bufErr)
	err := cmd.Execute()
	return err, bufOut.String(), bufErr.String()
}
