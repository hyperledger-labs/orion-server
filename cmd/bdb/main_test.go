package main

import (
	"bytes"
	"log"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestBDBCmd(t *testing.T) {
	cmd := bdbCmd()
	require.NoError(t, cmd.Execute())

	expectedOutput := "To start and interact with a blockchain database\n\nUsage:\n  bdb [command]\n\nAvailable Commands:\n  help        Help about any command\n  start       Starts a blockchain database\n  version     Print the version of blockchain database\n\nFlags:\n  -h, --help   help for bdb\n\nUse \"bdb [command] --help\" for more information about a command.\n"
	require.Equal(t, expectedOutput, captureOutput(cmd))

	cmd.SetArgs([]string{"start"})
	require.Contains(t, captureOutput(cmd), "starting a blockchain database")
}

func TestStartCmd(t *testing.T) {
	cmd := startCmd()
	require.NoError(t, cmd.Execute())

	require.Contains(t, captureOutput(cmd), "starting a blockchain database")

	cmd.SetArgs([]string{"arg1", "arg2"})
	require.EqualError(t, cmd.Execute(), "trailing arguments detected")
}

func TestVersionCmd(t *testing.T) {
	cmd := versionCmd()
	require.NoError(t, cmd.Execute())

	require.Contains(t, captureOutput(cmd), "bdb 0.1")

	cmd.SetArgs([]string{"arg1", "arg2"})
	require.EqualError(t, cmd.Execute(), "trailing arguments detected")
}

func captureOutput(cmd *cobra.Command) string {
	var buf bytes.Buffer
	cmd.SetOutput(&buf)
	log.SetOutput(&buf)
	cmd.Execute()
	log.SetOutput(os.Stderr)
	return buf.String()
}
