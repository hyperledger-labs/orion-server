package main

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEncoder(t *testing.T) {
	t.Run("incorrect args", func(t *testing.T) {
		for _, arg := range []string{
			"",
			"abc",
		} {
			out, err := exec.Command("go", "run", "decoder.go", arg).CombinedOutput()
			require.NoError(t, err)

			expectedOut := help + "\n  -getresponse string\n    \tjson output of " +
				"GetDataResponseEnvelope. The value field in the json output will be decoded. " +
				"Surround the JSON data with single quotes.\n"
			require.Equal(t, expectedOut, string(out))
		}
	})

	t.Run("correct args", func(t *testing.T) {
		data := `{"response":{"header":{"node_id":"bdb-node-1"},"value":"eyJuYW1lIjoiYWJjIiwiYWdlIjozMSwiZ3JhZHVhdGVkIjp0cnVlfQ==","metadata":{"version":{"block_num":4},"access_control":{"read_users":{"alice":true,"bob":true},"read_write_users":{"alice":true}}}},"signature":"MEYCIQCRpq5MCakj+GP0xLe8GbVH8rA0pQehW4EOfLyVWLdXUAIhANv5PtZG9Sw8mN6c0jIwuqL03kM+GZT0m4H2qtHRnIIS"}`

		args := []string{
			"run",
			"decoder.go",
			"-getresponse=" + data,
		}

		out, err := exec.Command("go", args...).Output()
		require.NoError(t, err)

		expectedOut := `{"response":{"header":{"node_id":"bdb-node-1"},"value":"{\"name\":\"abc\",\"age\":31,\"graduated\":true}","metadata":{"version":{"block_num":4},"access_control":{"read_users":{"alice":true,"bob":true},"read_write_users":{"alice":true}}}},"signature":"MEYCIQCRpq5MCakj+GP0xLe8GbVH8rA0pQehW4EOfLyVWLdXUAIhANv5PtZG9Sw8mN6c0jIwuqL03kM+GZT0m4H2qtHRnIIS"}`
		require.Equal(t, expectedOut, string(out))
	})
}
