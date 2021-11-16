package main

import (
	"encoding/base64"
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
			out, err := exec.Command("go", "run", "encoder.go", arg).CombinedOutput()
			require.NoError(t, err)

			expectedOut := help + "\n  -data string\n    \tjson or string data to be encoded. " +
				"Surround the JSON data with single quotes. " +
				"An example json data is '{\"userID\":\"admin\"}'\n"
			require.Equal(t, expectedOut, string(out))
		}
	})

	t.Run("correct args", func(t *testing.T) {
		for _, data := range []string{
			"'{\"user_id\":\"admin\"}'",
			"name",
		} {
			args := []string{
				"run",
				"encoder.go",
				"-data=" + data,
			}

			out, err := exec.Command("go", args...).Output()
			require.NoError(t, err)

			out, err = base64.StdEncoding.DecodeString(string(out))
			require.NoError(t, err)
			require.Equal(t, []byte(data), out)
		}
	})
}
