package backend

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPath(t *testing.T) {
	t.Run("worldstate path", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "statedb")
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		require.Equal(
			t,
			constructWorldStatePath(dir),
			fmt.Sprintf("%s/worldstate", dir),
		)
	})

	t.Run("blockstore path", func(t *testing.T) {
		dir, err := ioutil.TempDir("", "blockstore")
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		require.Equal(
			t,
			constructBlockStorePath(dir),
			fmt.Sprintf("%s/blockstore", dir),
		)
	})
}
