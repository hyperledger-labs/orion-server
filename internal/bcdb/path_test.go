// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPath(t *testing.T) {
	t.Run("worldstate path", func(t *testing.T) {
		dir := t.TempDir()

		require.Equal(
			t,
			constructWorldStatePath(dir),
			fmt.Sprintf("%s/worldstate", dir),
		)
	})

	t.Run("blockstore path", func(t *testing.T) {
		dir := t.TempDir()

		require.Equal(
			t,
			constructBlockStorePath(dir),
			fmt.Sprintf("%s/blockstore", dir),
		)
	})
}
