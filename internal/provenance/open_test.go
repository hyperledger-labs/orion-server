// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package provenance

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"testing"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
	"github.com/hidal-go/hidalgo/kv/flat/leveldb"
	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestOpenStore(t *testing.T) {
	t.Parallel()

	assertStore := func(t *testing.T, storeDir string, s *Store) {
		require.Equal(t, storeDir, s.rootDir)
		require.NotNil(t, s.cayleyGraph)
		p := cayley.StartPath(s.cayleyGraph).Out()
		quadValues, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph.QuadStore)
		require.NoError(t, err)
		require.Nil(t, quadValues)
	}

	lc := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(lc)
	require.NoError(t, err)

	t.Run("open a new store", func(t *testing.T) {
		t.Parallel()

		testDir := t.TempDir()

		storeDir := filepath.Join(testDir, "new-store")
		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		defer func() {
			if err := s.Close(); err != nil {
				t.Errorf("error wile closing the store: %s", err.Error())
			}
		}()

		require.NoError(t, err)

		assertStore(t, storeDir, s)
	})

	t.Run("open and reopen a disabled store", func(t *testing.T) {
		t.Parallel()

		testDir := t.TempDir()

		storeDir := filepath.Join(testDir, "new-store")
		c := &Config{
			StoreDir: storeDir,
			Disabled: true,
			Logger:   logger,
		}
		s, err := Open(c)
		require.NoError(t, err)
		require.Nil(t, s)
		err = s.Close() // close a nil store
		require.NoError(t, err)

		// reopen a disabled
		s, err = Open(c)
		require.NoError(t, err)
		require.Nil(t, s)

		//cannot re-enable a disabled store
		c.Disabled = false
		s, err = Open(c)
		require.EqualError(t, err, fmt.Sprintf("provenance store was disabled and cannot be re-enabled: disabled flag exists: %s", path.Join(c.StoreDir, disabledFlag)))
		require.Nil(t, s)
	})

	t.Run("disable an active store", func(t *testing.T) {
		t.Parallel()

		testDir := t.TempDir()

		storeDir := filepath.Join(testDir, "new-store")
		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		require.NoError(t, err)

		assertStore(t, storeDir, s)
		err = s.Close()
		require.NoError(t, err)

		c.Disabled = true
		s, err = Open(c)
		require.NoError(t, err)
		require.Nil(t, s)

		//cannot re-enable a disabled store
		c.Disabled = false
		s, err = Open(c)
		require.EqualError(t, err, fmt.Sprintf("provenance store was disabled and cannot be re-enabled: disabled flag exists: %s", path.Join(c.StoreDir, disabledFlag)))
		require.Nil(t, s)
	})

	t.Run("open while partial store exist with an empty dir", func(t *testing.T) {
		t.Parallel()

		testDir := t.TempDir()

		// create folders and files to mimic an existing creation but a crash before
		// the successful completion
		storeDir := filepath.Join(testDir, "existing-store")
		require.NoError(t, fileops.CreateDir(storeDir))

		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		defer func() {
			if err := s.Close(); err != nil {
				t.Errorf("error wile closing the store: %s", err.Error())
			}
		}()
		require.NoError(t, err)

		assertStore(t, storeDir, s)
	})

	t.Run("open while partial store exist with a creation flag", func(t *testing.T) {
		t.Parallel()

		testDir := t.TempDir()

		// create folders and files to mimic an existing creation but a crash before
		// the successful completion
		storeDir := filepath.Join(testDir, "existing-store-with-flag")
		require.NoError(t, fileops.CreateDir(storeDir))

		underCreationFilePath := filepath.Join(storeDir, "undercreation")
		require.NoError(t, fileops.CreateFile(underCreationFilePath))

		require.NoError(t, graph.InitQuadStore(leveldb.Name, storeDir, nil))

		cayleyGraph, err := cayley.NewGraph(leveldb.Name, storeDir, nil)
		require.NoError(t, err)
		require.NotNil(t, cayleyGraph)
		require.NoError(t, cayleyGraph.Close())

		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		defer func() {
			if err := s.Close(); err != nil {
				t.Errorf("error wile closing the store: %s", err.Error())
			}
		}()
		require.NoError(t, err)

		assertStore(t, storeDir, s)
		s.Close()
	})

	t.Run("reopen an empty store", func(t *testing.T) {
		t.Parallel()

		testDir := t.TempDir()

		storeDir := filepath.Join(testDir, "reopen-empty-store")
		require.NoError(t, fileops.CreateDir(storeDir))

		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		require.NoError(t, err)
		s.Close()

		assertStore(t, storeDir, s)

		// close and reopen the store
		require.NoError(t, s.Close())
		s, err = Open(c)
		defer func() {
			if err := s.Close(); err != nil {
				t.Errorf("error wile closing the store: %s", err.Error())
			}
		}()
		require.NoError(t, err)

		assertStore(t, storeDir, s)
	})

	t.Run("reopen non-empty store", func(t *testing.T) {
		t.Parallel()

		testDir := t.TempDir()

		storeDir := filepath.Join(testDir, "reopen-non-empty-store")
		require.NoError(t, fileops.CreateDir(storeDir))

		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		require.NoError(t, err)

		assertStore(t, storeDir, s)

		q := quad.Make("subject", "predicate", "object", "")
		err = s.cayleyGraph.AddQuad(q)
		require.NoError(t, err)

		// close and reopen the store
		require.NoError(t, s.Close())
		s, err = Open(c)
		defer func() {
			if err := s.Close(); err != nil {
				t.Errorf("error wile closing the store: %s", err.Error())
			}
		}()
		require.NoError(t, err)

		expectedNodes := []quad.Value{
			quad.String("subject"),
			quad.String("predicate"),
			quad.String("object"),
			quad.String(""),
		}
		p := cayley.StartPath(s.cayleyGraph)
		quadValues, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph.QuadStore)
		require.NoError(t, err)
		require.Len(t, quadValues, 4)
		require.ElementsMatch(t, expectedNodes, quadValues)
	})
}
