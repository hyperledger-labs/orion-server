package store

import (
	"path/filepath"
	"testing"

	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
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
		require.NoError(t, err)

		assertStore(t, storeDir, s)
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

		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		require.NoError(t, err)

		assertStore(t, storeDir, s)
	})

	t.Run("reopen an empty store", func(t *testing.T) {
		t.Parallel()

		testDir := t.TempDir()

		storeDir := filepath.Join(testDir, "reopen-empty-store")

		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		require.NoError(t, err)

		assertStore(t, storeDir, s)

		// close and reopen the store
		require.NoError(t, s.Close())
		s, err = Open(c)
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
		pointers := fillStore(t, s, true, 0, uint64(999))

		// close and reopen the store
		require.NoError(t, s.Close())
		s, err = Open(c)
		require.NoError(t, err)

		assertStore(t, storeDir, s)
		checkStoreContent(t, s, pointers, true, true, 0)
		lastBlock, err := s.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(999), lastBlock)
	})

	t.Run("open a disabled store", func(t *testing.T) {
		t.Parallel()

		testDir := t.TempDir()

		storeDir := filepath.Join(testDir, "reopen-non-empty-store")
		require.NoError(t, fileops.CreateDir(storeDir))

		c := &Config{
			Disabled: true,
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		require.NoError(t, err)

		// close and reopen the store
		require.NoError(t, s.Close())
		s, err = Open(c)
		require.NoError(t, err)
		require.NoError(t, s.Close())
	})
}

func assertStore(t *testing.T, storeDir string, s *Store) {
	require.NoFileExists(t, filepath.Join(storeDir, "undercreation"))

	dbPath := filepath.Join(storeDir, trieDataDBName)
	require.DirExists(t, dbPath)
}
