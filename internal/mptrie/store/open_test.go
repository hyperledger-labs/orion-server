package store

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/IBM-Blockchain/bcdb-server/internal/fileops"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
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

		testDir, err := ioutil.TempDir(".", "open_test")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

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

		testDir, err := ioutil.TempDir(".", "open_test")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		// create folders and files to mimic an existing creation but a crash before
		// the successful completion
		storeDir := filepath.Join(testDir, "existing-store")
		require.NoError(t, fileops.CreateDir(storeDir))

		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		defer os.RemoveAll(storeDir)
		require.NoError(t, err)

		assertStore(t, storeDir, s)
	})

	t.Run("open while partial store exist with a creation flag", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "open_test")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

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
		defer os.RemoveAll(storeDir)
		require.NoError(t, err)

		assertStore(t, storeDir, s)
	})

	t.Run("reopen an empty store", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "open_test")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		storeDir := filepath.Join(testDir, "reopen-empty-store")

		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		defer os.RemoveAll(storeDir)
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

		testDir, err := ioutil.TempDir(".", "open_test")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		storeDir := filepath.Join(testDir, "reopen-non-empty-store")
		defer os.RemoveAll(storeDir)
		require.NoError(t, fileops.CreateDir(storeDir))

		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		defer os.RemoveAll(storeDir)
		require.NoError(t, err)

		assertStore(t, storeDir, s)
		pointers := fillStore(t, s, true, 0, uint64(999))

		// close and reopen the store
		require.NoError(t, s.Close())
		s, err = Open(c)
		require.NoError(t, err)

		assertStore(t, storeDir, s)
		checkStoreContent(t, s, pointers, true, true, 0)
		lastBlock, err := s.LastBlock()
		require.NoError(t, err)
		require.Equal(t, uint64(999), lastBlock)
	})
}

func assertStore(t *testing.T, storeDir string, s *Store) {
	require.NoFileExists(t, filepath.Join(storeDir, "undercreation"))

	dbPath := filepath.Join(storeDir, trieDataDBName)
	require.DirExists(t, dbPath)
}
