package leveldb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.ibm.com/blockchaindb/server/pkg/common/logger"
	"github.ibm.com/blockchaindb/server/pkg/fileops"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

func TestOpenLevelDBInstance(t *testing.T) {
	assertDBInstance := func(dbRootDir string, l *LevelDB) {
		require.NoFileExists(t, filepath.Join(dbRootDir, "undercreation"))
		require.Equal(t, dbRootDir, l.dbRootDir)
		require.Len(t, l.dbs, len(preCreateDBs))

		for _, dbName := range preCreateDBs {
			require.NotNil(t, l.dbs[dbName])
		}
	}

	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	t.Run("open a new levelDB instance", func(t *testing.T) {
		testDir, err := ioutil.TempDir(".", "opentest")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		dbRootDir := filepath.Join(testDir, "new-leveldb")
		conf := &Config{
			DBRootDir: dbRootDir,
			Logger:    logger,
		}
		l, err := Open(conf)
		defer func() {
			require.NoError(t, l.Close())
		}()
		require.NoError(t, err)

		assertDBInstance(dbRootDir, l)
	})

	t.Run("open while partial leveldb instance exist with an empty dir", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "opentest")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		// create folders and files to mimic an existing creation but a crash before
		// the successful completion
		dbRootDir := filepath.Join(testDir, "existing-leveldb")
		require.NoError(t, fileops.CreateDir(dbRootDir))

		conf := &Config{
			DBRootDir: dbRootDir,
			Logger:    logger,
		}
		l, err := Open(conf)
		defer func() {
			require.NoError(t, l.Close())
		}()
		require.NoError(t, err)

		assertDBInstance(dbRootDir, l)
	})

	t.Run("open while partial leveldb instance exist with the creation flag", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "opentest")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		// create folders and files to mimic an existing creation but a crash before
		// the successful completion
		dbRootDir := filepath.Join(testDir, "existing-leveldb")
		require.NoError(t, fileops.CreateDir(dbRootDir))

		underCreationFlagPath := filepath.Join(dbRootDir, underCreationFlag)
		require.NoError(t, fileops.CreateFile(underCreationFlagPath))

		conf := &Config{
			DBRootDir: dbRootDir,
			Logger:    logger,
		}
		l, err := Open(conf)
		defer func() {
			require.NoError(t, l.Close())
		}()
		require.NoError(t, err)

		assertDBInstance(dbRootDir, l)
	})

	t.Run("reopen an empty leveldb", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "opentest")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		dbRootDir := filepath.Join(testDir, "reopen-empty-store")
		conf := &Config{
			DBRootDir: dbRootDir,
			Logger:    logger,
		}
		l, err := Open(conf)
		require.NoError(t, err)

		assertDBInstance(dbRootDir, l)

		// close and reopen the store
		require.NoError(t, l.Close())
		l, err = Open(conf)
		defer func() {
			require.NoError(t, l.Close())
		}()
		require.NoError(t, err)

		assertDBInstance(dbRootDir, l)
	})

	t.Run("reopen non-empty leveldb", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "opentest")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		dbRootDir := filepath.Join(testDir, "reopen-non-empty-store")
		conf := &Config{
			DBRootDir: dbRootDir,
			Logger:    logger,
		}
		l, err := Open(conf)
		defer func() {
			require.NoError(t, l.Close())
		}()
		require.NoError(t, err)

		assertDBInstance(dbRootDir, l)

		db := l.dbs[worldstate.DefaultDBName]
		db.file.Put([]byte("key1"), []byte("value1"), &opt.WriteOptions{Sync: true})

		// close and reopen the store
		require.NoError(t, l.Close())
		l, err = Open(conf)
		defer func() {
			require.NoError(t, l.Close())
		}()
		require.NoError(t, err)

		assertDBInstance(dbRootDir, l)

		db = l.dbs[worldstate.DefaultDBName]
		actualValue, err := db.file.Get([]byte("key1"), nil)
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), actualValue)
	})
}
