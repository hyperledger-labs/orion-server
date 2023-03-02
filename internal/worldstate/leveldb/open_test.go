// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package leveldb

import (
	"path/filepath"
	"testing"

	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func TestOpenLevelDBInstance(t *testing.T) {
	assertDBInstance := func(dbRootDir string, l *LevelDB) {
		require.NoFileExists(t, filepath.Join(dbRootDir, "undercreation"))
		require.Equal(t, dbRootDir, l.dbRootDir)
		require.Equal(t, l.size(), len(preCreateDBs))

		for _, dbName := range preCreateDBs {
			db, ok := l.getDB(dbName)
			require.True(t, ok)
			require.NotNil(t, db)
		}

		require.False(t, l.ValidDBName("db1/name"))
		require.True(t, l.ValidDBName("db_2"))
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
		testDir := t.TempDir()

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

		testDir := t.TempDir()

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

		testDir := t.TempDir()

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

		testDir := t.TempDir()

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

		testDir := t.TempDir()

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

		db, ok := l.getDB(worldstate.DefaultDBName)
		require.True(t, ok)
		err = db.file.Put([]byte("key1"), []byte("value1"), &opt.WriteOptions{Sync: true})
		require.NoError(t, err)

		// close and reopen the store
		require.NoError(t, l.Close())
		l, err = Open(conf)
		defer func() {
			require.NoError(t, l.Close())
		}()
		require.NoError(t, err)

		assertDBInstance(dbRootDir, l)

		db, ok = l.getDB(worldstate.DefaultDBName)
		require.True(t, ok)
		actualValue, err := db.file.Get([]byte("key1"), nil)
		require.NoError(t, err)
		require.Equal(t, []byte("value1"), actualValue)
	})
}

func TestValidDBName(t *testing.T) {
	tests := []struct {
		name           string
		dbName         string
		expectedResult bool
	}{
		{
			name:           "valid db name",
			dbName:         "db1DZ0-_.",
			expectedResult: true,
		},
		{
			name:           "invalid db name",
			dbName:         "/db1DZ0/-_.",
			expectedResult: false,
		},
		{
			name:           "invalid db name",
			dbName:         "$p",
			expectedResult: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			env := newTestEnv(t)
			defer env.cleanup()
			require.Equal(t, tt.expectedResult, env.l.ValidDBName(tt.dbName))
		})
	}
}
