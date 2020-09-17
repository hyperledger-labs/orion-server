package blockstore

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/fileops"
)

func TestOpenStore(t *testing.T) {
	t.Parallel()

	assertStore := func(storeDir string, s *Store) {
		require.Equal(t, filepath.Join(storeDir, "filechunks"), s.fileChunksDirPath)
		require.Equal(t, filepath.Join(storeDir, "blockindex"), s.blockIndexDirPath)
		require.FileExists(t, filepath.Join(storeDir, "filechunks/chunk_0"))
		require.Equal(t, int64(0), s.currentOffset)
		require.Equal(t, uint64(0), s.currentChunkNum)
		require.Equal(t, uint64(0), s.lastCommittedBlockNum)
		require.NoFileExists(t, filepath.Join(storeDir, "undercreation"))
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

		testDir, err := ioutil.TempDir(".", "opentest")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		storeDir := filepath.Join(testDir, "new-store")
		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		require.NoError(t, err)

		assertStore(storeDir, s)
	})

	t.Run("open while partial store exist with an empty dir", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "opentest")
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

		assertStore(storeDir, s)
	})

	t.Run("open while partial store exist with a creation flag", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "opentest")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		// create folders and files to mimic an existing creation but a crash before
		// the successful completion
		storeDir := filepath.Join(testDir, "existing-store-with-flag")
		require.NoError(t, fileops.CreateDir(storeDir))

		underCreationFilePath := filepath.Join(storeDir, "undercreation")
		require.NoError(t, fileops.CreateFile(underCreationFilePath))

		require.NoError(t, fileops.CreateDir(filepath.Join(storeDir, "filechunks")))
		require.NoError(t, fileops.CreateDir(filepath.Join(storeDir, "blockindex")))

		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		defer os.RemoveAll(storeDir)
		require.NoError(t, err)

		assertStore(storeDir, s)
	})

	t.Run("reopen an empty store", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "opentest")
		require.NoError(t, err)
		defer os.RemoveAll(testDir)

		storeDir := filepath.Join(testDir, "reopen-empty-store")
		require.NoError(t, fileops.CreateDir(storeDir))

		c := &Config{
			StoreDir: storeDir,
			Logger:   logger,
		}
		s, err := Open(c)
		defer os.RemoveAll(storeDir)
		require.NoError(t, err)

		assertStore(storeDir, s)

		// close and reopen the store
		require.NoError(t, s.Close())
		s, err = Open(c)
		require.NoError(t, err)

		assertStore(storeDir, s)
	})

	t.Run("reopen non-empty store", func(t *testing.T) {
		t.Parallel()

		testDir, err := ioutil.TempDir(".", "opentest")
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

		assertStore(storeDir, s)

		chunkSizeLimit = 4096
		totalBlocks := uint64(1000)
		for blockNumber := uint64(1); blockNumber <= totalBlocks; blockNumber++ {
			b := &types.Block{
				Header: &types.BlockHeader{
					Number:                  blockNumber,
					PreviousBlockHeaderHash: []byte(fmt.Sprintf("hash-%d", blockNumber-1)),
					TransactionsHash:        []byte(fmt.Sprintf("hash-%d", blockNumber)),
				},
				Payload: &types.Block_UserAdministrationTxEnvelope{
					UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
						Payload: &types.UserAdministrationTx{
							UserID: "user1",
							UserDeletes: []*types.UserDelete{
								{
									UserID: "user1",
								},
							},
						},
						Signature: []byte("sign"),
					},
				},
			}

			require.NoError(t, s.Commit(b))
		}

		fileChunkNum := s.currentChunkNum
		offset := s.currentOffset

		// close and reopen the store
		require.NoError(t, s.Close())
		s, err = Open(c)
		require.NoError(t, err)

		require.Equal(t, fileChunkNum, s.currentChunkNum)
		require.Equal(t, offset, s.currentOffset)
		require.Equal(t, uint64(1000), s.lastCommittedBlockNum)
	})
}
