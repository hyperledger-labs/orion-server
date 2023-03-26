// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockstore

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/hyperledger-labs/orion-server/internal/errors"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestMain(t *testing.M) {
	os.Exit(t.Run())
}

func TestOpenStore(t *testing.T) {
	chunkSizeLimit = 4096

	assertStore := func(t *testing.T, storeDir string, s *Store) {
		require.Equal(t, filepath.Join(storeDir, "filechunks"), s.fileChunksDirPath)
		require.FileExists(t, filepath.Join(storeDir, "filechunks/chunk_0"))
		require.Equal(t, int64(0), s.currentOffset)
		require.Equal(t, uint64(0), s.currentChunkNum)
		require.Equal(t, uint64(0), s.lastCommittedBlockNum)
		require.NoFileExists(t, filepath.Join(storeDir, "undercreation"))

		for _, dbName := range []string{blockIndexDBName, blockHeaderDBName, txValidationInfoDBName} {
			dbPath := filepath.Join(storeDir, dbName)
			require.DirExists(t, dbPath)
		}
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

		require.NoError(t, fileops.CreateDir(filepath.Join(storeDir, "filechunks")))
		require.NoError(t, fileops.CreateDir(filepath.Join(storeDir, "blockindex")))

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

		testDir := t.TempDir()

		storeDir := filepath.Join(testDir, "reopen-empty-store")
		require.NoError(t, fileops.CreateDir(storeDir))

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

		totalBlocks := uint64(1000)
		for blockNumber := uint64(1); blockNumber <= totalBlocks; blockNumber++ {
			b := &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number:                 blockNumber,
						PreviousBaseHeaderHash: []byte(fmt.Sprintf("hash-%d", blockNumber-1)),
					},
					ValidationInfo: []*types.ValidationInfo{
						{
							Flag: types.Flag_VALID,
						},
					},
				},
				Payload: &types.Block_UserAdministrationTxEnvelope{
					UserAdministrationTxEnvelope: &types.UserAdministrationTxEnvelope{
						Payload: &types.UserAdministrationTx{
							UserId: "user1",
							TxId:   "tx1",
							UserDeletes: []*types.UserDelete{
								{
									UserId: "user1",
								},
							},
						},
						Signature: []byte("sign"),
					},
				},
			}

			_, err = s.Commit(b)
			require.NoError(t, err)
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

func TestRecovery(t *testing.T) {
	chunkSizeLimit = 4096
	// scenario 1:
	//  - append block 1 to the file and store only the index for block 1
	//  - ensure that block and metadata DBs are synched after reopening the stores
	t.Run("only index metadata entry is present for block 1", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup(false)

		block := createSampleUserTxBlock(1, nil, nil)
		require.NoError(t, env.s.AddSkipListLinks(block))

		b, err := proto.Marshal(block)
		require.NoError(t, err)
		encodedBlock := snappy.Encode(nil, b)

		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, uint64(len(encodedBlock)))
		content := append(buf[:n], encodedBlock...)
		blockLocation, err := env.s.appendBlock(1, content)
		require.NoError(t, err)

		require.NoError(t, env.s.storeIndexForBlock(1, blockLocation))
		txID := block.GetUserAdministrationTxEnvelope().Payload.TxId

		assertIndexExist(t, env.s, 1, blockLocation)
		assertHashDoesNotExist(t, env.s, 1)
		assertHeaderDoesNotExist(t, env.s, 1)
		assertValidationInfoDoesNotExist(t, env.s, txID)

		env.closeAndReOpenStore(t)
		defer env.cleanup(true)

		assertBlockMetadataExist(t, env.s, block, blockLocation)
	})

	// scenario 2:
	//  - append block 1 to the file and keep metadata store as empty
	//  - ensure that block and metadata DBs are synched after reopening the stores
	t.Run("all metadata entries are missing for block 1", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup(false)

		block := createSampleUserTxBlock(1, nil, nil)
		require.NoError(t, env.s.AddSkipListLinks(block))

		b, err := proto.Marshal(block)
		require.NoError(t, err)
		encodedBlock := snappy.Encode(nil, b)

		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, uint64(len(encodedBlock)))
		content := append(buf[:n], encodedBlock...)
		blockLocation, err := env.s.appendBlock(1, content)
		require.NoError(t, err)

		txID := block.GetUserAdministrationTxEnvelope().Payload.TxId
		assertBlockMetadataDoesNotExist(t, env.s, 1, txID)

		env.closeAndReOpenStore(t)
		defer env.cleanup(true)

		assertBlockMetadataExist(t, env.s, block, blockLocation)
	})

	// scenario 3:
	//  - append block 1 and 2 to the file and store metadata only for block 1
	//  - ensure that block and metadata are synched after reopening the stores
	t.Run("all metadata entries are missing for block 2", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup(false)

		block1 := createSampleUserTxBlock(1, nil, nil)
		require.NoError(t, env.s.AddSkipListLinks(block1))
		_, err := env.s.Commit(block1)
		require.NoError(t, err)

		block1BaseHeaderHash, err := env.s.GetBaseHeaderHash(1)
		require.NoError(t, err)
		block1HeaderHash, err := env.s.GetHash(1)
		require.NoError(t, err)

		block2 := createSampleUserTxBlock(2, block1BaseHeaderHash, block1HeaderHash)
		require.NoError(t, env.s.AddSkipListLinks(block2))

		b, err := proto.Marshal(block2)
		require.NoError(t, err)
		encodedBlock := snappy.Encode(nil, b)

		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, uint64(len(encodedBlock)))
		content := append(buf[:n], encodedBlock...)
		block2Location, err := env.s.appendBlock(1, content)
		require.NoError(t, err)

		txID2 := block2.GetUserAdministrationTxEnvelope().Payload.TxId
		assertBlockMetadataDoesNotExist(t, env.s, 2, txID2)

		env.closeAndReOpenStore(t)
		defer env.cleanup(true)

		assertBlockMetadataExist(t, env.s, block2, block2Location)
	})

	// scenario 4:
	//  - append block 1 and partial block 2 to the file and store only the block 1's metadata
	//  - ensure that the partially written block is truncted
	t.Run("partially written block 2", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup(false)

		block1 := createSampleUserTxBlock(1, nil, nil)
		require.NoError(t, env.s.AddSkipListLinks(block1))
		_, err := env.s.Commit(block1)
		require.NoError(t, err)
		block1Offset := env.s.currentOffset

		block1BaseHeaderHash, err := env.s.GetBaseHeaderHash(1)
		require.NoError(t, err)
		block1HeaderHash, err := env.s.GetHash(1)
		require.NoError(t, err)

		block2 := createSampleUserTxBlock(2, block1BaseHeaderHash, block1HeaderHash)
		require.NoError(t, env.s.AddSkipListLinks(block2))

		b, err := proto.Marshal(block2)
		require.NoError(t, err)
		encodedBlock := snappy.Encode(nil, b)

		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, uint64(len(encodedBlock)))
		content := append(buf[:n], encodedBlock...)
		_, err = env.s.appendBlock(1, content[:len(content)-5])
		require.NoError(t, err)

		txID2 := block2.GetUserAdministrationTxEnvelope().Payload.TxId
		assertBlockMetadataDoesNotExist(t, env.s, 2, txID2)

		env.closeAndReOpenStore(t)
		defer env.cleanup(true)

		assertBlockMetadataDoesNotExist(t, env.s, 2, txID2)
		require.Equal(t, block1Offset, env.s.currentOffset)
	})

	// scenario 5:
	//  - append block n and n+1 to the file and store metadata only for block n
	//  - n'th block should be appended to the end of a fileChunk whereas n+1'th block
	//    should be appended to the start of the next fileChunk
	//  - ensure that block and metadata are synched after reopening the stores

	// scenario 6:
	//  - append block n and n+1 to the file and store metadata only for block n
	//  - n'th block should be appended to the end of a fileChunk whereas n+1'th block
	//    should be partially appended to the start of the next fileChunk
	//  - ensure that the partially written block 2 is deleted and the offset is set to 0
	t.Run("file boundary", func(t *testing.T) {
		setup := func(s *Store) *types.Block {
			totalBlocks := uint64(44)
			var preBlockBaseHash, preBlockHash []byte

			for blockNumber := uint64(1); blockNumber <= totalBlocks; blockNumber++ {
				b := createSampleUserTxBlock(blockNumber, preBlockBaseHash, preBlockHash)

				require.NoError(t, s.AddSkipListLinks(b))
				_, err := s.Commit(b)
				require.NoError(t, err)

				height, err := s.Height()
				require.NoError(t, err)
				require.Equal(t, blockNumber, height)

				blockHeaderBaseBytes, err := proto.Marshal(b.GetHeader().GetBaseHeader())
				require.NoError(t, err)
				preBlockBaseHash, err = crypto.ComputeSHA256Hash(blockHeaderBaseBytes)
				require.NoError(t, err)

				blockHeaderBytes, err := proto.Marshal(b.GetHeader())
				require.NoError(t, err)
				preBlockHash, err = crypto.ComputeSHA256Hash(blockHeaderBytes)
				require.NoError(t, err)
			}

			require.FileExists(t, constructBlockFileChunkPath(s.fileChunksDirPath, 0))
			require.FileExists(t, constructBlockFileChunkPath(s.fileChunksDirPath, 1))
			require.NoFileExists(t, constructBlockFileChunkPath(s.fileChunksDirPath, 2))

			return createSampleUserTxBlock(45, preBlockBaseHash, preBlockHash)
		}

		tests := []struct {
			name              string
			partialBlockWrite bool
		}{
			{
				name: "all metadata associated with the block" +
					" being appended to the beginning of a block file are missing",
				partialBlockWrite: false,
			},
			{
				name:              "partial block is appended to the beginning of a block file",
				partialBlockWrite: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				env := newTestEnv(t)
				defer env.cleanup(false)

				block := setup(env.s)
				require.NoError(t, env.s.AddSkipListLinks(block))

				b, err := proto.Marshal(block)
				require.NoError(t, err)
				encodedBlock := snappy.Encode(nil, b)

				buf := make([]byte, binary.MaxVarintLen64)
				n := binary.PutUvarint(buf, uint64(len(encodedBlock)))
				content := append(buf[:n], encodedBlock...)
				if !env.s.canCurrentFileChunkHold(len(content)) {
					require.NoError(t, env.s.moveToNextFileChunk())
				}
				require.FileExists(t, constructBlockFileChunkPath(env.s.fileChunksDirPath, 2))

				if tt.partialBlockWrite {
					_, err = env.s.appendBlock(1, content[:len(content)-5])
					require.NoError(t, err)

					txID := block.GetUserAdministrationTxEnvelope().Payload.TxId
					assertBlockMetadataDoesNotExist(t, env.s, 45, txID)

					env.closeAndReOpenStore(t)
					defer env.cleanup(true)

					assertBlockMetadataDoesNotExist(t, env.s, 45, txID)
					require.NoFileExists(t, constructBlockFileChunkPath(env.s.fileChunksDirPath, 2))
					return
				}

				location, err := env.s.appendBlock(1, content)
				require.NoError(t, err)

				txID := block.GetUserAdministrationTxEnvelope().Payload.TxId
				assertBlockMetadataDoesNotExist(t, env.s, 45, txID)

				env.closeAndReOpenStore(t)
				defer env.cleanup(true)

				assertBlockMetadataExist(t, env.s, block, location)
			})
		}
	})

	// scenario 7:
	//  - append block 1 and 2 to the file and keep metadata store as empty
	//  - ensure that the store is unrecoverable as this scenario should never occur unless there is a bug
	t.Run("unrecoverable store", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup(false)

		block1 := createSampleUserTxBlock(1, nil, nil)
		require.NoError(t, env.s.AddSkipListLinks(block1))

		b, err := proto.Marshal(block1)
		require.NoError(t, err)
		encodedBlock := snappy.Encode(nil, b)
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(buf, uint64(len(encodedBlock)))
		content := append(buf[:n], encodedBlock...)

		_, err = env.s.appendBlock(1, content)
		require.NoError(t, err)

		block1BaseHeaderHash, err := env.s.GetBaseHeaderHash(1)
		require.EqualError(t, err, "block header base hash not found: 1")
		require.IsType(t, &errors.NotFoundErr{}, err)
		block1HeaderHash, err := env.s.GetHash(1)
		require.EqualError(t, err, "block hash not found: 1")
		require.IsType(t, &errors.NotFoundErr{}, err)

		block2 := createSampleUserTxBlock(2, block1BaseHeaderHash, block1HeaderHash)
		err = env.s.AddSkipListLinks(block2)
		require.EqualError(t, err, "block hash not found: 1")
		require.IsType(t, &errors.NotFoundErr{}, err)

		b, err = proto.Marshal(block2)
		require.NoError(t, err)
		encodedBlock = snappy.Encode(nil, b)
		buf = make([]byte, binary.MaxVarintLen64)
		n = binary.PutUvarint(buf, uint64(len(encodedBlock)))
		content = append(buf[:n], encodedBlock...)

		_, err = env.s.appendBlock(1, content)
		require.NoError(t, err)

		txID1 := block1.GetUserAdministrationTxEnvelope().Payload.TxId
		assertBlockMetadataDoesNotExist(t, env.s, 1, txID1)
		txID2 := block2.GetUserAdministrationTxEnvelope().Payload.TxId
		assertBlockMetadataDoesNotExist(t, env.s, 2, txID2)

		require.NoError(t, env.s.Close())
		_, err = Open(&Config{
			StoreDir: env.storeDir,
			Logger:   env.s.logger,
		})
		require.EqualError(t, err, "the block store can have exactly one"+
			" fully committed block which is not indexed or exaclty one partially"+
			" written block. Any other case is an unexpected behavior")
	})

	// scenario 8:
	//  - append block 1 to the file and store all metadata
	//  - new chunk file is created and the node failed
	//  - ensure that the new chunk file is removed while reopening the store
	t.Run("empty block file is removed", func(t *testing.T) {
		tests := []struct {
			name  string
			empty bool
		}{
			{
				name:  "empty file is removed",
				empty: false,
			},
			{
				name:  "a file with parial block is removed",
				empty: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				env := newTestEnv(t)
				defer env.cleanup(false)

				block := createSampleUserTxBlock(1, nil, nil)
				require.NoError(t, env.s.AddSkipListLinks(block))
				_, err := env.s.Commit(block)
				require.NoError(t, err)

				require.NoError(t, env.s.moveToNextFileChunk())
				if !tt.empty {
					_, err := env.s.appendBlock(1, []byte("random"))
					require.NoError(t, err)
				}

				require.FileExists(t, constructBlockFileChunkPath(env.s.fileChunksDirPath, 1))
				require.Equal(t, uint64(1), env.s.currentChunkNum)

				env.closeAndReOpenStore(t)
				defer env.cleanup(true)

				require.NoFileExists(t, constructBlockFileChunkPath(env.s.fileChunksDirPath, 1))
				require.Equal(t, uint64(0), env.s.currentChunkNum)
			})
		}
	})
}

func assertBlockMetadataDoesNotExist(t *testing.T, s *Store, blockNum uint64, txID string) {
	assertIndexDoesNotExist(t, s, blockNum)
	assertHashDoesNotExist(t, s, blockNum)
	assertHeaderDoesNotExist(t, s, blockNum)
	assertValidationInfoDoesNotExist(t, s, txID)
}

func assertIndexDoesNotExist(t *testing.T, s *Store, blockNum uint64) {
	location, err := s.getLocation(blockNum)
	require.EqualError(t, err, fmt.Sprintf("block not found: %d", blockNum))
	require.IsType(t, &errors.NotFoundErr{}, err)
	require.Nil(t, location)
}

func assertHashDoesNotExist(t *testing.T, s *Store, blockNum uint64) {
	baseHeaderHash, err := s.GetBaseHeaderHash(blockNum)
	require.EqualError(t, err, fmt.Sprintf("block header base hash not found: %d", blockNum))
	require.IsType(t, &errors.NotFoundErr{}, err)
	require.Nil(t, baseHeaderHash)

	blockHeaderHash, err := s.GetHash(blockNum)
	require.EqualError(t, err, fmt.Sprintf("block hash not found: %d", blockNum))
	require.IsType(t, &errors.NotFoundErr{}, err)
	require.Nil(t, blockHeaderHash)
}

func assertHeaderDoesNotExist(t *testing.T, s *Store, blockNum uint64) {
	header, err := s.GetHeader(blockNum)
	require.EqualError(t, err, fmt.Sprintf("block not found: %d", blockNum))
	require.IsType(t, &errors.NotFoundErr{}, err)
	require.Nil(t, header)

	header, err = s.GetHeaderByHash(nil)
	require.EqualError(t, err, fmt.Sprintf("block number by hash not found: "))
	require.Nil(t, header)
}

func assertValidationInfoDoesNotExist(t *testing.T, s *Store, txID string) {
	valInfo, err := s.GetValidationInfo(txID)
	require.EqualError(t, err, fmt.Sprintf("txID not found: %s", txID))
	require.IsType(t, &errors.NotFoundErr{}, err)
	require.Nil(t, valInfo)
}

func assertBlockMetadataExist(t *testing.T, s *Store, block *types.Block, expectedLocation *BlockLocation) {
	blockNum := block.Header.BaseHeader.Number
	txID := block.GetUserAdministrationTxEnvelope().Payload.TxId

	assertIndexExist(t, s, blockNum, expectedLocation)
	assertHashExist(t, s, block)
	assertHeaderExist(t, s, block)
	assertValidationInfoExist(t, s, txID, block.Header.ValidationInfo[0])
}

func assertIndexExist(t *testing.T, s *Store, blockNum uint64, expectedLocation *BlockLocation) {
	location, err := s.getLocation(blockNum)
	require.NoError(t, err)
	require.True(t, proto.Equal(expectedLocation, location))
}

func assertHashExist(t *testing.T, s *Store, block *types.Block) {
	blockNum := block.Header.BaseHeader.Number

	blockHeaderBaseBytes, err := proto.Marshal(block.GetHeader().GetBaseHeader())
	require.NoError(t, err)
	expectedBaseHeaderHash, err := crypto.ComputeSHA256Hash(blockHeaderBaseBytes)
	require.NoError(t, err)

	baseHeaderHash, err := s.GetBaseHeaderHash(blockNum)
	require.NoError(t, err)
	require.Equal(t, expectedBaseHeaderHash, baseHeaderHash)

	blockHeaderBytes, err := proto.Marshal(block.GetHeader())
	require.NoError(t, err)
	expectedBlockHeaderHash, err := crypto.ComputeSHA256Hash(blockHeaderBytes)
	require.NoError(t, err)

	blockHeaderHash, err := s.GetHash(blockNum)
	require.NoError(t, err)
	require.Equal(t, expectedBlockHeaderHash, blockHeaderHash)
}

func assertHeaderExist(t *testing.T, s *Store, block *types.Block) {
	blockNum := block.Header.BaseHeader.Number

	header, err := s.GetHeader(blockNum)
	require.NoError(t, err)
	require.True(t, proto.Equal(block.GetHeader(), header))

	blockHeaderBytes, err := proto.Marshal(block.GetHeader())
	require.NoError(t, err)
	headerHash, err := crypto.ComputeSHA256Hash(blockHeaderBytes)
	require.NoError(t, err)

	header, err = s.GetHeaderByHash(headerHash)
	require.NoError(t, err)
	require.True(t, proto.Equal(block.GetHeader(), header))
}

func assertValidationInfoExist(t *testing.T, s *Store, txID string, expectedValInfo *types.ValidationInfo) {
	valInfo, err := s.GetValidationInfo(txID)
	require.NoError(t, err)
	require.True(t, proto.Equal(expectedValInfo, valInfo))
}
