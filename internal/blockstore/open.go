// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockstore

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var (
	// blocks are stored in an append-only file. As the
	// the file size could grow significantly in a longer
	// run, we use file chunks so that it would be easy
	// to archive chunks to free some storage space
	chunkPrefix    = "chunk_"
	chunkSizeLimit = int64(64 * 1024 * 1024)

	// block file chunks are stored inside fileChunksDir
	// while the index to the block file's offset to fetch
	// a given block number is stored inside blockIndexDir
	fileChunksDirName      = "filechunks"
	blockIndexDBName       = "blockindex"
	blockHeaderDBName      = "blockheader"
	txValidationInfoDBName = "txvalidationinfo"

	// underCreationFlag is used to mark that the store
	// is being created. If a failure happens during the
	// creation, the retry logic will use this file to
	// detect the partially created store and do cleanup
	// before creating a new store
	underCreationFlag = "undercreation"

	// Namespaces for block header and block hash storage:
	// number -> header bytes
	headerBytesNs = []byte{0}
	// number -> header (block) hash
	headerHashNs = []byte{1}
	// hash -> block number
	headerHashToBlockNumNs = []byte{2}
	// number -> base header (without validation info) hash
	headerBaseHashNs = []byte{3}
	// number -> block tx ids array
	blockTxsIDNs = []byte{4}
)

// Store maintains a chain of blocks in an append-only
// filesystem
type Store struct {
	fileChunksDirPath     string
	currentFileChunk      *os.File
	currentOffset         int64
	currentChunkNum       uint64
	lastCommittedBlockNum uint64
	blockIndexDB          *leveldb.DB
	blockHeaderDB         *leveldb.DB
	txValidationInfoDB    *leveldb.DB
	reusableBuffer        []byte
	logger                *logger.SugarLogger
	mu                    sync.Mutex
}

// Config holds the configuration of a block store
type Config struct {
	StoreDir string
	Logger   *logger.SugarLogger
}

// Open opens the store to maintains a chain of blocks
func Open(c *Config) (*Store, error) {
	exist, err := fileops.Exists(c.StoreDir)
	if err != nil {
		return nil, err
	}
	if !exist {
		return openNewStore(c)
	}

	partialStoreExist, err := isExistingStoreCreatedPartially(c.StoreDir)
	if err != nil {
		return nil, err
	}

	switch {
	case partialStoreExist:
		if err := fileops.RemoveAll(c.StoreDir); err != nil {
			return nil, errors.Wrap(err, "error while removing the existing partially created store")
		}

		return openNewStore(c)
	default:
		return openExistingStore(c)
	}
}

func isExistingStoreCreatedPartially(storeDir string) (bool, error) {
	empty, err := fileops.IsDirEmpty(storeDir)
	if err != nil || empty {
		return true, err
	}

	return fileops.Exists(filepath.Join(storeDir, underCreationFlag))
}

func openNewStore(c *Config) (*Store, error) {
	if err := fileops.CreateDir(c.StoreDir); err != nil {
		return nil, errors.WithMessagef(err, "error while creating directory [%s]", c.StoreDir)
	}

	underCreationFlagPath := filepath.Join(c.StoreDir, underCreationFlag)
	if err := fileops.CreateFile(underCreationFlagPath); err != nil {
		return nil, err
	}

	fileChunksDirPath := filepath.Join(c.StoreDir, fileChunksDirName)
	if err := fileops.CreateDir(fileChunksDirPath); err != nil {
		return nil, errors.WithMessagef(err, "error while creating directory [%s] for block file chunks", fileChunksDirPath)
	}

	blockIndexDBPath := filepath.Join(c.StoreDir, blockIndexDBName)
	blockHeaderDBPath := filepath.Join(c.StoreDir, blockHeaderDBName)
	txValidationInfoDBPath := filepath.Join(c.StoreDir, txValidationInfoDBName)

	file, err := openFileChunk(fileChunksDirPath, 0)
	if err != nil {
		return nil, err
	}

	indexDB, err := leveldb.OpenFile(blockIndexDBPath, &opt.Options{ErrorIfExist: true})
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating an index database")
	}

	headersDB, err := leveldb.OpenFile(blockHeaderDBPath, &opt.Options{ErrorIfExist: true})
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating a leveldb database to store the block headers")
	}

	txValidationInfoDB, err := leveldb.OpenFile(txValidationInfoDBPath, &opt.Options{ErrorIfExist: true})
	if err != nil {
		return nil, errors.WithMessage(err, "error while creating a leveldb database to store the transaction validation info")
	}

	if err := fileops.Remove(underCreationFlagPath); err != nil {
		return nil, errors.WithMessagef(err, "error while removing the under creation flag [%s]", underCreationFlagPath)
	}

	return &Store{
		fileChunksDirPath:     fileChunksDirPath,
		currentFileChunk:      file,
		currentOffset:         0,
		currentChunkNum:       0,
		lastCommittedBlockNum: 0,
		blockIndexDB:          indexDB,
		blockHeaderDB:         headersDB,
		txValidationInfoDB:    txValidationInfoDB,
		reusableBuffer:        make([]byte, binary.MaxVarintLen64),
		logger:                c.Logger,
	}, nil
}

func openExistingStore(c *Config) (*Store, error) {
	fileChunksDirPath := filepath.Join(c.StoreDir, fileChunksDirName)
	blockIndexDBPath := filepath.Join(c.StoreDir, blockIndexDBName)
	blockHeaderDBPath := filepath.Join(c.StoreDir, blockHeaderDBName)
	txValidationInfoDBPath := filepath.Join(c.StoreDir, txValidationInfoDBName)

	currentFileChunk, currentChunkNum, err := findAndOpenLastFileChunk(fileChunksDirPath)
	if err != nil {
		return nil, err
	}

	chunkFileInfo, err := currentFileChunk.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "error while getting the metadata of file [%s]", currentFileChunk.Name())
	}

	indexDB, err := leveldb.OpenFile(blockIndexDBPath, &opt.Options{ErrorIfMissing: true})
	if err != nil {
		return nil, errors.WithMessage(err, "error while opening the existing leveldb file for the block index")
	}

	headersDB, err := leveldb.OpenFile(blockHeaderDBPath, &opt.Options{ErrorIfMissing: true})
	if err != nil {
		return nil, errors.WithMessage(err, "error while opening the existing leveldb file for the block headers")
	}

	txValidationInfoDB, err := leveldb.OpenFile(txValidationInfoDBPath, &opt.Options{ErrorIfMissing: true})
	if err != nil {
		return nil, errors.WithMessage(err, "error while opening the existing leveldb file for the transaction validation info")
	}

	s := &Store{
		fileChunksDirPath:  fileChunksDirPath,
		currentFileChunk:   currentFileChunk,
		currentOffset:      chunkFileInfo.Size(),
		currentChunkNum:    currentChunkNum,
		blockIndexDB:       indexDB,
		blockHeaderDB:      headersDB,
		txValidationInfoDB: txValidationInfoDB,
		reusableBuffer:     make([]byte, binary.MaxVarintLen64),
		logger:             c.Logger,
	}
	return s, s.recover()
}

func (s *Store) recover() error {
	lastBlockNumberInIndex, lastBlockLocation, err := s.getLastBlockLocationInIndex()
	if err != nil {
		return err
	}

	var startBlockLocation *BlockLocation
	switch lastBlockLocation {
	case nil:
		startBlockLocation = &BlockLocation{
			FileChunkNum: s.currentChunkNum,
			Offset:       0,
		}
	default:
		startBlockLocation = &BlockLocation{
			FileChunkNum: lastBlockLocation.FileChunkNum,
			Offset:       lastBlockLocation.Offset + lastBlockLocation.Length,
		}
	}

	chunkFileStream, err := newBlockfileStream(s.logger, s.fileChunksDirPath, startBlockLocation)
	if err != nil {
		return err
	}
	defer func() {
		if err := chunkFileStream.close(); err != nil {
			s.logger.Warn(err.Error())
		}
	}()

	nextBlockAndLocation, err := chunkFileStream.nextBlockWithLocation()
	switch {
	// Scenario 1: no partial block write to file chunk and the block index
	// DB is sync with the file-based block store. To keep the recovery logic simple,
	// we reply the last block onto the index DB, block header DB and validationInfo
	// DB though it might be already in sync with the file-based block store.
	case nextBlockAndLocation == nil && err == nil:
		if lastBlockNumberInIndex == 0 {
			return nil
		}

		s.lastCommittedBlockNum = lastBlockNumberInIndex
		if s.currentChunkNum-1 == lastBlockLocation.FileChunkNum {
			// if a node has failed just after creating a new file chunk, currentChunk
			// would point to the empty file only during the restart. We need move to
			// the previous file chunk and delete the empty file
			newChunkFilePath := constructBlockFileChunkPath(s.fileChunksDirPath, s.currentChunkNum)

			if err := s.moveToChunk(lastBlockLocation.FileChunkNum); err != nil {
				return err
			}
			return fileops.RemoveAll(newChunkFilePath)
		}
		s.currentOffset = lastBlockLocation.Offset + lastBlockLocation.Length

		block, err := s.Get(lastBlockNumberInIndex)
		if err != nil {
			return err
		}

		if err = s.storeMetadataInDB(block, lastBlockLocation); err != nil {
			return err
		}

		return nil

	// Scenario 2: no partial block write to file chunk but the block index
	// DB is NOT in sync with the file-based block store. Here, we need to
	// reply the last block onto the block index DB, block header DB, and
	// validationInfo DB.
	case nextBlockAndLocation != nil && err == nil:
		secondNextBlockAndLocation, err := chunkFileStream.nextBlockWithLocation()
		if err != nil {
			return err
		}
		if secondNextBlockAndLocation != nil {
			return errors.Errorf("the block store can have exactly one fully committed block " +
				"which is not indexed or exaclty one partially written block. Any other case is " +
				"an unexpected behavior")
		}

		location := &BlockLocation{
			FileChunkNum: nextBlockAndLocation.fileChunkNum,
			Offset:       nextBlockAndLocation.blockStartOffset,
			Length:       nextBlockAndLocation.blockEndOffset - nextBlockAndLocation.blockStartOffset,
		}

		if err = s.storeMetadataInDB(nextBlockAndLocation.block, location); err != nil {
			return err
		}

		s.lastCommittedBlockNum = nextBlockAndLocation.block.Header.BaseHeader.Number
		s.currentOffset = nextBlockAndLocation.blockEndOffset
		return nil

	// Scenario 3: partial block write to file chunk. This can occur when the node
	// failure happened during the block append operation to the file chunk. We just
	// need to discard the partial write by setting current offset appropriately and
	// then truncating the file. No need to reply the block onto other DBs as they
	// all would be in sync with the last complete block
	case nextBlockAndLocation == nil && err == ErrUnexpectedEndOfBlockfile:
		s.lastCommittedBlockNum = lastBlockNumberInIndex
		s.currentOffset = lastBlockLocation.Offset + lastBlockLocation.Length

		if s.currentChunkNum-1 == lastBlockLocation.FileChunkNum {
			// if a node has failed just after creating a new file chunk and appending
			// a block partially to it, currentChunk would point to the new file only
			// after the restart. We need move to the previous chunk and delete the new
			// file that has the partially appended block
			newChunkFilePath := constructBlockFileChunkPath(s.fileChunksDirPath, s.currentChunkNum)

			if err := s.moveToChunk(lastBlockLocation.FileChunkNum); err != nil {
				return err
			}
			return fileops.RemoveAll(newChunkFilePath)
		}

		// for partial appends to the existing chunk file, we can simply truncate
		return fileops.Truncate(s.currentFileChunk, s.currentOffset)

	// internal error will result in a panic at the caller
	case nextBlockAndLocation == nil && err != nil:
		return errors.WithMessage(err, "error while recovering block store")
	}

	return nil
}

func (s *Store) getLastBlockLocationInIndex() (uint64, *BlockLocation, error) {
	itr := s.blockIndexDB.NewIterator(&util.Range{}, &opt.ReadOptions{})
	if err := itr.Error(); err != nil {
		return 0, nil, errors.Wrap(err, "error while finding the last committed block number in the index")
	}
	if !itr.Last() {
		return 0, nil, nil
	}

	key := itr.Key()
	val := itr.Value()

	blockNumber, _, err := decodeOrderPreservingVarUint64(key)
	if err != nil {
		return 0, nil, errors.Wrap(err, "error while decoding the last block index key")
	}

	blockLocation := &BlockLocation{}
	if err := proto.Unmarshal(val, blockLocation); err != nil {
		return 0, nil, errors.Wrap(err, "error while unmarshaling block location")
	}

	return blockNumber, blockLocation, nil
}

// Close closes the store
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.currentFileChunk.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the store")
	}

	if err := s.blockIndexDB.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the block index database")
	}

	if err := s.blockHeaderDB.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the block headers database")
	}

	if err := s.txValidationInfoDB.Close(); err != nil {
		return errors.WithMessage(err, "error while closing the tx validation info database")
	}

	return nil
}

func (s *Store) moveToChunk(chunkNum uint64) error {
	if err := s.currentFileChunk.Close(); err != nil {
		return err
	}

	s.currentChunkNum = chunkNum
	var err error
	s.currentFileChunk, err = openFileChunk(s.fileChunksDirPath, s.currentChunkNum)
	if err != nil {
		return err
	}
	s.currentOffset = 0

	return nil
}

func openFileChunk(dir string, chunkNum uint64) (*os.File, error) {
	path := constructBlockFileChunkPath(dir, chunkNum)
	file, err := fileops.OpenFile(path, 0644)
	if err != nil {
		return nil, errors.WithMessagef(err, "error while opening the file chunk")
	}

	return file, nil
}

func constructBlockFileChunkPath(dir string, chunkNum uint64) string {
	chunkName := fmt.Sprintf("%s%d", chunkPrefix, chunkNum)
	return filepath.Join(dir, chunkName)
}

func findAndOpenLastFileChunk(fileChunksDirPath string) (*os.File, uint64, error) {
	files, err := ioutil.ReadDir(fileChunksDirPath)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "error while listing file chunks in [%s]", fileChunksDirPath)
	}

	lastChunkNum := uint64(len(files) - 1)
	lastFileChunk, err := openFileChunk(fileChunksDirPath, lastChunkNum)
	if err != nil {
		return nil, 0, err
	}

	return lastFileChunk, lastChunkNum, nil
}
