package blockstore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/fileops"
)

// Commit commits the block to the block store
func (s *Store) Commit(block *types.Block) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if block == nil {
		return errors.New("block cannot be nil")
	}

	blockNumber := block.GetHeader().GetNumber()
	if blockNumber != s.lastCommittedBlockNum+1 {
		return fmt.Errorf(
			"expected block number [%d] but received [%d]",
			s.lastCommittedBlockNum+1,
			blockNumber,
		)
	}

	b, err := proto.Marshal(block)
	if err != nil {
		return errors.Wrapf(err, "error while marshaling block, %v", block)
	}

	encodedBlock := snappy.Encode(nil, b)
	n := binary.PutUvarint(s.reusableBuffer, uint64(len(encodedBlock)))
	content := append(s.reusableBuffer[:n], encodedBlock...)

	if !s.canCurrentFileChunkHold(len(content)) {
		if err := s.moveToNextFileChunk(); err != nil {
			return err
		}
	}

	return s.appendBlock(blockNumber, content)
}

func (s *Store) canCurrentFileChunkHold(toBeAddedBytesLength int) bool {
	return s.currentOffset+int64(toBeAddedBytesLength) < chunkSizeLimit
}

func (s *Store) moveToNextFileChunk() error {
	f, err := openFileChunk(s.fileChunksDirPath, s.currentChunkNum+1)
	if err != nil {
		return err
	}

	if err := s.currentFileChunk.Close(); err != nil {
		return errors.Wrapf(err, "error while closing the file %s", s.currentFileChunk.Name())
	}
	s.currentFileChunk = f
	s.currentChunkNum++
	s.currentOffset = 0

	return nil
}

func (s *Store) appendBlock(number uint64, content []byte) error {
	offsetBeforeWrite := s.currentOffset

	n, err := s.currentFileChunk.Write(content)
	if err == nil {
		s.currentOffset += int64(len(content))
		s.lastCommittedBlockNum = number
		return s.addIndexForBlock(number, offsetBeforeWrite)
	}

	if n > 0 {
		if err := fileops.Truncate(s.currentFileChunk, offsetBeforeWrite); err != nil {
			log.Println(err.Error())
		}
	}

	return errors.Wrapf(
		err,
		"error while writing the block to currentFileChunk [%s]",
		s.currentFileChunk.Name(),
	)
}

func (s *Store) addIndexForBlock(number uint64, offset int64) error {
	value, err := proto.Marshal(
		&BlockLocation{
			FileChunkNum: s.currentChunkNum,
			Offset:       offset,
		},
	)
	if err != nil {
		return errors.Wrap(err, "error while marshaling BlockLocation")
	}

	return s.blockIndex.Put(
		encodeOrderPreservingVarUint64(number),
		value,
		&opt.WriteOptions{
			Sync: true,
		},
	)
}

// Height returns the height of the block store, i.e., the last committed block number
func (s *Store) Height() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lastCommittedBlockNum, nil
}

// Get retuns the requested block
func (s *Store) Get(blockNumber uint64) (*types.Block, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if blockNumber > s.lastCommittedBlockNum {
		switch {
		case s.lastCommittedBlockNum == 0:
			return nil, errors.New("block store is empty")
		default:
			return nil, fmt.Errorf(
				"requested block number [%d] cannot be greater than the last committed block number [%d]",
				blockNumber,
				s.lastCommittedBlockNum,
			)
		}
	}

	location, err := s.getLocation(blockNumber)
	if err != nil {
		return nil, err
	}

	var f *os.File

	switch {
	case s.currentChunkNum == location.FileChunkNum:
		f = s.currentFileChunk
		offSet := s.currentOffset
		defer func() {
			s.currentOffset = offSet
		}()
	default:
		f, err = openFileChunk(s.fileChunksDirPath, location.FileChunkNum)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := f.Close(); err != nil {
				log.Printf("error while closing the file [%s]", f.Name())
			}
		}()
	}

	return readBlockFromFile(f, location)
}

func (s *Store) getLocation(blockNumber uint64) (*BlockLocation, error) {
	val, err := s.blockIndex.Get(encodeOrderPreservingVarUint64(blockNumber), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	blockLocation := &BlockLocation{}
	if err := proto.Unmarshal(val, blockLocation); err != nil {
		return nil, errors.Wrap(err, "error while unmarshaling block location")
	}

	return blockLocation, nil
}

func readBlockFromFile(f *os.File, location *BlockLocation) (*types.Block, error) {
	if _, err := f.Seek(location.Offset, 0); err != nil {
		return nil, errors.Wrap(err, "error while seeking")
	}

	bufReader := bufio.NewReader(f)
	blockSize, err := binary.ReadUvarint(bufReader)
	if err != nil {
		return nil, errors.Wrap(err, "error while reading the length of the stored block")
	}

	buf := make([]byte, blockSize)
	if _, err := io.ReadFull(bufReader, buf); err != nil {
		return nil, errors.Wrap(err, "error while reading block from the file")
	}

	marshaledBlock, err := snappy.Decode(nil, buf)
	if err != nil {
		return nil, errors.Wrap(err, "error while decoding the block using snappy compression")
	}

	block := &types.Block{}
	if err := proto.Unmarshal(marshaledBlock, block); err != nil {
		return nil, errors.Wrap(err, "error while unmarshaling the block")
	}

	return block, nil
}
