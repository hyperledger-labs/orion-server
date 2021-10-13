// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockstore

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"

	"github.com/hyperledger-labs/orion-server/internal/fileops"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
)

// ErrUnexpectedEndOfBlockfile error used to indicate an unexpected end of a file segment
// this can happen mainly if a crash occurs during appending a block and partial block contents
// get written towards the end of the file
var ErrUnexpectedEndOfBlockfile = errors.New("unexpected end of blockfile")

// blockfileStream reads blocks sequentially from chunk files.
// It starts from the given offset and can traverse till the end of the file and
// move to the next file to repeat the same till it reaches the end of the last
// chunk file
type blockfileStream struct {
	filePathDir    string
	fileChunkNum   uint64
	file           *os.File
	reader         *bufio.Reader
	currentOffset  int64
	remainingBytes int64
	logger         *logger.SugarLogger
}

type blockAndLocation struct {
	block            *types.Block
	fileChunkNum     uint64
	blockStartOffset int64
	blockEndOffset   int64
}

func newBlockfileStream(logger *logger.SugarLogger, rootDir string, startLocation *BlockLocation) (*blockfileStream, error) {
	filePath := constructBlockFileChunkPath(rootDir, startLocation.FileChunkNum)
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "error opening block file %s", filePath)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		if closeErr := file.Close(); closeErr != nil {
			logger.Warn(closeErr, "error while closing file "+filePath)
		}
		return nil, errors.Wrap(err, "error getting block file stat")
	}

	if _, err := file.Seek(startLocation.Offset, 0); err != nil {
		if closeErr := file.Close(); closeErr != nil {
			logger.Warn(closeErr, "error while closing file "+filePath)
		}
		return nil, errors.Wrap(err, "error seeking block file "+filePath+" to the offset 0")
	}

	return &blockfileStream{
		filePathDir:    rootDir,
		fileChunkNum:   startLocation.FileChunkNum,
		file:           file,
		reader:         bufio.NewReader(file),
		currentOffset:  startLocation.Offset,
		remainingBytes: fileInfo.Size() - startLocation.Offset,
		logger:         logger,
	}, nil
}

func (s *blockfileStream) nextBlockWithLocation() (*blockAndLocation, error) {
	if s.remainingBytes == 0 {
		moved, err := s.moveToNextFileChunkIfExist()
		if err != nil {
			return nil, err
		}
		if !moved {
			return nil, nil
		}
	}

	startOffsetOfNextBlock := s.currentOffset
	blockSize, err := s.readNextBlockSize()
	if err != nil {
		return nil, err
	}

	if blockSize > s.remainingBytes {
		return nil, ErrUnexpectedEndOfBlockfile
	}

	blockBytes := make([]byte, blockSize)
	if _, err := io.ReadFull(s.reader, blockBytes); err != nil {
		return nil, errors.Wrap(err, "error while reading block from the file")
	}

	s.currentOffset += blockSize
	s.remainingBytes -= blockSize

	marshaledBlock, err := snappy.Decode(nil, blockBytes)
	if err != nil {
		return nil, errors.Wrap(err, "error while decoding the block using snappy compression")
	}

	block := &types.Block{}
	if err := proto.Unmarshal(marshaledBlock, block); err != nil {
		return nil, errors.Wrap(err, "error while unmarshalling the block")
	}

	return &blockAndLocation{
		block:            block,
		fileChunkNum:     s.fileChunkNum,
		blockStartOffset: startOffsetOfNextBlock,
		blockEndOffset:   s.currentOffset,
	}, nil
}

func (s *blockfileStream) moveToNextFileChunkIfExist() (bool, error) {
	nextFileChunkPath := constructBlockFileChunkPath(s.filePathDir, s.fileChunkNum+1)
	exist, err := fileops.Exists(nextFileChunkPath)
	if err != nil {
		return false, err
	}
	if !exist {
		return false, nil
	}

	file, err := os.OpenFile(nextFileChunkPath, os.O_RDONLY, 0600)
	if err != nil {
		return false, errors.Wrapf(err, "error opening block file %s", nextFileChunkPath)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		if closeErr := file.Close(); closeErr != nil {
			s.logger.Warn(closeErr, "error while closing block file "+nextFileChunkPath)
		}
		return false, errors.Wrap(err, "error getting block file stat")
	}

	if fileInfo.Size() == 0 {
		if closeErr := file.Close(); closeErr != nil {
			s.logger.Warn(closeErr, "error while closing block file "+nextFileChunkPath)
		}
		return false, nil
	}

	if err := s.file.Close(); err != nil {
		return false, errors.Wrap(err, "error while closing block file "+s.file.Name())
	}

	s.fileChunkNum++
	s.file = file
	s.reader = bufio.NewReader(file)
	s.currentOffset = 0
	s.remainingBytes = fileInfo.Size()

	return true, nil
}

func (s *blockfileStream) readNextBlockSize() (int64, error) {
	moreContentAvailable := true

	peekBytes := binary.MaxVarintLen64
	if s.remainingBytes < int64(peekBytes) {
		peekBytes = int(s.remainingBytes)
		moreContentAvailable = false
	}

	lenBytes, err := s.reader.Peek(peekBytes)
	if err != nil {
		s.logger.Debug(s.remainingBytes, peekBytes)
		return 0, errors.Wrapf(err, "error peeking [%d] bytes from block file", peekBytes)
	}

	length, n := proto.DecodeVarint(lenBytes)
	if n == 0 {
		if !moreContentAvailable {
			return 0, ErrUnexpectedEndOfBlockfile
		}
		return 0, errors.Errorf("error in decoding varint bytes [%#v] representing the block length", lenBytes)
	}

	// skip the bytes representing the block size
	if _, err = s.reader.Discard(n); err != nil {
		return 0, errors.Wrapf(err, "error discarding [%d] bytes", n)
	}

	s.remainingBytes -= int64(n)
	s.currentOffset += int64(n)

	return int64(length), nil
}

func (s *blockfileStream) close() error {
	return errors.Wrap(s.file.Close(), "error while closing block file "+s.file.Name())
}
