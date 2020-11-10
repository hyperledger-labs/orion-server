package blockstore

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/fileops"
)

const (
	SkipListBase   = uint64(2)
	nonDataTxIndex = 0
)

// Commit commits the block to the block store
func (s *Store) Commit(block *types.Block) error {
	if block == nil {
		return errors.New("block cannot be nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	blockNumber := block.GetHeader().GetBaseHeader().GetNumber()
	if blockNumber != s.lastCommittedBlockNum+1 {
		return errors.Errorf(
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

	if err := s.appendBlock(blockNumber, content); err != nil {
		return err
	}

	if err = s.storeBlockValidationInfo(block); err != nil {
		return err
	}

	return s.storeBlockHeaders(blockNumber, block.Header)
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
			s.logger.Warn(err.Error())
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

	return s.blockIndexDB.Put(
		encodeOrderPreservingVarUint64(number),
		value,
		&opt.WriteOptions{
			Sync: true,
		},
	)
}

func (s *Store) UpdateBlock(block *types.Block) error {
	skipListHashes := make([][]byte, 0)

	for _, linkedBlockNum := range CalculateSkipListLinks(block.Header.GetBaseHeader().GetNumber()) {

		hash, err := s.GetHash(linkedBlockNum)
		if err != nil {
			return err
		}
		skipListHashes = append(skipListHashes, hash)
	}
	block.Header.SkipchainHashes = skipListHashes
	return nil
}

func skipListHeight(blockNum uint64) uint64 {
	if blockNum%SkipListBase != 0 {
		return 1
	}
	return 1 + skipListHeight(blockNum/SkipListBase)
}

func CalculateSkipListLinks(blockNum uint64) []uint64 {
	links := make([]uint64, 0)
	if blockNum > 1 {
		distance := uint64(1)
		for i := uint64(0); i < skipListHeight(blockNum-1); i++ {
			index := blockNum - distance
			links = append(links, index)
			distance *= SkipListBase
		}
	}
	return links
}

func (s *Store) storeBlockValidationInfo(block *types.Block) error {
	blockNum := block.Header.BaseHeader.Number
	var txID string

	switch block.Payload.(type) {
	case *types.Block_DataTxEnvelopes:
		dataTxs := block.GetDataTxEnvelopes().Envelopes
		updateBatch := &leveldb.Batch{}

		for txNum, tx := range dataTxs {
			key := []byte(tx.Payload.TxID)
			value, err := proto.Marshal(block.Header.ValidationInfo[txNum])
			if err != nil {
				return errors.Wrapf(err, "error while marshaling validation info of transaction %d in block %d", txNum, blockNum)
			}

			updateBatch.Put(key, value)
		}

		return s.txValidationInfoDB.Write(updateBatch, &opt.WriteOptions{Sync: true})

	case *types.Block_ConfigTxEnvelope:
		txID = block.GetConfigTxEnvelope().Payload.TxID

	case *types.Block_DBAdministrationTxEnvelope:
		txID = block.GetDBAdministrationTxEnvelope().Payload.TxID

	case *types.Block_UserAdministrationTxEnvelope:
		txID = block.GetUserAdministrationTxEnvelope().Payload.TxID

	default:
		return errors.Errorf("unknown block payload")
	}

	key := []byte(txID)
	value, err := proto.Marshal(block.Header.ValidationInfo[nonDataTxIndex])
	if err != nil {
		return errors.Wrapf(err, "error while marshaling validation info of non-data transaction in block %d", blockNum)
	}

	return s.txValidationInfoDB.Put(key, value, &opt.WriteOptions{Sync: true})
}

func (s *Store) storeBlockHeaders(number uint64, header *types.BlockHeader) error {
	blockHeaderBaseBytes, err := proto.Marshal(header.GetBaseHeader())
	if err != nil {
		return errors.Wrapf(err, "can't marshal block base header {%d, %v}", number, header)
	}

	blockHeaderBaseHash, err := crypto.ComputeSHA256Hash(blockHeaderBaseBytes)
	if err != nil {
		return errors.Wrapf(err, "can't calculate block base header hash {%d, %v}", number, header.GetBaseHeader())
	}

	blockHeaderBytes, err := proto.Marshal(header)
	if err != nil {
		return errors.Wrapf(err, "can't marshal block header {%d, %v}", number, header)
	}

	blockHash, err := crypto.ComputeSHA256Hash(blockHeaderBytes)
	if err != nil {
		return errors.Wrapf(err, "can't calculate block hash {%d, %v}", number, header)
	}

	if err = s.blockHeaderDB.Put(
		constructHeaderBaseHashKey(number),
		blockHeaderBaseHash,
		&opt.WriteOptions{
			Sync: true,
		},
	); err != nil {
		return errors.Wrapf(err, "can't create block base header hash index {%d, %v}", number, header)
	}

	if err = s.blockHeaderDB.Put(
		constructHeaderHashKey(number),
		blockHash,
		&opt.WriteOptions{
			Sync: true,
		},
	); err != nil {
		return errors.Wrapf(err, "can't create block hash index {%d, %v}", number, header)
	}

	if err = s.blockHeaderDB.Put(
		constructHeaderBytesKey(number),
		blockHeaderBytes,
		&opt.WriteOptions{
			Sync: true,
		},
	); err != nil {
		return errors.Wrapf(err, "can't create block hash index {%d, %v}", number, header)
	}

	if err = s.blockHeaderDB.Put(
		constructHeaderHashIndexKey(blockHash),
		encodeOrderPreservingVarUint64(number),
		&opt.WriteOptions{
			Sync: true,
		},
	); err != nil {
		return errors.Wrapf(err, "can't create block header by hash index {%d, %v}", number, header)
	}
	return nil
}

// Height returns the height of the block store, i.e., the last committed block number
func (s *Store) Height() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lastCommittedBlockNum, nil
}

// Get returns the requested block
func (s *Store) Get(blockNumber uint64) (*types.Block, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if blockNumber > s.lastCommittedBlockNum {
		switch {
		case s.lastCommittedBlockNum == 0:
			return nil, errors.New("block store is empty")
		default:
			return nil, errors.Errorf(
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
				s.logger.Warnf("error while closing the file [%s]", f.Name())
			}
		}()
	}

	return readBlockFromFile(f, location)
}

// GetHeader returns block header by block number, operation should be faster that regular Get,
// because it requires only one db access, without file reads
func (s *Store) GetHeader(blockNumber uint64) (*types.BlockHeader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, err := s.blockHeaderDB.Get(constructHeaderBytesKey(blockNumber), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, errors.Wrapf(err, "can't access block's %d hash", blockNumber)
	}

	blockHeader := &types.BlockHeader{}

	if err := proto.Unmarshal(val, blockHeader); err != nil {
		return nil, errors.Wrap(err, "error while unmarshalling block header")
	}
	return blockHeader, nil
}

// GetHash returns block hash by block number
func (s *Store) GetHash(blockNumber uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, err := s.blockHeaderDB.Get(constructHeaderHashKey(blockNumber), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, errors.Wrapf(err, "can't access block's %d hash", blockNumber)
	}
	return val, nil
}

// GetBaseHeaderHash returns block header base hash by block number
func (s *Store) GetBaseHeaderHash(blockNumber uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, err := s.blockHeaderDB.Get(constructHeaderBaseHashKey(blockNumber), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, errors.Wrapf(err, "can't access block's %d header base hash", blockNumber)
	}
	return val, nil
}

// GetHeaderByHash returns block header by block hash, used for travel in Merkle list or Merkle skip list
func (s *Store) GetHeaderByHash(blockHash []byte) (*types.BlockHeader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	blockNumBytes, err := s.blockHeaderDB.Get(constructHeaderHashIndexKey(blockHash), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, errors.Wrap(err, "can't access block's number by hash")
	}

	headerVal, err := s.blockHeaderDB.Get(append(headerBytesNs, blockNumBytes...), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, errors.Wrap(err, "can't access block's header by number")
	}
	blockHeader := &types.BlockHeader{}

	if err := proto.Unmarshal(headerVal, blockHeader); err != nil {
		return nil, errors.Wrap(err, "error while unmarshalling block header")
	}

	return blockHeader, nil
}

// DoesTxIDExist returns true if any of the committed block has a transaction with
// the given txID. Otherwise, it returns false
func (s *Store) DoesTxIDExist(txID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.txValidationInfoDB.Has([]byte(txID), &opt.ReadOptions{})
}

// GetValidationInfo returns the validation info associated with a given txID
func (s *Store) GetValidationInfo(txID string) (*types.ValidationInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	valInfoSerialized, err := s.txValidationInfoDB.Get([]byte(txID), &opt.ReadOptions{})
	if err != nil && err != leveldb.ErrNotFound {
		return nil, errors.Wrapf(err, "error while fetching validation info of txID [%s ]from the block store", txID)
	}

	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	valInfo := &types.ValidationInfo{}
	if err := proto.Unmarshal(valInfoSerialized, valInfo); err != nil {
		return nil, errors.Wrapf(err, "error while unmarshalling stored validation info of txID [%s]", txID)
	}

	return valInfo, nil
}

func (s *Store) getLocation(blockNumber uint64) (*BlockLocation, error) {
	val, err := s.blockIndexDB.Get(encodeOrderPreservingVarUint64(blockNumber), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}

	blockLocation := &BlockLocation{}
	if err := proto.Unmarshal(val, blockLocation); err != nil {
		return nil, errors.Wrap(err, "error while unmarshalling block location")
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
		return nil, errors.Wrap(err, "error while unmarshalling the block")
	}

	return block, nil
}

// ComputeBlockHash returns block hash. Currently block header hash is considered block hash, because it contains
// all crypto related information, like Merkle tree root(s) and Merkle list and skip list hashes.
func ComputeBlockHash(block *types.Block) ([]byte, error) {
	headerBytes, err := proto.Marshal(block.GetHeader())
	if err != nil {
		return nil, err
	}
	return crypto.ComputeSHA256Hash(headerBytes)
}

// ComputeBlockBaseHash returns block hash before all validation and state data was updated. Currently block header base hash
// is considered block hash, because it contains  all crypto related information, like Tx Merkle tree root
// and hash of previous block before validation as well
func ComputeBlockBaseHash(block *types.Block) ([]byte, error) {
	headerBytes, err := proto.Marshal(block.GetHeader().GetBaseHeader())
	if err != nil {
		return nil, err
	}
	return crypto.ComputeSHA256Hash(headerBytes)
}

func constructHeaderBaseHashKey(blockNum uint64) []byte {
	return append(headerBaseHashNs, encodeOrderPreservingVarUint64(blockNum)...)
}

func constructHeaderHashIndexKey(blockHash []byte) []byte {
	return append(headerHashToBlockNumNs, blockHash...)
}

func constructHeaderBytesKey(blockNum uint64) []byte {
	return append(headerBytesNs, encodeOrderPreservingVarUint64(blockNum)...)
}

func constructHeaderHashKey(blockNum uint64) []byte {
	return append(headerHashNs, encodeOrderPreservingVarUint64(blockNum)...)
}
