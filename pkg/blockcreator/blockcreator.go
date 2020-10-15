package blockcreator

import (
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

const (
	skipListBase = uint64(2)
)

// BlockCreator uses transactions batch queue to construct the
// block and stores the created block in the block queue
type BlockCreator struct {
	txBatchQueue    *queue.Queue
	blockQueue      *queue.Queue
	nextBlockNumber uint64
	blockStore      *blockstore.Store
	logger          *logger.SugarLogger
	blockHashCache  *hashCache
}

// Config holds the configuration information required to initialize the
// block creator
type Config struct {
	TxBatchQueue    *queue.Queue
	BlockQueue      *queue.Queue
	NextBlockNumber uint64
	BlockStore      *blockstore.Store
	Logger          *logger.SugarLogger
}

// New creates a new block assembler
func New(conf *Config) *BlockCreator {
	return &BlockCreator{
		txBatchQueue:    conf.TxBatchQueue,
		blockQueue:      conf.BlockQueue,
		nextBlockNumber: conf.NextBlockNumber,
		logger:          conf.Logger,
		blockStore:      conf.BlockStore,
		blockHashCache:  newCache(conf.BlockQueue.Capacity() + 1),
	}
}

// Run runs the block assembler in an infinite loop
func (b *BlockCreator) Run() {
	for {
		txBatch := b.txBatchQueue.Dequeue()

		blkNum := b.nextBlockNumber

		header, err := b.createHeader(blkNum)
		if err != nil {
			b.logger.Panicf("Error while filling block header, possible problems with block indexes {%v}", err)
		}
		block := &types.Block{
			Header: header,
		}

		switch txBatch.(type) {
		case *types.Block_DataTxEnvelopes:
			block.Payload = txBatch.(*types.Block_DataTxEnvelopes)
			b.logger.Debugf("created block %d with %d data transactions\n",
				blkNum,
				len(txBatch.(*types.Block_DataTxEnvelopes).DataTxEnvelopes.Envelopes),
			)

		case *types.Block_UserAdministrationTxEnvelope:
			block.Payload = txBatch.(*types.Block_UserAdministrationTxEnvelope)
			b.logger.Debugf("created block %d with an user administrative transaction", blkNum)

		case *types.Block_ConfigTxEnvelope:
			block.Payload = txBatch.(*types.Block_ConfigTxEnvelope)
			b.logger.Debugf("created block %d with a cluster config administrative transaction", blkNum)

		case *types.Block_DBAdministrationTxEnvelope:
			block.Payload = txBatch.(*types.Block_DBAdministrationTxEnvelope)
			b.logger.Debugf("created block %d with a DB administrative transaction", blkNum)
		}

		b.blockQueue.Enqueue(block)
		h, err := blockstore.ComputeBlockHash(block)
		if err != nil {
			b.logger.Panicf("Error calculating block hash {%v}", err)
		}
		b.blockHashCache.Put(blkNum, h)
		b.nextBlockNumber++
	}
}

func skipListHeight(blockNum uint64) uint64 {
	if blockNum%skipListBase != 0 {
		return 1
	}
	return 1 + skipListHeight(blockNum/skipListBase)
}

func skipListLinks(blockNum uint64) []uint64 {
	links := make([]uint64, 0)
	if blockNum > 1 {
		distance := uint64(1)
		for i := uint64(0); i < skipListHeight(blockNum-1); i++ {
			index := blockNum - distance
			links = append(links, index)
			distance *= skipListBase
		}
	}
	return links
}

func (b *BlockCreator) createHeader(blockNum uint64) (*types.BlockHeader, error) {
	header := &types.BlockHeader{
		Number:          blockNum,
		SkipchainHashes: make([][]byte, 0),
	}

	for _, linkedBlockNum := range skipListLinks(blockNum) {
		hash, err := b.blockStore.GetHash(linkedBlockNum)
		if err != nil {
			return nil, err
		}
		if hash == nil {
			if hash = b.blockHashCache.Get(linkedBlockNum); hash == nil {
				return nil, errors.Errorf("can't get hash for block {%d}, not from ledger, nor from cache", blockNum)
			}
		}
		header.SkipchainHashes = append(header.SkipchainHashes, hash)
	}
	return header, nil
}

type hashCache struct {
	cache  map[uint64][]byte
	size   int
	minKey uint64
}

func newCache(size int) *hashCache {
	return &hashCache{
		cache: make(map[uint64][]byte),
		size:  size,
	}
}

func (h *hashCache) Get(key uint64) []byte {
	return h.cache[key]
}

func (h *hashCache) Put(key uint64, val []byte) {
	if len(h.cache) == h.size {
		minKey := key - uint64(h.size)
		delete(h.cache, minKey)
	}
	h.cache[key] = val
}
