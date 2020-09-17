package blockcreator

import (
	"log"

	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

// BlockCreator uses transactions batch queue to construct the
// block and stores the created block in the block queue
type BlockCreator struct {
	txBatchQueue    *queue.Queue
	blockQueue      *queue.Queue
	nextBlockNumber uint64
}

// Config holds the configuration information required to initialize the
// block creator
type Config struct {
	TxBatchQueue    *queue.Queue
	BlockQueue      *queue.Queue
	NextBlockNumber uint64
}

// New creates a new block assembler
func New(conf *Config) *BlockCreator {
	return &BlockCreator{
		txBatchQueue:    conf.TxBatchQueue,
		blockQueue:      conf.BlockQueue,
		nextBlockNumber: conf.NextBlockNumber,
	}
}

// Run runs the block assembler in an infinte loop
func (b *BlockCreator) Run() {
	for {
		txBatch := b.txBatchQueue.Dequeue()

		blkNum := b.nextBlockNumber
		block := &types.Block{
			Header: &types.BlockHeader{
				Number: blkNum,
			},
		}

		switch txBatch.(type) {
		case *types.Block_DataTxEnvelopes:
			block.Payload = txBatch.(*types.Block_DataTxEnvelopes)
			log.Printf("created block %d with %d data transactions\n",
				blkNum,
				len(txBatch.(*types.Block_DataTxEnvelopes).DataTxEnvelopes.Envelopes),
			)

		case *types.Block_UserAdministrationTxEnvelope:
			block.Payload = txBatch.(*types.Block_UserAdministrationTxEnvelope)
			log.Printf("created block %d with an user administrative transaction", blkNum)

		case *types.Block_ConfigTxEnvelope:
			block.Payload = txBatch.(*types.Block_ConfigTxEnvelope)
			log.Printf("created block %d with a cluster config administrative transaction", blkNum)

		case *types.Block_DBAdministrationTxEnvelope:
			block.Payload = txBatch.(*types.Block_DBAdministrationTxEnvelope)
			log.Printf("created block %d with a DB administrative transaction", blkNum)
		}

		b.blockQueue.Enqueue(block)
		b.nextBlockNumber++
	}
}
