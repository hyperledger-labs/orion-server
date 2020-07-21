package blockcreator

import (
	"log"

	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/queue"
)

type Assembler struct {
	txBatchQueue *queue.Queue
	blockQueue   *queue.Queue
	blockNumber  uint64
}

func NewAssembler(txBatchQueue, blockQueue *queue.Queue) *Assembler {
	return &Assembler{
		txBatchQueue: txBatchQueue,
		blockQueue:   blockQueue,
		blockNumber:  1, // once the blockstore is added, we need to
		// retrieve the last committed block number
	}
}

func (a *Assembler) Run() {
	for {
		txBatch := a.txBatchQueue.Dequeue().([]*types.TransactionEnvelope)

		block := &types.Block{
			Header: &types.BlockHeader{
				Number: a.blockNumber,
			},
			TransactionEnvelopes: txBatch,
		}

		a.blockQueue.Enqueue(block)
		log.Printf("created block %d with %d transactions\n", a.blockNumber, len(txBatch))
		a.blockNumber++
	}
}
