package blockprocessor

import (
	"log"

	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/queue"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

// BlockProcessor holds block validator and committer
type BlockProcessor struct {
	blockQueue *queue.Queue
	validator  *validator
	committer  *committer
}

// New creates a ValidatorAndCommitter
func New(blockQueue *queue.Queue, db worldstate.DB) *BlockProcessor {
	return &BlockProcessor{
		blockQueue: blockQueue,
		validator:  newValidator(db),
		committer:  newCommitter(db),
	}
}

// Run runs validator and committer
func (b *BlockProcessor) Run() {
	for {
		block := b.blockQueue.Dequeue().(*types.Block)

		validationInfo, err := b.validator.validateBlock(block)
		if err != nil {
			panic(err)
		}

		if err = b.committer.commitBlock(block, validationInfo); err != nil {
			panic(err)
		}
		log.Printf("validated and committed block %d\n", block.Header.Number)
	}
}
