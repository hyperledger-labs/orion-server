package blockprocessor

import (
	"log"

	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/queue"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

// ValidatorAndCommitter holds block validator and committer
type ValidatorAndCommitter struct {
	blockQueue *queue.Queue
	validator  *validator
	committer  *committer
}

// NewValidatorAndCommitter creates a ValidatorAndCommitter
func NewValidatorAndCommitter(blockQueue *queue.Queue, db worldstate.DB) *ValidatorAndCommitter {
	c := &ValidatorAndCommitter{
		blockQueue: blockQueue,
		validator:  newValidator(db),
		committer:  newCommitter(db),
	}
	return c
}

// Run runs validator and committer
func (c *ValidatorAndCommitter) Run() {
	for {
		block := c.blockQueue.Dequeue().(*types.Block)

		validationInfo, err := c.validator.validateBlock(block)
		if err != nil {
			panic(err)
		}

		if err = c.committer.commitBlock(block, validationInfo); err != nil {
			panic(err)
		}
		log.Printf("validated and committed block %d\n", block.Header.Number)
	}
}
