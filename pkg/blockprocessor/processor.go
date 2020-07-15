package blockprocessor

import (
	"log"

	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/committer"
	"github.ibm.com/blockchaindb/server/pkg/queue"
	"github.ibm.com/blockchaindb/server/pkg/txisolation"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

// ValidatorAnValidatorAndCommitter holds block validator and committer
type ValidatorAndCommitter struct {
	blockQueue *queue.Queue
	validator  *txisolation.Validator
	committer  *committer.Committer
}

// NewValidatorAndCommitter creates a ValidatorAndCommitter
func NewValidatorAndCommitter(blockQueue *queue.Queue, db worldstate.DB) *ValidatorAndCommitter {
	p := &ValidatorAndCommitter{
		blockQueue: blockQueue,
		validator:  txisolation.NewValidator(db),
		committer:  committer.NewCommitter(db),
	}
	return p
}

// Run runs validator and committer
func (p *ValidatorAndCommitter) Run() {
	for {
		block := p.blockQueue.Dequeue().(*api.Block)

		validationInfo, err := p.validator.ValidateBlock(block)
		if err != nil {
			panic(err)
		}

		if err = p.committer.Commit(block, validationInfo); err != nil {
			panic(err)
		}
		log.Printf("validated and committed block %d\n", block.Header.Number)
	}
}
