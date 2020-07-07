package blockprocessor

import (
	"github.ibm.com/blockchaindb/server/api"
	"github.ibm.com/blockchaindb/server/pkg/committer"
	"github.ibm.com/blockchaindb/server/pkg/queue"
	"github.ibm.com/blockchaindb/server/pkg/txisolation"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type Processor struct {
	blockQueue *queue.Queue
	validator  *txisolation.Validator
	committer  *committer.Committer
}

func NewProcessor(queue *queue.Queue, db worldstate.DB) *Processor {
	p := &Processor{
		blockQueue: queue,
		validator:  txisolation.NewValidator(db),
		committer:  committer.NewCommitter(db),
	}
	return p
}

func (p *Processor) Process() {
	for {
		block := p.blockQueue.Dequeue().(*api.Block)
		validationInfo, err := p.validator.ValidateBlock(block)
		if err != nil {
			panic(err)
		}
		if err = p.committer.Commit(block, validationInfo); err != nil {
			panic(err)
		}
	}
}
