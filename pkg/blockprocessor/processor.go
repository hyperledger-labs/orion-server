package blockprocessor

import (
	"log"

	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
	"github.ibm.com/blockchaindb/server/pkg/queue"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

// BlockProcessor holds block validator and committer
type BlockProcessor struct {
	blockQueue *queue.Queue
	blockStore *blockstore.Store
	validator  *validator
	committer  *committer
}

// Config holds the configuration information needed to bootstrap the
// block processor
type Config struct {
	BlockQueue *queue.Queue
	BlockStore *blockstore.Store
	DB         worldstate.DB
}

// New creates a ValidatorAndCommitter
func New(conf *Config) *BlockProcessor {
	return &BlockProcessor{
		blockQueue: conf.BlockQueue,
		blockStore: conf.BlockStore,
		validator:  newValidator(conf),
		committer:  newCommitter(conf),
	}
}

// Run runs validator and committer
func (b *BlockProcessor) Run() {
	for {
		block := b.blockQueue.Dequeue().(*types.Block)

		log.Printf("validating and commit block %d", block.GetHeader().GetNumber())
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
