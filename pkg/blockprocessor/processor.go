package blockprocessor

import (
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/logger"
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
	logger     *logger.SugarLogger
}

// Config holds the configuration information needed to bootstrap the
// block processor
type Config struct {
	BlockQueue *queue.Queue
	BlockStore *blockstore.Store
	DB         worldstate.DB
	Logger     *logger.SugarLogger
}

// New creates a ValidatorAndCommitter
func New(conf *Config) *BlockProcessor {
	return &BlockProcessor{
		blockQueue: conf.BlockQueue,
		blockStore: conf.BlockStore,
		validator:  newValidator(conf),
		committer:  newCommitter(conf),
		logger:     conf.Logger,
	}
}

// Run runs validator and committer
func (b *BlockProcessor) Run(stopBlockProcessing chan struct{}) {
	b.logger.Debug("starting block processor")
	if err := b.recoverWorldStateDBIfNeeded(); err != nil {
		panic(errors.WithMessage(err, "error while recovering node"))
	}

	for {
		select {
		default:
			block := b.blockQueue.Dequeue().(*types.Block)

			b.logger.Debugf("validating and committing block %d", block.GetHeader().GetBaseHeader().GetNumber())
			validationInfo, err := b.validator.validateBlock(block)
			if err != nil {
				panic(err)
			}

			block.Header.ValidationInfo = validationInfo

			if err := b.blockStore.UpdateBlock(block); err != nil {
				panic(err)
			}

			if err = b.committer.commitBlock(block); err != nil {
				panic(err)
			}
			b.logger.Debugf("validated and committed block %d\n", block.GetHeader().GetBaseHeader().GetNumber())

		case <-stopBlockProcessing:
			b.logger.Info("stopping block processing")
			return
		}
	}
}

func (b *BlockProcessor) recoverWorldStateDBIfNeeded() error {
	blockStoreHeight, err := b.blockStore.Height()
	if err != nil {
		return err
	}

	stateDBHeight, err := b.committer.db.Height()
	if err != nil {
		return err
	}

	switch {
	case stateDBHeight == blockStoreHeight:
		return nil
	case stateDBHeight > blockStoreHeight:
		return errors.Errorf(
			"the height of state database [%d] is higher than the height of block store [%d]. The node cannot be recovered",
			stateDBHeight,
			blockStoreHeight,
		)
	case blockStoreHeight-stateDBHeight > 1:
		// Note: when we support rollback, the different in height can be more than 1.
		// For now, a failure can occur before committing the block to the block store or after.
		// As a result, the height of block store would be at most 1 higher than the state database
		// height.
		return errors.Errorf(
			"the difference between the height of the block store [%d] and the state database [%d] cannot be greater than 1 block. The node cannot be recovered",
			blockStoreHeight,
			stateDBHeight,
		)
	case blockStoreHeight-stateDBHeight == 1:
		block, err := b.blockStore.Get(blockStoreHeight)
		if err != nil {
			return err
		}

		if err := b.committer.commitToStateDB(block); err != nil {
			return err
		}
	}

	return nil
}
