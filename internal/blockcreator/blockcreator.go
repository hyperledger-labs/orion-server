// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockcreator

import (
	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/queue"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

//go:generate counterfeiter -o mocks/replicator.go --fake-name Replicator . Replicator

// Replicator is the input to the block replication layer.
type Replicator interface {
	Submit(block *types.Block) error
}

// BlockCreator uses transactions batch queue to construct a block proposal and submits the proposed block to the
// block-replicator. The block-replicator is in charge of numbering the blocks and setting the previous
// BlockHeaderBase hash.
type BlockCreator struct {
	txBatchQueue       *queue.Queue
	blockReplicator    Replicator
	pendingTxs         *queue.PendingTxs
	nextProposalNumber uint64 // this numbers the local blocks proposed throughout the life cycle of the node
	blockStore         *blockstore.Store

	started chan struct{}
	stop    chan struct{}
	stopped chan struct{}

	logger *logger.SugarLogger
}

// Config holds the configuration information required to initialize the
// block creator
type Config struct {
	TxBatchQueue *queue.Queue
	BlockStore   *blockstore.Store
	PendingTxs   *queue.PendingTxs
	Logger       *logger.SugarLogger
}

// New creates a new block assembler
func New(conf *Config) (*BlockCreator, error) {
	return &BlockCreator{
		txBatchQueue:       conf.TxBatchQueue,
		nextProposalNumber: 1,
		logger:             conf.Logger,
		blockStore:         conf.BlockStore,
		pendingTxs:         conf.PendingTxs,
		started:            make(chan struct{}),
		stop:               make(chan struct{}),
		stopped:            make(chan struct{}),
	}, nil
}

func (b *BlockCreator) RegisterReplicator(blockReplicator Replicator) {
	b.blockReplicator = blockReplicator
}

func BootstrapBlock(tx *types.ConfigTxEnvelope) (*types.Block, error) {
	block := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number: 1,
			},
		},
		Payload: &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: tx,
		},
	}

	return block, nil
}

// Start runs the block assembler in an infinite loop
func (b *BlockCreator) Start() {
	defer close(b.stopped)

	b.logger.Info("starting the block creator")
	close(b.started)
	for {
		select {
		case <-b.stop:
			b.logger.Info("stopping the block creator")
			return

		default:
			txBatch := b.txBatchQueue.Dequeue()
			if txBatch == nil {
				// when the queue is closed during the teardown/cleanup,
				// the dequeued txBatch would be nil.
				continue
			}

			blkNum := b.nextProposalNumber //Exact block numbering is done in replication
			block := &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: blkNum,
					},
				},
			}

			switch batch := txBatch.(type) {
			case *types.Block_DataTxEnvelopes:
				block.Payload = batch
				b.logger.Debugf("created block %d with %d data transactions\n",
					blkNum,
					len(batch.DataTxEnvelopes.Envelopes),
				)

			case *types.Block_UserAdministrationTxEnvelope:
				block.Payload = batch
				b.logger.Debugf("created block %d with an user administrative transaction", blkNum)

			case *types.Block_ConfigTxEnvelope:
				block.Payload = batch
				b.logger.Debugf("created block %d with a cluster config administrative transaction", blkNum)

			case *types.Block_DbAdministrationTxEnvelope:
				block.Payload = batch
				b.logger.Debugf("created block %d with a DB administrative transaction", blkNum)
			}

			err := b.blockReplicator.Submit(block)
			switch err.(type) {
			case nil:
				// All is well
			case *ierrors.ClosedError:
				// This may happen when shutting down the server. 'continue' will eventually pick up the stop signal.
				b.logger.Warnf("block submission to block-replicator failed, dropping block, shutting down, because: %s", err)
				continue

			case *ierrors.NotLeaderError:
				b.logger.Warnf("block submission to block-replicator failed, because this node is not a leader: %s", err)
				// Releasing with an error will reject or redirect all sync TXs in the block via the pending-tx component.
				// If there is another leader it will redirect, else reject. Async TXs will be removed.
				// This will drain the pipeline and eventually there will be no more transactions coming in.
				if txIDs, errID := utils.BlockPayloadToTxIDs(block.Payload); errID == nil {
					b.pendingTxs.ReleaseWithError(txIDs, err)
				} else {
					b.logger.Errorf("failed to extract TXIDs from block: %s", errID)
				}
				continue

			default:
				b.logger.Panicf("block submission to block-replicator failed: %v", err)
			}

			b.nextProposalNumber++
		}
	}
}

// WaitTillStart waits till the block creator is started
func (b *BlockCreator) WaitTillStart() {
	<-b.started
}

// Stop stops the block creator
func (b *BlockCreator) Stop() {
	b.txBatchQueue.Close()
	close(b.stop)
	<-b.stopped
}
