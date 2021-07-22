// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockcreator

import (
	"github.com/IBM-Blockchain/bcdb-server/internal/blockstore"
	ierrors "github.com/IBM-Blockchain/bcdb-server/internal/errors"
	"github.com/IBM-Blockchain/bcdb-server/internal/queue"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/pkg/errors"
)

//go:generate counterfeiter -o mocks/replicator.go --fake-name Replicator . Replicator

// Replicator is the input to the block replication layer.
type Replicator interface {
	Submit(block *types.Block) error
}

// BlockCreator uses transactions batch queue to construct the
// block and stores the created block in the block queue
type BlockCreator struct {
	txBatchQueue                *queue.Queue
	blockReplicator             Replicator
	nextBlockNumber             uint64
	previousBlockHeaderBaseHash []byte
	blockStore                  *blockstore.Store
	started                     chan struct{}
	stop                        chan struct{}
	stopped                     chan struct{}
	logger                      *logger.SugarLogger
}

// Config holds the configuration information required to initialize the
// block creator
type Config struct {
	TxBatchQueue *queue.Queue
	BlockStore   *blockstore.Store
	Logger       *logger.SugarLogger
}

// New creates a new block assembler
func New(conf *Config) (*BlockCreator, error) {
	height, err := conf.BlockStore.Height()
	if err != nil {
		return nil, err
	}

	lastBlockBaseHash, err := conf.BlockStore.GetBaseHeaderHash(height)
	if err != nil {
		return nil, err
	}
	return &BlockCreator{
		txBatchQueue:                conf.TxBatchQueue,
		nextBlockNumber:             height + 1,
		logger:                      conf.Logger,
		blockStore:                  conf.BlockStore,
		started:                     make(chan struct{}),
		stop:                        make(chan struct{}),
		stopped:                     make(chan struct{}),
		previousBlockHeaderBaseHash: lastBlockBaseHash,
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

			blkNum := b.nextBlockNumber

			baseHeader, err := b.createBaseHeader(blkNum)
			if err != nil {
				b.logger.Panicf("Error while filling block header, possible problems with block indexes {%v}", err)
			}
			block := &types.Block{
				Header: &types.BlockHeader{
					BaseHeader: baseHeader,
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

			err = b.blockReplicator.Submit(block)
			switch err.(type) {
			case nil:
				// All is well
			case *ierrors.ClosedError:
				// This may happen when shutting down the server. 'continue' will eventually pick up the stop signal.
				b.logger.Warnf("block submission to block-replicator failed, dropping block, shutting down, because: %s", err)
				continue
			case *ierrors.NotLeaderError:
				b.logger.Warnf("block submission to block-replicator failed, because this node is not a leader: %s", err)
				//TODO reject or redirect all TXs in the block via the pending-tx component.
				// If there is another leader then redirect, else reject.
				// This will drain the pipeline and eventually there will be no more transactions coming in.
				// See issue:
				// https://github.ibm.com/blockchaindb/server/issues/401
				continue

			default:
				b.logger.Panicf("block submission to block-replicator failed: %v", err)
			}

			h, err := blockstore.ComputeBlockBaseHash(block)
			if err != nil {
				b.logger.Panicf("Error calculating block hash {%v}", err)
			}
			b.previousBlockHeaderBaseHash = h
			b.nextBlockNumber++
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

func (b *BlockCreator) createBaseHeader(blockNum uint64) (*types.BlockHeaderBase, error) {
	baseHeader := &types.BlockHeaderBase{
		Number:                 blockNum,
		PreviousBaseHeaderHash: b.previousBlockHeaderBaseHash,
	}

	if blockNum > 1 {
		lastCommittedBlockNum, err := b.blockStore.Height()
		if err != nil {
			return nil, err
		}
		lastCommittedBlockHash, err := b.blockStore.GetHash(lastCommittedBlockNum)
		if err != nil {
			return nil, err
		}
		if lastCommittedBlockHash == nil && !(lastCommittedBlockNum == 0) {
			return nil, errors.Errorf("can't get hash of last committed block {%d}", lastCommittedBlockNum)
		}
		baseHeader.LastCommittedBlockHash = lastCommittedBlockHash
		baseHeader.LastCommittedBlockNum = lastCommittedBlockNum
	}
	return baseHeader, nil
}
