// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"sync"

	ierrors "github.ibm.com/blockchaindb/server/internal/errors"
	"github.ibm.com/blockchaindb/server/internal/queue"
	"github.ibm.com/blockchaindb/server/pkg/logger"
)

type BlockReplicator struct {
	raftCh          chan interface{}       // TODO a placeholder for the Raft pipeline
	oneQueueBarrier *queue.OneQueueBarrier // Synchronizes the block-replication deliver  with the block-processor commit

	stopCh   chan struct{}
	stopOnce sync.Once
	doneCh   chan struct{}

	logger *logger.SugarLogger
}

// Config holds the configuration information required to initialize the block replicator.
type Config struct {
	//TODO just a skeleton
	BlockOneQueueBarrier *queue.OneQueueBarrier
	Logger               *logger.SugarLogger
}

// NewBlockReplicator creates a new BlockReplicator.
func NewBlockReplicator(conf *Config) *BlockReplicator {
	//TODO just a skeleton

	br := &BlockReplicator{
		raftCh:          make(chan interface{}, 10), // TODO a placeholder for the Raft pipeline
		oneQueueBarrier: conf.BlockOneQueueBarrier,
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
		logger:          conf.Logger,
	}

	return br
}

// Submit a block for replication.
//
// Submit may block if the replication input queue is full.
// Returns an error if the current node is not a leader.
// Returns an error if the component is already closed.
func (br *BlockReplicator) Submit(block interface{}) error {
	//TODO just a skeleton

	select {
	case <-br.stopCh:
		return &ierrors.ClosedError{ErrMsg: "block replicator closed"}
	case br.raftCh <- block: // TODO a placeholder for the Raft pipeline
		return nil
	}
}

// Start an internal go-routine to serve the main replication loop.
func (br *BlockReplicator) Start() {
	readyCh := make(chan struct{})
	go br.run(readyCh)
	<-readyCh
}

func (br *BlockReplicator) run(readyCh chan<- struct{}) {
	defer close(br.doneCh)

	br.logger.Info("starting the block replicator")
	close(readyCh)

Replicator_Loop:
	for {
		select {
		case <-br.stopCh:
			br.logger.Info("stopping block replicator")
			return

		case blockToCommit := <-br.raftCh:
			// Enqueue the block for the block-processor to commit, wait for a reply.
			// Once a reply is received, the block is safely committed.
			reConfig, err := br.oneQueueBarrier.EnqueueWait(blockToCommit)
			if err != nil {
				br.logger.Errorf("queue barrier error: %s, stopping block replicator", err.Error())
				break Replicator_Loop
			}

			// A non-nil reply is an indication that the last block was a valid config that applies to the replication
			// component.
			if reConfig != nil {
				br.logger.Infof("New config committed, going to apply to block replicator: %v", reConfig)
				// TODO
			}
		}
	}
}

// Close signals the internal go-routine to stop and waits for it to exit.
// If the component is already closed, and error is returned.
func (br *BlockReplicator) Close() (err error) {
	err = &ierrors.ClosedError{ErrMsg: "block replicator already closed"}
	br.stopOnce.Do(func() {
		br.logger.Info("closing block replicator")
		close(br.stopCh)
		if errQB := br.oneQueueBarrier.Close(); errQB != nil {
			br.logger.Debugf("OneQueueBarrier error: %s", errQB)
		}
		<-br.doneCh
		err = nil
	})

	return err
}
