// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"context"
	"errors"
	"sync"

	"github.com/IBM-Blockchain/bcdb-server/config"
	"github.com/IBM-Blockchain/bcdb-server/internal/comm"
	ierrors "github.com/IBM-Blockchain/bcdb-server/internal/errors"
	"github.com/IBM-Blockchain/bcdb-server/internal/queue"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

type BlockReplicator struct {
	localConf       *config.LocalConfiguration
	raftCh          chan interface{}       // TODO a placeholder for the Raft pipeline
	oneQueueBarrier *queue.OneQueueBarrier // Synchronizes the block-replication deliver  with the block-processor commit
	transport       *comm.HTTPTransport

	stopCh   chan struct{}
	stopOnce sync.Once
	doneCh   chan struct{}

	mutex         sync.Mutex
	clusterConfig *types.ClusterConfig

	logger *logger.SugarLogger
}

// Config holds the configuration information required to initialize the block replicator.
type Config struct {
	//TODO just a skeleton
	LocalConf            *config.LocalConfiguration
	ClusterConfig        *types.ClusterConfig
	Transport            *comm.HTTPTransport
	BlockOneQueueBarrier *queue.OneQueueBarrier
	Logger               *logger.SugarLogger
}

// NewBlockReplicator creates a new BlockReplicator.
func NewBlockReplicator(conf *Config) *BlockReplicator {
	//TODO just a skeleton

	br := &BlockReplicator{
		localConf:       conf.LocalConf,
		raftCh:          make(chan interface{}, 10), // TODO a placeholder for the Raft pipeline
		oneQueueBarrier: conf.BlockOneQueueBarrier,
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
		clusterConfig:   conf.ClusterConfig,
		transport:       conf.Transport,
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

	br.logger.Info("Starting the block replicator")
	close(readyCh)

Replicator_Loop:
	for {
		select {
		case <-br.stopCh:
			br.logger.Info("Stopping block replicator")
			return

		case blockToCommit := <-br.raftCh:
			// Enqueue the block for the block-processor to commit, wait for a reply.
			// Once a reply is received, the block is safely committed.
			reConfig, err := br.oneQueueBarrier.EnqueueWait(blockToCommit)
			if err != nil {
				br.logger.Errorf("queue barrier error: %s, stopping block replicator", err.Error())
				break Replicator_Loop
			}

			// A non-nil reply is an indication that the last block was a valid config that applies
			// to the replication component.
			if reConfig != nil {
				clusterConfig := reConfig.(*types.ClusterConfig)
				if err = br.updateClusterConfig(clusterConfig); err != nil {
					br.logger.Panicf("Failed to update ClusterConfig: %s", err)
				}
			}
		}
	}

	br.logger.Info("Exiting the block replicator")
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

func (br *BlockReplicator) updateClusterConfig(clusterConfig *types.ClusterConfig) error {
	br.logger.Infof("New cluster config committed, going to apply to block replicator: %+v", clusterConfig)

	//TODO dynamic re-config, update transport config, etc

	return errors.New("dynamic re-config of ClusterConfig not supported yet")
}

func (br *BlockReplicator) Process(ctx context.Context, m raftpb.Message) error {
	// see: rafthttp.RAFT
	//TODO
	return nil
}

func (br *BlockReplicator) IsIDRemoved(id uint64) bool {
	// see: rafthttp.RAFT
	//TODO
	return false
}
func (br *BlockReplicator) ReportUnreachable(id uint64) {
	// see: rafthttp.RAFT
	//TODO
}
func (br *BlockReplicator) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	// see: rafthttp.RAFT
	//TODO
}
