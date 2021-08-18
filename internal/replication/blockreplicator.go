// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/config"
	"github.com/IBM-Blockchain/bcdb-server/internal/comm"
	ierrors "github.com/IBM-Blockchain/bcdb-server/internal/errors"
	"github.com/IBM-Blockchain/bcdb-server/internal/queue"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
)

const (
	// DefaultSnapshotCatchUpEntries is the default number of entries
	// to preserve in memory when a snapshot is taken. This is for
	// slow followers to catch up.
	DefaultSnapshotCatchUpEntries = uint64(4)
)

type BlockLedgerReader interface {
	Height() (uint64, error)
	Get(blockNumber uint64) (*types.Block, error)
}

type BlockReplicator struct {
	localConf *config.LocalConfiguration

	proposeCh       chan *types.Block
	raftID          uint64
	raftStorage     *RaftStorage
	raftNode        raft.Node
	oneQueueBarrier *queue.OneQueueBarrier // Synchronizes the block-replication deliver  with the block-processor commit
	transport       *comm.HTTPTransport
	ledgerReader    BlockLedgerReader

	stopCh        chan struct{}
	stopOnce      sync.Once
	doneProposeCh chan struct{}
	doneEventCh   chan struct{}

	mutex           sync.Mutex
	clusterConfig   *types.ClusterConfig
	lastKnownLeader uint64

	appliedIndex uint64
	lastBlock    *types.Block

	// needed by snapshotting
	sizeLimit        uint64 // SnapshotIntervalSize in bytes
	accDataSize      uint64 // accumulative data size since last snapshot
	lastSnapBlockNum uint64
	confState        raftpb.ConfState // Etcdraft requires ConfState to be persisted within snapshot

	lg *logger.SugarLogger
}

// Config holds the configuration information required to initialize the block replicator.
type Config struct {
	LocalConf            *config.LocalConfiguration
	ClusterConfig        *types.ClusterConfig
	LedgerReader         BlockLedgerReader
	Transport            *comm.HTTPTransport
	BlockOneQueueBarrier *queue.OneQueueBarrier
	Logger               *logger.SugarLogger
}

// NewBlockReplicator creates a new BlockReplicator.
func NewBlockReplicator(conf *Config) (*BlockReplicator, error) {
	raftID, err := comm.MemberRaftID(conf.LocalConf.Server.Identity.ID, conf.ClusterConfig)
	if err != nil {
		return nil, err
	}

	lg := conf.Logger.With("nodeID", conf.LocalConf.Server.Identity.ID, "raftID", raftID)

	haveWAL := wal.Exist(conf.LocalConf.Replication.WALDir)
	storage, err := CreateStorage(lg, conf.LocalConf.Replication.WALDir, conf.LocalConf.Replication.SnapDir)
	if err != nil {
		return nil, errors.Errorf("failed to restore persisted raft data: %s", err)
	}
	storage.SnapshotCatchUpEntries = DefaultSnapshotCatchUpEntries

	var snapBlkNum uint64
	var confState raftpb.ConfState
	if s := storage.Snapshot(); !raft.IsEmptySnap(s) {
		snapBlock := &types.Block{}
		if err := proto.Unmarshal(s.Data, snapBlock); err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal snapshot block")
		}

		snapBlkNum = snapBlock.GetHeader().GetBaseHeader().GetNumber()
		confState = s.Metadata.ConfState
		lg.Debugf("Starting from last snapshot: block number [%d], Raft ConfState: %+v", snapBlkNum, confState)
	}

	br := &BlockReplicator{
		localConf:        conf.LocalConf,
		proposeCh:        make(chan *types.Block, 1),
		raftID:           raftID,
		raftStorage:      storage,
		oneQueueBarrier:  conf.BlockOneQueueBarrier,
		stopCh:           make(chan struct{}),
		doneProposeCh:    make(chan struct{}),
		doneEventCh:      make(chan struct{}),
		clusterConfig:    conf.ClusterConfig,
		transport:        conf.Transport,
		ledgerReader:     conf.LedgerReader,
		sizeLimit:        conf.ClusterConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize,
		confState:        confState,
		lastSnapBlockNum: snapBlkNum,
		lg:               lg,
	}

	height, err := br.ledgerReader.Height()
	if err != nil {
		br.lg.Panic("Failed to read block height")
	}

	if height > 0 {
		br.lastBlock, err = br.ledgerReader.Get(height)
		if err != nil {
			br.lg.Panic("Failed to read last block")
		}
	}

	if height > 1 {
		metadata := br.lastBlock.GetConsensusMetadata()
		br.appliedIndex = metadata.GetRaftIndex()
		br.lg.Debugf("last block [%d], consensus metadata: %+v", height, metadata)
	}

	//DO NOT use Applied option in config, we guard against replay of written blocks with `appliedIndex` instead.
	raftConfig := &raft.Config{
		ID:              raftID,
		ElectionTick:    int(br.clusterConfig.ConsensusConfig.RaftConfig.ElectionTicks),
		HeartbeatTick:   int(br.clusterConfig.ConsensusConfig.RaftConfig.HeartbeatTicks),
		MaxSizePerMsg:   br.localConf.BlockCreation.MaxBlockSize,
		MaxInflightMsgs: int(br.clusterConfig.ConsensusConfig.RaftConfig.MaxInflightBlocks),
		Logger:          lg,
		Storage:         br.raftStorage.MemoryStorage,
		// PreVote prevents reconnected node from disturbing network.
		// See etcd/raft doc for more details.
		PreVote:                   true,
		CheckQuorum:               true,
		DisableProposalForwarding: true, // This prevents blocks from being accidentally proposed by followers
	}

	lg.Debugf("haveWAL: %v, Storage: %v, Raft config: %+v", haveWAL, storage, raftConfig)

	joinExistingCluster := false // TODO support node join to an existing cluster

	if haveWAL {
		br.raftNode = raft.RestartNode(raftConfig)
	} else {
		if joinExistingCluster {
			// TODO support node join to an existing cluster
			br.lg.Panicf("not supported yet: joinExistingCluster")
		} else {
			startPeers := raftPeers(br.clusterConfig)
			br.raftNode = raft.StartNode(raftConfig, startPeers)
		}
	}

	return br, nil
}

func (br *BlockReplicator) RaftID() uint64 {
	return br.raftID
}

// Submit a block for replication.
//
// This call may block if the replication input queue is full.
// Returns an error if the current node is not a leader.
// Returns an error if the component is already closed.
func (br *BlockReplicator) Submit(block *types.Block) error {
	blockNum := block.GetHeader().GetBaseHeader().GetNumber()
	if err := br.IsLeader(); err != nil {
		br.lg.Debugf("Submit of block [%d] refused, not a leader: %s", blockNum, err)
		return err
	}

	select {
	case <-br.stopCh:
		return &ierrors.ClosedError{ErrMsg: "block replicator closed"}
	case br.proposeCh <- block:
		br.lg.Debugf("Submitted block [%d]", blockNum)
		return nil
	}
}

// Start an internal go-routine to serve the main replication loop.
func (br *BlockReplicator) Start() {
	readyRaftCh := make(chan struct{})
	go br.runRaftEventLoop(readyRaftCh)
	<-readyRaftCh

	readyProposeCh := make(chan struct{})
	go br.runProposeLoop(readyProposeCh)
	<-readyProposeCh
}

func (br *BlockReplicator) runRaftEventLoop(readyCh chan<- struct{}) {
	defer close(br.doneEventCh)

	br.lg.Info("Starting the block replicator event loop")
	close(readyCh)

	//If height is smaller than the block number in the last snapshot, it means the node stopped after a
	// snapshot trigger was received, but before catch-up was completed. In order to cover this case, we do
	// catch-up first.
	if lastSnapshot := br.raftStorage.Snapshot(); !raft.IsEmptySnap(lastSnapshot) {
		if err := br.catchUp(lastSnapshot); err != nil {
			br.lg.Panicf("Failed to catch-up to last snapshot: %+v", lastSnapshot)
		}
	}

	// TODO use 'clock.Clock' so that tests can inject a fake clock
	tickInterval, err := time.ParseDuration(br.clusterConfig.ConsensusConfig.RaftConfig.TickInterval)
	if err != nil {
		br.lg.Panicf("Error parsing raft tick interval duration: %s", err)
	}
	raftTicker := time.NewTicker(tickInterval)
	electionTimeout := tickInterval.Seconds() * float64(br.clusterConfig.ConsensusConfig.RaftConfig.ElectionTicks)
	halfElectionTimeout := electionTimeout / 2

	// TODO proactive campaign to speed up leader election on a new cluster

	var raftStatusStr string
Event_Loop:
	for {
		select {
		case <-raftTicker.C:
			if status := br.raftNode.Status().String(); status != raftStatusStr {
				br.lg.Debugf("Raft node status: %+v", status)
				raftStatusStr = status
			}
			br.raftNode.Tick()

		case rd := <-br.raftNode.Ready():
			startStoring := time.Now()
			if err := br.raftStorage.Store(rd.Entries, rd.HardState, rd.Snapshot); err != nil {
				br.lg.Panicf("Failed to persist etcd/raft data: %s", err)
			}
			duration := time.Since(startStoring).Seconds()
			if duration > halfElectionTimeout {
				br.lg.Warningf("WAL sync took %v seconds and the network is configured to start elections after %v seconds. Your disk is too slow and may cause loss of quorum and trigger leadership election.", duration, electionTimeout)
			}

			// update last known leader
			if rd.SoftState != nil {
				leader := atomic.LoadUint64(&rd.SoftState.Lead) // etcdraft requires atomic access to this var
				if leader != raft.None {
					br.lg.Debugf("Leader %d is present", leader)
				} else {
					br.lg.Debug("No leader")
				}

				br.mutex.Lock()
				if leader != br.lastKnownLeader {
					br.lg.Infof("Leader changed: %d to %d", br.lastKnownLeader, leader)
					br.lastKnownLeader = leader
				}
				br.mutex.Unlock()
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := br.catchUp(rd.Snapshot); err != nil {
					br.lg.Panicf("Failed to catch-up to snapshot: %+v", rd.Snapshot)
				}
			}

			br.transport.SendConsensus(rd.Messages)

			if ok := br.deliverEntries(rd.CommittedEntries); !ok {
				br.lg.Warningf("Failed to deliver committed entries, breaking out of event loop")
				break Event_Loop
			}

			br.raftNode.Advance()

		case <-br.stopCh:
			br.lg.Info("Stopping block replicator")
			break Event_Loop
		}
	}

	raftTicker.Stop()
	br.raftNode.Stop()
	if err := br.raftStorage.Close(); err != nil {
		br.lg.Errorf("Error while stopping RaftStorage: %s", err) // TODO move to raft main loop
	}

	br.lg.Info("Exiting block replicator event loop")
}

// When a node lags behind the cluster more than the last checkpoint of the leader, the leader will send a snapshot to
// it. A snapshot is a block with some raft information. A received snapshot serves as a trigger for the node to
// perform catch-up, or state transfer. It will contact one of the active members of the cluster (preferably the
// leader), and will request the missing blocks up to the block indicated by the snapshot.
func (br *BlockReplicator) catchUp(snap raftpb.Snapshot) error {
	if snap.Metadata.Index <= br.appliedIndex {
		br.lg.Debugf("Skip snapshot taken at index %d, because it is behind current applied index %d", snap.Metadata.Index, br.appliedIndex)
		return nil
	}

	var snapBlock = &types.Block{}
	if err := proto.Unmarshal(snap.Data, snapBlock); err != nil {
		return errors.Errorf("failed to unmarshal snapshot data to block: %s", err)
	}

	br.lg.Debugf("last block: %+v", br.lastBlock)
	br.lg.Debugf("snap block: %+v", snapBlock)
	initBlockNumber := br.lastBlock.Header.BaseHeader.Number
	if br.lastBlock.Header.BaseHeader.Number >= snapBlock.Header.BaseHeader.Number {
		br.lg.Errorf("Snapshot is at block [%d], local block number is %d, no catch-up needed", snapBlock.Header.BaseHeader.Number, initBlockNumber)
		return nil
	}

	br.lg.Infof("Starting state transfer; From block: %d, index %d; To block: %d, index: %d",
		initBlockNumber, br.appliedIndex, snapBlock.Header.BaseHeader.Number, snap.Metadata.Index)
	br.confState = snap.Metadata.ConfState
	br.appliedIndex = snap.Metadata.Index

	// Pull the missing blocks, starting with one past the last block we have, and ending with the block number from the snapshot.
	for nextBlockNumber := initBlockNumber + 1; nextBlockNumber <= snapBlock.Header.BaseHeader.Number; {
		var blocks []*types.Block
		var err error
		blocksReadyCh := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())

		//Try to pull some blocks in a go-routine so that we may cancel it if the server shuts down.
		//Note that `PullBlocks` will not necessarily return all the blocks we requested, hence the enclosing loop.
		go func() {
			defer close(blocksReadyCh)
			blocks, err = br.transport.PullBlocks(ctx, nextBlockNumber, snapBlock.Header.BaseHeader.Number, br.GetLeaderID())
		}()

		select {
		case <-br.stopCh:
			cancel()
			<-blocksReadyCh
			return &ierrors.ClosedError{ErrMsg: "server stopped during catch-up"}
		case <-blocksReadyCh:
			if err != nil {
				switch err.(type) {
				case *ierrors.ClosedError:
					br.lg.Warnf("closing, stopping to pull blocks from cluster; last block number [%d], snapshot: %+v", br.lastBlock.Header.BaseHeader.Number, snap)
					return nil
				default:
					return errors.Wrapf(err, "failed to pull blocks from cluster; last block number [%d], snapshot: %+v", br.lastBlock.Header.BaseHeader.Number, snap)
				}
			}

			br.lg.Infof("Going to commit [%d] blocks", len(blocks)) //Not necessarily the entire range requested!

			for _, blockToCommit := range blocks {
				br.lg.Infof("enqueue for commit block [%d], ConsensusMetadata: [%+v]",
					blockToCommit.GetHeader().GetBaseHeader().GetNumber(),
					blockToCommit.GetConsensusMetadata())

				if err := br.commitBlock(blockToCommit); err != nil {
					switch err.(type) {
					case *ierrors.ClosedError:
						br.lg.Warnf("closing, stopping to pull blocks from cluster; last block number [%d], snapshot: %+v", br.lastBlock.Header.BaseHeader.Number, snap)
						return nil
					default:
						return err
					}
				}

				nextBlockNumber++
			}
		}
	}

	br.lg.Infof("Finished syncing with cluster up to and including block [%d]", br.lastBlock.Header.BaseHeader.Number)

	return nil

}

func (br *BlockReplicator) deliverEntries(committedEntries []raftpb.Entry) bool {
	br.lg.Debugf("Num. entries: %d", len(committedEntries))
	if len(committedEntries) == 0 {
		return true
	}

	if committedEntries[0].Index > br.appliedIndex+1 {
		br.lg.Panicf("First index of committed entry [%d] should <= appliedIndex [%d]+1", committedEntries[0].Index, br.appliedIndex)
	}

	var position int
	for i := range committedEntries {
		br.lg.Debugf("processing commited entry [%d]: %s", i, raftEntryString(committedEntries[i]))

		switch committedEntries[i].Type {
		case raftpb.EntryNormal:
			if len(committedEntries[i].Data) == 0 {
				br.lg.Debugf("commited entry [%d] has empty data, ignoring", i)
				break
			}

			position = i
			br.accDataSize += uint64(len(committedEntries[i].Data))

			// We need to strictly avoid re-applying normal entries,
			// otherwise we are writing the same block twice.
			if committedEntries[i].Index <= br.appliedIndex {
				br.lg.Debugf("Received block with raft index (%d) <= applied index (%d), skip", committedEntries[i].Index, br.appliedIndex)
				break
			}

			var block = &types.Block{}
			if err := proto.Unmarshal(committedEntries[i].Data, block); err != nil {
				br.lg.Panicf("Error unmarshaling entry [#%d], entry: %+v, error: %s", i, committedEntries[i], err)
			}
			block.ConsensusMetadata = &types.ConsensusMetadata{
				RaftTerm:  committedEntries[i].Term,
				RaftIndex: committedEntries[i].Index,
			}

			err := br.commitBlock(block)
			if err != nil {
				br.lg.Errorf("commit block error: %s, stopping block replicator", err.Error())
				return false
			}

		case raftpb.EntryConfChange:
			// TODO support reconfig
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(committedEntries[i].Data); err != nil {
				br.lg.Warnf("Failed to unmarshal ConfChange data: %s", err)
				continue
			}

			br.confState = *br.raftNode.ApplyConfChange(cc)

			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				br.lg.Infof("Applied config change to add node %d, current nodes in cluster: %+v", cc.NodeID, br.confState.Voters)
			case raftpb.ConfChangeRemoveNode:
				br.lg.Infof("Applied config change to remove node %d, current nodes in cluster: %+v", cc.NodeID, br.confState.Voters)
			default:
				br.lg.Panic("Programming error, encountered unsupported raft config change")
			}

			// TODO configure transport

			// TODO detect removal of leader

			// TODO detect removal of self
		}

		// after commit, update appliedIndex
		if br.appliedIndex < committedEntries[i].Index {
			br.appliedIndex = committedEntries[i].Index
		}
	}

	// Take a snapshot if in-memory storage size exceeds the limit
	if br.accDataSize >= br.sizeLimit {
		var snapBlock = &types.Block{}
		if err := proto.Unmarshal(committedEntries[position].Data, snapBlock); err != nil {
			br.lg.Panicf("Error unmarshaling entry [#%d], entry: %+v, error: %s", position, committedEntries[position], err)
		}

		if err := br.raftStorage.TakeSnapshot(br.appliedIndex, br.confState, committedEntries[position].Data); err != nil {
			br.lg.Fatalf("Failed to create snapshot at index %d: %s", br.appliedIndex, err)
		}

		br.lg.Infof("Accumulated %d bytes since last snapshot, exceeding size limit (%d bytes), "+
			"taking snapshot at block [%d] (index: %d), last snapshotted block number is %d, current voters: %+v",
			br.accDataSize, br.sizeLimit, snapBlock.GetHeader().GetBaseHeader().GetNumber(), br.appliedIndex, br.lastSnapBlockNum, br.confState.Voters)

		br.accDataSize = 0
		br.lastSnapBlockNum = snapBlock.GetHeader().GetBaseHeader().GetNumber()
	}

	return true
}

func (br *BlockReplicator) runProposeLoop(readyCh chan<- struct{}) {
	defer close(br.doneProposeCh)

	br.lg.Info("Starting the block replicator propose loop")
	close(readyCh)

Propose_Loop:
	for {
		select {
		case blockToPropose := <-br.proposeCh:

			//TODO check if leader, if not, refuse to propose

			blockBytes, err := proto.Marshal(blockToPropose)
			if err != nil {
				br.lg.Panicf("Error marshaling a block: %s", err)
			}

			err = br.raftNode.Propose(context.Background(), blockBytes)
			if err != nil {
				br.lg.Warnf("Failed to propose block: Num: %d; error: %s", blockToPropose.GetHeader().GetBaseHeader().GetNumber(), err)
				//TODO reject all TXs in that block,
			}

		case <-br.stopCh:
			br.lg.Debug("Stopping block replicator")
			break Propose_Loop
		}
	}

	br.lg.Info("Exiting the block replicator propose loop")
}

// Close signals the internal go-routine to stop and waits for it to exit.
// If the component is already closed, and error is returned.
func (br *BlockReplicator) Close() (err error) {
	err = &ierrors.ClosedError{ErrMsg: "block replicator already closed"}
	br.stopOnce.Do(func() {
		br.lg.Info("closing block replicator")
		close(br.stopCh)
		if errQB := br.oneQueueBarrier.Close(); errQB != nil {
			br.lg.Debugf("OneQueueBarrier error: %s", errQB)
		}
		<-br.doneProposeCh
		<-br.doneEventCh

		//after the node stops, it no longer knows who the leader is
		br.mutex.Lock()
		defer br.mutex.Unlock()
		br.lastKnownLeader = 0

		err = nil
	})

	return err
}

func (br *BlockReplicator) IsLeader() *ierrors.NotLeaderError {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	if br.lastKnownLeader == br.raftID {
		return nil
	}

	return &ierrors.NotLeaderError{LeaderID: br.lastKnownLeader}
}

func (br *BlockReplicator) GetLeaderID() uint64 {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	return br.lastKnownLeader
}

func (br *BlockReplicator) commitBlock(block *types.Block) error {
	br.lg.Infof("enqueue for commit block [%d], ConsensusMetadata: %+v ",
		block.GetHeader().GetBaseHeader().GetNumber(),
		block.GetConsensusMetadata())

	reConfig, err := br.oneQueueBarrier.EnqueueWait(block)
	if err != nil {
		return err
	}

	br.lastBlock = block

	if reConfig == nil {
		return nil
	}

	clusterConfig := reConfig.(*types.ClusterConfig)
	if err := br.updateClusterConfig(clusterConfig); err != nil {
		// TODO support dynamic re-config
		br.lg.Panicf("Failed to update to ClusterConfig during raft normal entry: error: %s", err)
	}

	return nil
}

func (br *BlockReplicator) updateClusterConfig(clusterConfig *types.ClusterConfig) error {
	br.lg.Infof("New cluster config committed, going to apply to block replicator: %+v", clusterConfig)

	nodes, consensus, _, _ := ClassifyClusterReConfig(br.clusterConfig, clusterConfig)
	if nodes || consensus {
		return errors.New("dynamic re-config of ClusterConfig Nodes & Consensus not supported yet")
		//TODO dynamic re-config, update transport config, etc
	}

	return nil
}

func (br *BlockReplicator) Process(ctx context.Context, m raftpb.Message) error {
	br.lg.Debugf("Incoming raft message: %+v", m)
	err := br.raftNode.Step(ctx, m)
	if err != nil {
		br.lg.Errorf("Error during raft node Step: %s", err)
	}
	return err
}

func (br *BlockReplicator) IsIDRemoved(id uint64) bool {
	br.lg.Debugf("> IsIDRemoved: %d", id)
	// see: rafthttp.RAFT

	//TODO look into the cluster config and check whether this RaftID was removed.
	// removed RaftIDs may never return.
	// see issue: https://github.com/ibm-blockchain/bcdb-server/issues/40
	return false
}
func (br *BlockReplicator) ReportUnreachable(id uint64) {
	br.lg.Debugf("ReportUnreachable: %d", id)
	br.raftNode.ReportUnreachable(id)
}
func (br *BlockReplicator) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	br.lg.Debugf("> ReportSnapshot: %d, %+v", id, status)
	// see: rafthttp.RAFT
	//TODO see issue: https://github.com/ibm-blockchain/bcdb-server/issues/41
}
