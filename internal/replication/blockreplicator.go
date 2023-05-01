// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"context"
	"fmt"
	"hash/crc64"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	"github.com/hyperledger-labs/orion-server/internal/comm"
	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/queue"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
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

//go:generate counterfeiter -o mocks/pending_txs.go --fake-name PendingTxsReleaser . PendingTxsReleaser

type PendingTxsReleaser interface {
	ReleaseWithError(txIDs []string, err error)
}

//go:generate counterfeiter -o mocks/config_tx_validator.go --fake-name ConfigTxValidator . ConfigTxValidator

type ConfigTxValidator interface {
	Validate(txEnv *types.ConfigTxEnvelope) (*types.ValidationInfo, error)
}

type BlockReplicator struct {
	localConf       *config.LocalConfiguration
	joinBlock       *types.Block
	joinBlockNumber uint64

	proposeCh         chan *types.Block
	raftID            uint64
	raftStorage       *RaftStorage
	raftConfig        *raft.Config
	oneQueueBarrier   *queue.OneQueueBarrier // Synchronizes the block-replication deliver with the block-processor commit
	transport         *comm.HTTPTransport
	ledgerReader      BlockLedgerReader
	pendingTxs        PendingTxsReleaser
	configTxValidator ConfigTxValidator

	stopCh        chan struct{}
	stopOnce      sync.Once
	doneProposeCh chan struct{}
	doneEventCh   chan struct{}

	// shared state between the propose-loop go-routine and event-loop go-routine; as well as transport go-routines.
	mutex                           sync.Mutex
	clusterConfig                   *types.ClusterConfig
	joinExistingCluster             bool
	finishedJoin                    bool
	runCampaign                     bool // this node starts a campaign on a new cluster
	raftNode                        raft.Node
	lastKnownLeader                 uint64
	lastKnownLeaderHost             string // cache the leader's Node host:port for client request redirection
	justElectedInFlightBlocks       bool   // when a leader is just elected, it may have in-flight blocks
	cancelProposeContext            func() // cancels the propose-context if leadership is lost
	lastProposedBlockNumber         uint64
	lastProposedBlockHeaderBaseHash []byte
	lastCommittedBlock              *types.Block
	numInFlightBlocks               uint32 // number of in-flight blocks
	inFlightConfigBlockNumber       uint64 // the block number of the in-flight config, if any; 0 if none
	condTooManyInFlightBlocks       *sync.Cond

	appliedIndex uint64

	// needed by snapshotting
	sizeLimit        uint64 // SnapshotIntervalSize in bytes
	accDataSize      uint64 // accumulative data size since last snapshot
	lastSnapBlockNum uint64
	confState        raftpb.ConfState // Etcdraft requires ConfState to be persisted within snapshot

	lg      *logger.SugarLogger
	metrics *utils.TxProcessingMetrics
}

// Config holds the configuration information required to initialize the block replicator.
type Config struct {
	LocalConf            *config.LocalConfiguration
	ClusterConfig        *types.ClusterConfig
	JoinBlock            *types.Block
	LedgerReader         BlockLedgerReader
	Transport            *comm.HTTPTransport
	BlockOneQueueBarrier *queue.OneQueueBarrier
	PendingTxs           PendingTxsReleaser
	ConfigValidator      ConfigTxValidator
	Logger               *logger.SugarLogger
	Metrics              *utils.TxProcessingMetrics
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
		localConf:            conf.LocalConf,
		joinBlock:            conf.JoinBlock,
		joinBlockNumber:      conf.JoinBlock.GetHeader().GetBaseHeader().GetNumber(), // if joinBlock==nil => 0
		proposeCh:            make(chan *types.Block, 1),
		raftID:               raftID,
		raftStorage:          storage,
		oneQueueBarrier:      conf.BlockOneQueueBarrier,
		transport:            conf.Transport,
		ledgerReader:         conf.LedgerReader,
		pendingTxs:           conf.PendingTxs,
		configTxValidator:    conf.ConfigValidator,
		stopCh:               make(chan struct{}),
		doneProposeCh:        make(chan struct{}),
		doneEventCh:          make(chan struct{}),
		clusterConfig:        conf.ClusterConfig,
		cancelProposeContext: func() {}, //NOOP
		sizeLimit:            conf.ClusterConfig.ConsensusConfig.RaftConfig.SnapshotIntervalSize,
		lastSnapBlockNum:     snapBlkNum,
		confState:            confState,
		lg:                   lg,
		metrics:              conf.Metrics,
	}
	br.condTooManyInFlightBlocks = sync.NewCond(&br.mutex)

	height, err := br.ledgerReader.Height()
	if err != nil {
		br.lg.Panicf("Failed to read block height: %s", err)
	}

	if height > 0 {
		br.lastCommittedBlock, err = br.ledgerReader.Get(height)
		if err != nil {
			br.lg.Panicf("Failed to read last block: %s", err)
		}
		br.lastProposedBlockNumber = br.lastCommittedBlock.GetHeader().GetBaseHeader().GetNumber()
		if baseHash, err := blockstore.ComputeBlockBaseHash(br.lastCommittedBlock); err == nil {
			br.lastProposedBlockHeaderBaseHash = baseHash
		} else {
			br.lg.Panicf("Failed to compute last block base hash: %s", err)
		}
	}

	if height > 1 {
		metadata := br.lastCommittedBlock.GetConsensusMetadata()
		br.appliedIndex = metadata.GetRaftIndex()
		br.lg.Debugf("last block [%d], consensus metadata: %+v", height, metadata)
	}

	//DO NOT use Applied option in config, we guard against replay of written blocks with `appliedIndex` instead.
	br.raftConfig = &raft.Config{
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

	lg.Debugf("height: %d, haveWAL: %v, Storage: %v, Raft config: %+v", height, haveWAL, storage, br.raftConfig)

	br.joinExistingCluster = (conf.JoinBlock != nil) && (height < br.joinBlockNumber)

	if haveWAL {
		lg.Info("Restarting Raft")
		br.raftNode = raft.RestartNode(br.raftConfig)
	} else {
		if br.joinExistingCluster {
			// Note: when on-boarding, the raftNode is constructed late, after on-boarding is complete.
			lg.Infof("Starting Raft to join an existing cluster, current peers: %v, ConsensusMetadata: %+v",
				conf.JoinBlock.GetConfigTxEnvelope().GetPayload().GetNewConfig().GetConsensusConfig().GetMembers(),
				conf.JoinBlock.GetConsensusMetadata())
		} else {
			startPeers := raftPeers(br.clusterConfig)

			lg.Infof("Starting Raft on a new cluster, start peers: %v", startPeers)
			genesisBytes, err := proto.Marshal(br.lastCommittedBlock)
			if err != nil {
				br.lg.Panicf("Failed to read genesis block: %s", err)
			}
			genesisHash := crc64.Checksum(genesisBytes, crc64.MakeTable(crc64.ISO))
			index := int(genesisHash % uint64(len(startPeers)))
			if startPeers[index].ID == br.raftID {
				br.runCampaign = true
				lg.Info("This node was selected to run a leader election campaign on the new cluster")
			}
			br.raftNode = raft.StartNode(br.raftConfig, startPeers)
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

// Start internal go-routines to serve the main replication loops.
//
// If the `joinExistingCluster` flag is true, the on-boarding process starts first, in its own go-routine.
// When on-boarding is complete, replication will start.
func (br *BlockReplicator) Start() {
	if br.joinExistingCluster {
		go br.startOnBoarding()
		return
	}

	br.startConsenting()
}

// startConsenting starts the to go-routines that service raft: the event-loop and the propose-loop.
// Note that these two routines always start after the raftNode is already constructed.
func (br *BlockReplicator) startConsenting() {
	readyRaftCh := make(chan struct{})
	go br.runRaftEventLoop(readyRaftCh)
	<-readyRaftCh

	readyProposeCh := make(chan struct{})
	go br.runProposeLoop(readyProposeCh)
	<-readyProposeCh
}

// startOnBoarding pulls the missing blocks from the current ledger height up to (and including) the join block.
// It then constructs the raftpb.ConfState and raftpb.Snapshot from the cluster configuration of the join block and
// stores it. This way, raft storage starts with the right state to join the cluster. In addition, if the node is
// restarted, the snapshot indicates that the node needs to restart normally, rather than perform on-boarding again.
func (br *BlockReplicator) startOnBoarding() {
	br.lg.Info("Starting the on-boarding process")

	err := br.onBoard(br.joinBlock)
	if err != nil {
		switch err.(type) {
		case *ierrors.ClosedError:
			br.lg.Warn("Closing, stopping on-boarding")
			_ = br.raftStorage.Close()
			close(br.doneProposeCh)
			close(br.doneEventCh)
			return // do not cause panic when the server shuts down
		default:
			br.lg.Panicf("Failed to on-board to join-block number: %d, block: %+v", br.joinBlockNumber, br.joinBlock)
		}
	}

	br.lg.Infof("On-boarding completed successfully, starting replication")

	// make a snapshot from the join block
	snapData, err := proto.Marshal(br.joinBlock)
	if err != nil {
		br.lg.Panicf("Failed to marshal join block: %s", err)
	}
	br.confState = raftpb.ConfState{}
	for _, peer := range br.clusterConfig.GetConsensusConfig().GetMembers() {
		br.confState.Voters = append(br.confState.Voters, peer.RaftId)
	}
	sort.Slice(br.confState.Voters, func(i, j int) bool { return br.confState.Voters[i] < br.confState.Voters[j] })
	br.lg.Infof("Starting Raft on an existing cluster, current peers: %v", br.confState.Voters)

	snapshot := raftpb.Snapshot{
		Data: snapData,
		Metadata: raftpb.SnapshotMetadata{
			ConfState: br.confState,
			Index:     br.joinBlock.GetConsensusMetadata().GetRaftIndex(),
			Term:      br.joinBlock.GetConsensusMetadata().GetRaftTerm(),
		},
	}

	err = br.raftStorage.Store(nil, raftpb.HardState{}, snapshot)
	if err != nil {
		br.lg.Panicf("Failed to store join block as snapshot in raft storage: %s", err)
	}

	// mark join as finished, this will allow incoming messages to flow into the raftNode
	br.mutex.Lock()
	br.raftNode = raft.RestartNode(br.raftConfig)
	br.finishedJoin = true
	br.mutex.Unlock()

	br.startConsenting()
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

	tickInterval, err := time.ParseDuration(br.clusterConfig.ConsensusConfig.RaftConfig.TickInterval)
	if err != nil {
		br.lg.Panicf("Error parsing raft tick interval duration: %s", err)
	}
	raftTicker := time.NewTicker(tickInterval)
	electionTimeout := tickInterval.Seconds() * float64(br.clusterConfig.ConsensusConfig.RaftConfig.ElectionTicks)
	halfElectionTimeout := electionTimeout / 2

	// Proactive campaign to speed up leader election on a new cluster
	stopCampaignCh := make(chan struct{})
	finishedCampaign := false
	if br.runCampaign {
		go br.runCampaignLoop(stopCampaignCh)
	}

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

			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := br.catchUp(rd.Snapshot); err != nil {
					br.lg.Panicf("Failed to catch-up to snapshot: %+v", rd.Snapshot)
				}
			}

			_ = br.transport.SendConsensus(rd.Messages)

			if ok := br.deliverEntries(rd.CommittedEntries); !ok {
				br.lg.Warningf("Stopping to deliver committed entries, breaking out of event loop")
				break Event_Loop
			}

			br.processLeaderJustElected()

			// update last known leader
			if rd.SoftState != nil {
				leader := atomic.LoadUint64(&rd.SoftState.Lead) // etcdraft requires atomic access to this var
				if leader != raft.None {
					br.lg.Debugf("Leader %d is present", leader)
					// finish the campaign on the first leader, whether it is running or not
					if !finishedCampaign {
						close(stopCampaignCh)
						finishedCampaign = true
					}
				} else {
					br.lg.Debug("No leader")
				}

				br.processLeaderChanges(leader)
			}

			br.raftNode.Advance()

		case <-br.stopCh:
			br.lg.Info("Stopping block replicator")
			break Event_Loop
		}
	}

	// Notify the propose-loop go-routine in case it is waiting for blocks to commit or a leadership change.
	br.mutex.Lock()
	br.lastKnownLeader = 0           // stop proposing
	br.lastKnownLeaderHost = ""      // stop proposing
	br.numInFlightBlocks = 0         // stop waiting for blocks to commit
	br.inFlightConfigBlockNumber = 0 // stop waiting for config block to commit
	br.condTooManyInFlightBlocks.Broadcast()
	br.mutex.Unlock()

	raftTicker.Stop()
	br.raftNode.Stop()
	if err := br.raftStorage.Close(); err != nil {
		br.lg.Errorf("Error while stopping RaftStorage: %s", err) // TODO move to raft main loop
	}

	br.lg.Info("Exiting block replicator event loop")
}

// Initiate a leader election campaign every two HeartbeatTimeout elapses, until leader is present. Either this
// node successfully claims leadership, or another leader already existed when this node starts.
// We could do this more lazily and exit proactive campaign once transitioned to Candidate state
// (not PreCandidate because other nodes might not have started yet, in which case PreVote
// messages are dropped at recipients). But there is no obvious reason (for now) to be lazy.
//
// 2*HeartbeatTicks is used to avoid excessive campaign when network latency is significant and
// Raft term keeps advancing in this extreme case.
func (br *BlockReplicator) runCampaignLoop(stopCampaignCh chan struct{}) {
	tickInterval, err := time.ParseDuration(br.clusterConfig.ConsensusConfig.RaftConfig.TickInterval)
	if err != nil {
		br.lg.Panicf("Error parsing raft tick interval duration: %s", err)
	}
	campaignInterval := 2 * time.Duration(br.clusterConfig.ConsensusConfig.RaftConfig.HeartbeatTicks) * tickInterval

	br.lg.Infof("Starting to campaign every %s", campaignInterval)
	for i := 1; ; i++ {
		select {
		case <-br.stopCh:
			br.lg.Debugf("Closing, exiting campaign loop")
			return
		case <-stopCampaignCh:
			br.lg.Debugf("Done, exiting campaign loop")
			return
		case <-time.After(campaignInterval):
			if err := br.raftNode.Campaign(context.TODO()); err != nil {
				br.lg.Errorf("Error starting campaign, aborting campaign loop: %s", err)
				return
			}
			br.lg.Debugf("Started campaign #%d", i)
		}
	}
}

func (br *BlockReplicator) processLeaderJustElected() {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	if br.justElectedInFlightBlocks {
		lastIndex, _ := br.raftStorage.MemoryStorage.LastIndex() //never returns error
		msgInflight := lastIndex > br.appliedIndex
		if msgInflight {
			br.lg.Debugf("There are [%d] in flight blocks, Raft new leader should not serve requests", lastIndex-br.appliedIndex)
		} else {
			br.lg.Infof("Start accepting requests as Raft new leader at block [%d]", br.lastCommittedBlock.GetHeader().GetBaseHeader().GetNumber())
			br.justElectedInFlightBlocks = false
			br.resetLastProposed()
		}
	}

}

func (br *BlockReplicator) processLeaderChanges(leader uint64) {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	if leader != br.lastKnownLeader {
		br.lg.Infof("Leader changed: %d to %d", br.lastKnownLeader, leader)

		lostLeadership := br.lastKnownLeader == br.raftID
		assumedLeadership := leader == br.raftID

		if lostLeadership {
			br.lg.Info("Lost leadership")
			// cancel the current proposal to free the propose-loop go-routine, as it might block for a long time.
			br.cancelProposeContext()
			br.cancelProposeContext = func() {} // NOOP
		} else if assumedLeadership {
			br.lg.Info("Assumed leadership")
			lastIndex, _ := br.raftStorage.MemoryStorage.LastIndex() //never returns error
			if lastIndex > br.appliedIndex {
				br.lg.Infof("There are [%d] in flight blocks, Raft new leader will not serve requests util they are committed", lastIndex-br.appliedIndex)
				br.justElectedInFlightBlocks = true
			} else {
				br.lg.Debugf("There are no in flight blocks, Raft new leader will serve requests, lastIndex: %d, appliedIndex: %d", lastIndex, br.appliedIndex)
			}
		}

		if lostLeadership || assumedLeadership {
			br.resetLastProposed()
			br.numInFlightBlocks = 0
			br.inFlightConfigBlockNumber = 0
			br.condTooManyInFlightBlocks.Broadcast()
		}

		br.lastKnownLeader = leader
		br.lastKnownLeaderHost = br.nodeHostPortFromRaftID(leader)
	}
}

func (br *BlockReplicator) resetLastProposed() {
	var err error
	br.lastProposedBlockNumber = br.lastCommittedBlock.GetHeader().GetBaseHeader().GetNumber()
	br.lastProposedBlockHeaderBaseHash, err = blockstore.ComputeBlockBaseHash(br.lastCommittedBlock)
	if err != nil {
		br.lg.Panicf("Error computing base header hash of last commited block: %+v; error: %s",
			br.lastCommittedBlock.GetHeader(), err)
	}
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

	initBlockNumber := br.getLastCommittedBlockNumber()
	br.lg.Debugf("initial last block number: %+v", initBlockNumber)
	br.lg.Debugf("snap block: %+v", snapBlock)

	if initBlockNumber >= snapBlock.Header.BaseHeader.Number {
		br.lg.Errorf("Snapshot is at block [%d], local block number is %d, no catch-up needed", snapBlock.Header.BaseHeader.Number, initBlockNumber)
		return nil
	}

	br.lg.Infof("Starting state transfer; From block: %d, index %d; To block: %d, index: %d",
		initBlockNumber, br.appliedIndex, snapBlock.Header.BaseHeader.Number, snap.Metadata.Index)
	br.confState = snap.Metadata.ConfState
	br.appliedIndex = snap.Metadata.Index

	err := br.catchUpToBlock(initBlockNumber, snapBlock.Header.BaseHeader.Number, true)
	if err != nil {
		switch err.(type) {
		case *ierrors.ClosedError:
			br.lg.Warn("Closing, stopping catch-up to snapshot")
			return nil // do not cause panic when the server shuts down
		default:
			return err
		}
	}

	lastBlockNumber := br.getLastCommittedBlockNumber()
	br.lg.Infof("Finished syncing with cluster up to and including block [%d]", lastBlockNumber)

	return nil
}

func (br *BlockReplicator) onBoard(joinBlock *types.Block) error {
	initBlockNumber := br.getLastCommittedBlockNumber()
	joinBlockNumber := joinBlock.GetHeader().GetBaseHeader().GetNumber()
	joinBlockMeta := joinBlock.GetConsensusMetadata()
	br.lg.Debugf("Join-block number: %d, last committed block number: %d,", joinBlockNumber, initBlockNumber)
	br.lg.Debugf("Join-block consensus metadata: %+v", joinBlockMeta)

	if initBlockNumber >= joinBlockNumber {
		br.lg.Errorf("Join-block is number [%d], local block number is [%d], no on-boarding needed", joinBlockNumber, initBlockNumber)
		return nil
	}

	br.lg.Infof("Starting on-boarding state transfer; From block: %d, To block: %d", initBlockNumber, joinBlockNumber)
	// we do not update the cluster-config with incoming config blocks because the join-block is the most updated
	// config, and it is already applied to `replication` and `comm`.
	err := br.catchUpToBlock(initBlockNumber, joinBlockNumber, false)
	if err != nil {
		return err
	}

	lastBlockNumber := br.getLastCommittedBlockNumber()
	br.lg.Infof("Finished syncing with cluster up to and including block [%d]", lastBlockNumber)

	if lastBlockNumber > 1 {
		metadata := br.lastCommittedBlock.GetConsensusMetadata()
		br.appliedIndex = metadata.GetRaftIndex()
		br.lg.Debugf("last block [%d], consensus metadata: %+v", lastBlockNumber, metadata)
	}

	return nil
}

// Pull the missing blocks, starting with one past the last block we have, and ending with the target block number
// (inclusive); that is, get (initBlockNumber, targetBlockNumber].
//
// When catching-up to a snapshot, we update `replication` and `comm` with each config block we bring.
// When pulling blocks during on-boarding, we do not, because the latest cluster-config comes from the join-block.
func (br *BlockReplicator) catchUpToBlock(initBlockNumber, targetBlockNumber uint64, updateConfig bool) error {
	for nextBlockNumber := initBlockNumber + 1; nextBlockNumber <= targetBlockNumber; {
		var blocks []*types.Block
		var err error
		blocksReadyCh := make(chan struct{})
		ctx, cancel := context.WithCancel(context.Background())

		//Try to pull some blocks in a go-routine so that we may cancel it if the server shuts down.
		//Note that `PullBlocks` will not necessarily return all the blocks we requested, hence the enclosing loop.
		go func() {
			defer close(blocksReadyCh)
			blocks, err = br.transport.PullBlocks(ctx, nextBlockNumber, targetBlockNumber, br.GetLeaderID())
		}()

		select {
		case <-br.stopCh:
			cancel()
			<-blocksReadyCh
			return &ierrors.ClosedError{ErrMsg: "server stopped during catch-up"}
		case <-blocksReadyCh:
			cancel()
			if err != nil {
				lastBlockNumber := br.getLastCommittedBlockNumber()
				switch err.(type) {
				case *ierrors.ClosedError:
					br.lg.Warnf("closing, stopping to pull blocks from cluster; last block number [%d]", lastBlockNumber)
					return err
				default:
					return errors.Wrapf(err, "failed to pull blocks from cluster; last block number [%d]", lastBlockNumber)
				}
			}

			br.lg.Infof("Going to commit [%d] blocks", len(blocks)) //Not necessarily the entire range requested!

			for _, blockToCommit := range blocks {
				br.lg.Infof("enqueue for commit block [%d], ConsensusMetadata: [%+v]",
					blockToCommit.GetHeader().GetBaseHeader().GetNumber(),
					blockToCommit.GetConsensusMetadata())

				if err := br.commitBlock(blockToCommit, updateConfig); err != nil {
					lastBlockNumber := br.getLastCommittedBlockNumber()
					switch err.(type) {
					case *ierrors.ClosedError:
						br.lg.Warnf("closing, stopping to pull blocks from cluster; last block number [%d]", lastBlockNumber)
						return err
					default:
						return err
					}
				}

				nextBlockNumber++
			}
		}
	}
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

			err := br.commitBlock(block, true)
			if err != nil {
				br.lg.Errorf("commit block error: %s, stopping block replicator", err.Error())
				return false
			}

		case raftpb.EntryConfChange:
			// For re-config, we use the V2 API, with `raftpb.EntryConfChangeV2` messages. These message happen only
			// when the raft node bootstraps, i.e. when `raft.StartNode` is called.
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

		case raftpb.EntryConfChangeV2:
			var ccV2 raftpb.ConfChangeV2
			if err := ccV2.Unmarshal(committedEntries[i].Data); err != nil {
				br.lg.Warnf("Failed to unmarshal ConfChangeV2 data: %s", err)
				continue
			}

			// we need to avoid re-committing a block that was created during membership config changes, but apply
			// the 'ConfChangeV2' to the raft state machine. This may happen when:
			// - existing nodes apply a config change but then restart from a snapshot prior to the config change;
			// - a node joins an existing cluster, as on-boarding brings all the ledger from a remote peer, and then we start raft.
			skipCommit := false
			if committedEntries[i].Index <= br.appliedIndex {
				br.lg.Debugf("Received config block with raft index (%d) <= applied index (%d), skip commit", committedEntries[i].Index, br.appliedIndex)
				skipCommit = true
			}

			if !skipCommit && ccV2.Context != nil {
				var block = &types.Block{}
				if err := proto.Unmarshal(ccV2.Context, block); err != nil {
					br.lg.Panicf("Error unmarshaling entry [#%d], entry: %+v, error: %s", i, committedEntries[i], err)
				}

				block.ConsensusMetadata = &types.ConsensusMetadata{
					RaftTerm:  committedEntries[i].Term,
					RaftIndex: committedEntries[i].Index,
				}

				err := br.commitBlock(block, true) // transport is reconfigured within after the block commits.
				if err != nil {
					br.lg.Errorf("commit block error: %s, stopping block replicator", err.Error())
					return false
				}
			}

			br.confState = *br.raftNode.ApplyConfChange(ccV2)
			br.lg.Infof("Applied config changes: %+v, current nodes in cluster: %+v", ccV2.Changes, br.confState.Voters)
			br.lg.Infof("Raft ConfState: %+v", br.confState)

			// TODO detect removal of leader?

			// detect removal of self
			removalOfSelf := true
			for _, id := range br.confState.Voters {
				if id == br.raftID {
					removalOfSelf = false
					break
				}
			}
			if removalOfSelf {
				br.lg.Warning("This node was removed from the cluster, replication is shutting down")
				return false
			}
		}

		// after commit, update appliedIndex
		if br.appliedIndex < committedEntries[i].Index {
			br.appliedIndex = committedEntries[i].Index
		}
	}

	// Take a snapshot if in-memory storage size exceeds the limit
	if br.accDataSize >= br.sizeLimit {
		var snapBlock = &types.Block{}
		var snapData []byte
		switch committedEntries[position].Type {
		case raftpb.EntryNormal:
			if err := proto.Unmarshal(committedEntries[position].Data, snapBlock); err != nil {
				br.lg.Panicf("Error unmarshaling Normal entry [#%d], entry: %+v, error: %s", position, committedEntries[position], err)
			}
			snapData = committedEntries[position].Data
		case raftpb.EntryConfChangeV2:
			var ccV2 raftpb.ConfChangeV2
			if err := ccV2.Unmarshal(committedEntries[position].Data); err != nil {
				br.lg.Panicf("Error unmarshaling ConfChangeV2 entry [#%d], entry: %+v, error: %s", position, committedEntries[position], err)
			}
			if ccV2.Context != nil {
				if err := proto.Unmarshal(ccV2.Context, snapBlock); err != nil {
					br.lg.Panicf("Error unmarshaling entry [#%d], entry: %+v, error: %s", position, committedEntries[position], err)
				}
				snapData = ccV2.Context
			}
		default:
			br.lg.Debug("entry [#%d], entry: %+v, does not contain a block, ignored", position, committedEntries[position])
		}

		if snapData != nil {
			if err := br.raftStorage.TakeSnapshot(br.appliedIndex, br.confState, snapData); err != nil {
				br.lg.Fatalf("Failed to create snapshot at index %d: %s", br.appliedIndex, err)
			}

			br.lg.Infof("Accumulated %d bytes since last snapshot, exceeding size limit (%d bytes), "+
				"taking snapshot at block [%d] (index: %d), last snapshotted block number is %d, current voters: %+v",
				br.accDataSize, br.sizeLimit, snapBlock.GetHeader().GetBaseHeader().GetNumber(), br.appliedIndex, br.lastSnapBlockNum, br.confState.Voters)

			br.accDataSize = 0
			br.lastSnapBlockNum = snapBlock.GetHeader().GetBaseHeader().GetNumber()
		}
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
			var addedPeers, removedPeers []*types.PeerConfig
			var isMembershipConfig bool

			if utils.IsConfigBlock(blockToPropose) {
				configTxEnv := blockToPropose.GetConfigTxEnvelope()
				// We validate a config TX preorder, to prevent a membership change to be applied to Raft, and then
				// have the TX marked invalid during commit. Note that ConfigTxValidator.Validate reads the current
				// config from the store.
				// However, together with the code to prevent in-flight config blocks before proposing a new one, the
				// config from the store should match the config stored in the BlockReplicator.
				valInfo, errVal := br.configTxValidator.Validate(configTxEnv)
				if errVal != nil {
					br.lg.Errorf("Failed to validate configTxEnv, internal error: %s", errVal)
					br.releasePendingTXs(blockToPropose, "Declined to propose block, internal error in Validate", errVal)
					continue Propose_Loop
				}
				if valInfo.Flag != types.Flag_VALID {
					br.releasePendingTXs(blockToPropose, "Declined to propose block, invalid config tx",
						&ierrors.BadRequestError{ErrMsg: fmt.Sprintf("Invalid config tx, reason: %s", valInfo.ReasonIfInvalid)})
					continue Propose_Loop
				}

				newClusterConfig := configTxEnv.GetPayload().GetNewConfig()
				_, consensus, _, _, _ := ClassifyClusterReConfig(br.clusterConfig, newClusterConfig)
				if consensus {
					var errDetect error
					addedPeers, removedPeers, _, errDetect = detectPeerConfigChanges(br.clusterConfig.ConsensusConfig, newClusterConfig.ConsensusConfig)
					if errDetect != nil {
						br.releasePendingTXs(blockToPropose, "Declined to propose block, internal error in detect peer config change", errDetect)
						continue Propose_Loop
					}
					isMembershipConfig = len(addedPeers)+len(removedPeers) > 0
				}
			}

			if !isMembershipConfig {
				// Data-Tx, User-admin-Tx, DB-admin-Tx, as well as Config-Tx that do not membership changes
				if ok := br.proposeRegular(blockToPropose); !ok {
					continue Propose_Loop
				}
			} else {
				//  Config-Tx: with consensus membership change
				if ok := br.proposeMembershipConfigChange(blockToPropose, addedPeers, removedPeers); !ok {
					continue Propose_Loop
				}
			}

			br.updateLastProposal(blockToPropose)

		case <-br.stopCh:
			br.lg.Debug("Stopping block replicator")
			break Propose_Loop
		}
	}

	br.lg.Info("Exiting the block replicator propose loop")
}

// proposeRegular proposes the block to Raft as a regular message.
func (br *BlockReplicator) proposeRegular(blockToPropose *types.Block) bool {
	ctx, blockBytes, doPropose := br.prepareProposal(blockToPropose)
	if !doPropose {
		return false
	}

	// Propose to raft: the call to raft.Node.Propose() may block when a leader loses its leadership and has no quorum.
	// It is cancelled when the node loses leadership, by the event-loop go-routine.
	err := br.raftNode.Propose(ctx, blockBytes)
	if err != nil {
		br.releasePendingTXs(blockToPropose, "Failed to propose block", err)
		return false
	}

	return true
}

// proposeMembershipConfigChange propose membership config changes, that is, adding or removing a peer.
// This is proposed in a 'raftpb.ConfChangeV2' message using the 'ProposeConfChange' API. This call consents on the
// Raft membership change as well as on the config block, which is given as the 'ConfChangeV2.Context'.
// The return value signals whether the proposal completed correctly.
func (br *BlockReplicator) proposeMembershipConfigChange(blockToPropose *types.Block, addedPeers, removedPeers []*types.PeerConfig) bool {
	//  consensus membership config change
	ctx, blockBytes, doPropose := br.prepareProposal(blockToPropose)
	if !doPropose {
		return false
	}

	ccV2 := &raftpb.ConfChangeV2{
		Transition: raftpb.ConfChangeTransitionAuto,
		Context:    blockBytes,
	}
	for _, peer := range addedPeers {
		ccV2.Changes = append(ccV2.Changes, raftpb.ConfChangeSingle{
			Type:   raftpb.ConfChangeAddNode,
			NodeID: peer.RaftId,
		})
	}
	for _, peer := range removedPeers {
		ccV2.Changes = append(ccV2.Changes, raftpb.ConfChangeSingle{
			Type:   raftpb.ConfChangeRemoveNode,
			NodeID: peer.RaftId,
		})
	}

	br.lg.Infof("Going to propose membership config changes: %v", ccV2.Changes)
	// ProposeConfChange to raft: the call to raft.Node.ProposeConfChange() may block when a leader loses
	// its leadership and has no quorum.
	// It is cancelled when the node loses leadership, by the event-loop go-routine.
	err := br.raftNode.ProposeConfChange(ctx, ccV2)
	if err != nil {
		br.releasePendingTXs(blockToPropose, "Failed to propose block", err)
		return false
	}

	return true
}

func (br *BlockReplicator) releasePendingTXs(blockToPropose *types.Block, reasonMsg string, reasonErr error) {
	br.lg.Infof("%s: %+v; because: %s", reasonMsg, blockToPropose.GetHeader(), reasonErr)
	txIDs, err := utils.BlockPayloadToTxIDs(blockToPropose.GetPayload())
	if err != nil {
		br.lg.Errorf("Failed to extract TxIDs from block, dropping block: %v; error: %s", blockToPropose.GetHeader(), err)
		return
	}

	br.pendingTxs.ReleaseWithError(txIDs, reasonErr)
}

// prepareProposal Prepares the Raft proposal context and bytes, and determine whether to propose (only the leader can
// propose). This also numbers the block and sets the base header hash.
func (br *BlockReplicator) prepareProposal(blockToPropose *types.Block) (ctx context.Context, blockBytes []byte, doPropose bool) {
	br.mutex.Lock()

	if errLeader := br.isLeaderReady(); errLeader != nil {
		br.mutex.Unlock() //do not call the pendingTxs component with a mutex locked

		br.releasePendingTXs(blockToPropose, "Declined to propose block", errLeader)

		return nil, nil, false //skip proposing
	}

	// number the block and set the base header hash
	br.insertBlockBaseHeader(blockToPropose)

	var err error
	blockBytes, err = proto.Marshal(blockToPropose)
	if err != nil {
		br.lg.Panicf("Error marshaling a block: %s", err)
	}

	ctx, br.cancelProposeContext = context.WithCancel(context.Background())

	br.mutex.Unlock()

	return ctx, blockBytes, true
}

// updateLastProposal updates the last block proposed in order to keep track of block numbering.
// In addition, keep track of proposed config, and prevent additional proposals when one is already in flight.
// We are preventing all proposals when a config is in flight. This is not strictly necessary, but is easier
// to implement. The implications on steady state performance are negligible, as config transactions are rare.
func (br *BlockReplicator) updateLastProposal(lastBlockProposed *types.Block) {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	if br.isLeader() == nil {
		br.lastProposedBlockNumber = lastBlockProposed.GetHeader().GetBaseHeader().GetNumber()
		if baseHash, err := blockstore.ComputeBlockBaseHash(lastBlockProposed); err == nil {
			br.lastProposedBlockHeaderBaseHash = baseHash
		} else {
			br.lg.Panicf("Failed to compute last block base hash: %s", err)
		}
		br.numInFlightBlocks++
		if utils.IsConfigBlock(lastBlockProposed) {
			if br.inFlightConfigBlockNumber > 0 {
				br.lg.Panicf("A config block was proposed, but there is still a config block in-flight! proposed: %d, in-flight: %d",
					lastBlockProposed.GetHeader().GetBaseHeader().GetNumber(), br.inFlightConfigBlockNumber)
			}

			br.inFlightConfigBlockNumber = lastBlockProposed.GetHeader().GetBaseHeader().GetNumber()
		}

		tooManyInFlightBlocksCond := func() bool {
			return br.numInFlightBlocks > br.clusterConfig.ConsensusConfig.RaftConfig.MaxInflightBlocks
		}
		inFlightConfigCond := func() bool {
			return br.inFlightConfigBlockNumber > 0
		}

		if tooManyInFlightBlocksCond() || inFlightConfigCond() {
			if tooManyInFlightBlocksCond() {
				br.lg.Debugf("Number of in-flight blocks exceeds max, %d > %d, waiting for blocks to commit", //Tested side effect
					br.numInFlightBlocks, br.clusterConfig.ConsensusConfig.RaftConfig.MaxInflightBlocks)
			}
			if inFlightConfigCond() {
				br.lg.Debugf("In-flight config block [%d], waiting for block to commit", //Tested side effect
					br.inFlightConfigBlockNumber)
			}

			for tooManyInFlightBlocksCond() || inFlightConfigCond() {
				// the go-routine will be notified by the event-loop go-routine when:
				// - a block commits, or
				// - the config block in-flight committed
				// - when leadership is lost or assumed, or
				// - when the event-loop go-routine exits. This is done in order to remain
				//   reactive to server shutdown while waiting for blocks to commit.
				br.condTooManyInFlightBlocks.Wait()
			}
			br.lg.Debugf("Number of in-flight blocks back to normal: %d", br.numInFlightBlocks)
			br.lg.Debugf("No in-flight config block: %d", br.inFlightConfigBlockNumber)
		}
	}
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
		br.lastKnownLeaderHost = ""

		err = nil
	})

	return err
}

func (br *BlockReplicator) IsLeader() *ierrors.NotLeaderError {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	return br.isLeaderReady()
}

func (br *BlockReplicator) isLeader() *ierrors.NotLeaderError {
	if br.lastKnownLeader == br.raftID {
		return nil
	}

	return &ierrors.NotLeaderError{
		LeaderID: br.lastKnownLeader, LeaderHostPort: br.lastKnownLeaderHost}
}

// isLeaderReady determines if this node is the leader, and if it is ready for proposing blocks.
// If this node is the leader, and it is ready, it returns nil.
// If this node is the leader, but it is not ready, it returns an empty `NotLeaderError` error.
// If this node is not the leader, it returns the last know leader in a `NotLeaderError` error.
// A leader is not ready when it was just elected and still has in-flight blocks.
func (br *BlockReplicator) isLeaderReady() *ierrors.NotLeaderError {
	if br.lastKnownLeader == br.raftID {
		if br.justElectedInFlightBlocks {
			// this node was just elected leader, but may be processing in-flight messages
			return &ierrors.NotLeaderError{}
		}
		return nil
	}

	return &ierrors.NotLeaderError{
		LeaderID: br.lastKnownLeader, LeaderHostPort: br.lastKnownLeaderHost}
}

func (br *BlockReplicator) GetLeaderID() uint64 {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	return br.lastKnownLeader
}

func (br *BlockReplicator) GetClusterStatus() (leaderID uint64, activePeers map[string]*types.PeerConfig) {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	activePeers = br.transport.ActivePeers(500*time.Millisecond, true)

	if br.lastKnownLeader == br.raftID && br.justElectedInFlightBlocks {
		leaderID = 0 // it is this node, but it is not ready yet
	} else {
		leaderID = br.lastKnownLeader
	}

	return
}

// Commit the block to the ledger and DB.
//
// If the block is a config block, update the cluster config if `updateConfig` is true.
// When catching-up to a snapshot, we update `replication` and `comm` with each config block we bring.
// When pulling blocks during on-boarding, we do not, because the latest cluster-config comes from the join-block.
func (br *BlockReplicator) commitBlock(block *types.Block, updateConfig bool) error {
	blockNumber := block.GetHeader().GetBaseHeader().GetNumber()
	br.lg.Infof("Enqueue for commit block [%d], ConsensusMetadata: %+v ",
		blockNumber, block.GetConsensusMetadata())

	// we can only get a valid config transaction
	reConfig, err := br.oneQueueBarrier.EnqueueWait(block)
	if err != nil {
		return err
	}

	br.setLastCommittedBlock(block)

	if reConfig == nil {
		return nil
	}

	clusterConfig := reConfig.(*types.ClusterConfig)
	if !updateConfig {
		br.lg.Infof("Skipping re-config update: block number [%d], ClusterConfig: %+v", blockNumber, clusterConfig)
		return nil
	}

	if err := br.updateClusterConfig(clusterConfig); err != nil {
		br.lg.Panicf("Failed to update to ClusterConfig during commitBlock: error: %s", err)
	}

	return nil
}

func (br *BlockReplicator) setLastCommittedBlock(block *types.Block) {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	br.lastCommittedBlock = block

	var doBroadcast bool

	if br.numInFlightBlocks > 0 { // only reduce on the leader
		br.numInFlightBlocks--
		doBroadcast = true
	}

	if br.inFlightConfigBlockNumber > 0 && utils.IsConfigBlock(block) { // only check on the leader
		if br.inFlightConfigBlockNumber == block.GetHeader().GetBaseHeader().GetNumber() {
			br.inFlightConfigBlockNumber = 0
			doBroadcast = true
		} else {
			br.lg.Panicf("A config block committed, but it is not the one expected to be in-flight! committed: %d, expected: %d",
				block.GetHeader().GetBaseHeader().GetNumber(), br.inFlightConfigBlockNumber)
		}
	}

	if doBroadcast {
		br.condTooManyInFlightBlocks.Broadcast()
	}
}

func (br *BlockReplicator) getLastCommittedBlockNumber() uint64 {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	return br.lastCommittedBlock.GetHeader().GetBaseHeader().GetNumber()
}

func (br *BlockReplicator) updateClusterConfig(clusterConfig *types.ClusterConfig) error {
	br.lg.Infof("New cluster config committed, going to apply to block replicator: %+v", clusterConfig)

	br.mutex.Lock()
	defer br.mutex.Unlock()

	// We must not receive any errors here, because that would render the config-tx invalid, and invalid config txs
	// are not transferred to the BlockReplicator.
	addedPeers, removedPeers, changedPeers, err := detectPeerConfigChanges(br.clusterConfig.ConsensusConfig, clusterConfig.ConsensusConfig)
	if err != nil {
		return errors.Wrap(err, "failed to detect peer config changes")
	}

	err = br.transport.UpdatePeers(addedPeers, removedPeers, changedPeers, clusterConfig)
	if err != nil {
		return errors.Wrap(err, "failed to update peers on transport")
	}

	br.clusterConfig = clusterConfig

	return nil
}

func (br *BlockReplicator) nodeHostPortFromRaftID(raftID uint64) string {
	if raftID == 0 {
		return ""
	}

	var nodeID string
	for _, p := range br.clusterConfig.ConsensusConfig.Members {
		if p.RaftId == raftID {
			nodeID = p.NodeId
			break
		}
	}

	if nodeID == "" {
		br.lg.Warnf("not found: no member with RaftID: %d", raftID)
		return ""
	}

	for _, n := range br.clusterConfig.Nodes {
		if n.Id == nodeID {
			hostPort := fmt.Sprintf("%s:%d", n.Address, n.Port)
			return hostPort
		}
	}

	br.lg.Warnf("not found: no node with NodeID: %s, RaftID: %d", nodeID, raftID)
	return ""
}

// when the node is on-boarding, the raftNode is not yet constructed and is nil.
func (br *BlockReplicator) isOnBoarding() bool {
	return br.joinExistingCluster && !br.finishedJoin
}

// Process incoming raft messages
func (br *BlockReplicator) Process(ctx context.Context, m raftpb.Message) error {
	br.lg.Debugf("Process incoming raft message: %+v", m)

	br.mutex.Lock()
	defer br.mutex.Unlock()

	if br.isOnBoarding() {
		br.lg.Debugf("Rejected raft message from peer, not ready, on-boarding in progress: %v", m)
		return &RaftHTTPError{Code: http.StatusServiceUnavailable, Message: "rejected raft message, not ready, on-boarding in progress"}
	}

	if br.isIDRemoved(m.From) {
		br.lg.Warningf("Rejected raft message from removed peer: %v", m)
		return &RaftHTTPError{Code: http.StatusForbidden, Message: "rejected raft message from removed peer"}
	}

	err := br.raftNode.Step(ctx, m)
	if err != nil {
		br.lg.Errorf("Error during raft node Step: %s", err)
	}
	return err
}

func (br *BlockReplicator) isIDRemoved(id uint64) bool {
	// look into the cluster config and check whether this RaftID was removed.
	for _, member := range br.clusterConfig.GetConsensusConfig().GetMembers() {
		if member.RaftId == id {
			br.lg.Debugf("isIDRemoved: %d, false", id)
			return false
		}
	}

	br.lg.Debugf("isIDRemoved: %d, true", id)
	return true
}

func (br *BlockReplicator) IsIDRemoved(id uint64) bool {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	return br.isIDRemoved(id)
}

func (br *BlockReplicator) ReportUnreachable(id uint64) {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	if br.isOnBoarding() {
		br.lg.Debugf("ReportUnreachable: %d, ignored, on-boarding", id)
		return
	}

	br.lg.Debugf("ReportUnreachable: %d", id)
	br.raftNode.ReportUnreachable(id)
}

func (br *BlockReplicator) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	if br.isOnBoarding() {
		br.lg.Debugf("ReportSnapshot: %d, ignored, on-boarding", id)
		return
	}

	br.lg.Debugf("ReportSnapshot: %d, %v", id, status)
	br.raftNode.ReportSnapshot(id, status)
}

// called inside a br.mutex.Lock()
func (br *BlockReplicator) insertBlockBaseHeader(proposedBlock *types.Block) {
	blockNum := br.lastProposedBlockNumber + 1
	baseHeader := &types.BlockHeaderBase{
		Number:                 blockNum,
		PreviousBaseHeaderHash: br.lastProposedBlockHeaderBaseHash,
	}

	if blockNum > 1 {
		lastCommittedBlockNum := br.lastCommittedBlock.GetHeader().GetBaseHeader().GetNumber()
		lastCommittedBlockHash, err := blockstore.ComputeBlockHash(br.lastCommittedBlock)
		if err != nil {
			br.lg.Panicf("Error while creating block header for proposed block: %d; possible problems at last commited block header: %+v; error: %s",
				blockNum, br.lastCommittedBlock.GetHeader(), err)
		}
		baseHeader.LastCommittedBlockHash = lastCommittedBlockHash
		baseHeader.LastCommittedBlockNum = lastCommittedBlockNum
	}

	proposedBlock.Header = &types.BlockHeader{BaseHeader: baseHeader}
}
