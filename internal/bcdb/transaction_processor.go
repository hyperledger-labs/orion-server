// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/blockcreator"
	"github.com/hyperledger-labs/orion-server/internal/blockprocessor"
	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	"github.com/hyperledger-labs/orion-server/internal/comm"
	internalerror "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/mptrie"
	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/internal/queue"
	"github.com/hyperledger-labs/orion-server/internal/replication"
	"github.com/hyperledger-labs/orion-server/internal/txreorderer"
	"github.com/hyperledger-labs/orion-server/internal/txvalidation"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	commitListenerName = "transactionProcessor"
)

type transactionProcessor struct {
	nodeID               string
	txQueue              *queue.Queue
	txBatchQueue         *queue.Queue
	blockOneQueueBarrier *queue.OneQueueBarrier
	txReorderer          *txreorderer.TxReorderer
	blockCreator         *blockcreator.BlockCreator
	blockReplicator      *replication.BlockReplicator
	peerTransport        *comm.HTTPTransport
	blockProcessor       *blockprocessor.BlockProcessor
	blockStore           *blockstore.Store
	pendingTxs           *queue.PendingTxs
	logger               *logger.SugarLogger
	metrics              *utils.TxProcessingMetrics
}

type txProcessorConfig struct {
	config          *config.Configurations
	db              worldstate.DB
	blockStore      *blockstore.Store
	provenanceStore *provenance.Store
	stateTrieStore  mptrie.Store
	metricsRegistry *prometheus.Registry
	logger          *logger.SugarLogger
}

func newTransactionProcessor(conf *txProcessorConfig) (*transactionProcessor, error) {
	p := &transactionProcessor{}

	localConfig := conf.config.LocalConfig

	p.nodeID = localConfig.Server.Identity.ID
	p.logger = conf.logger
	p.txQueue = queue.New(localConfig.Server.QueueLength.Transaction)
	p.txBatchQueue = queue.New(localConfig.Server.QueueLength.ReorderedTransactionBatch)
	p.blockOneQueueBarrier = queue.NewOneQueueBarrier(conf.logger)
	p.pendingTxs = queue.NewPendingTxs(conf.logger)
	p.metrics = utils.NewTxProcessingMetrics(conf.metricsRegistry)

	p.txReorderer = txreorderer.New(
		&txreorderer.Config{
			TxQueue:            p.txQueue,
			TxBatchQueue:       p.txBatchQueue,
			MaxTxCountPerBatch: localConfig.BlockCreation.MaxTransactionCountPerBlock,
			BatchTimeout:       localConfig.BlockCreation.BlockTimeout,
			Logger:             conf.logger,
			Metrics:            p.metrics,
		},
	)

	var err error

	// The txValidator is used by the block processor (commit-phase) as well as by some pre-order components that need
	// it (or one of its sub-components), e.g. the config-validator is used by the block-replicator.
	txValidator := txvalidation.NewValidator(
		&txvalidation.Config{
			DB:      conf.db,
			Logger:  conf.logger,
			Metrics: p.metrics,
		},
	)

	p.blockProcessor = blockprocessor.New(
		&blockprocessor.Config{
			BlockOneQueueBarrier: p.blockOneQueueBarrier,
			BlockStore:           conf.blockStore,
			ProvenanceStore:      conf.provenanceStore,
			StateTrieStore:       conf.stateTrieStore,
			DB:                   conf.db,
			TxValidator:          txValidator,
			Logger:               conf.logger,
			Metrics:              p.metrics,
		},
	)

	ledgerHeight, err := conf.blockStore.Height()
	if err != nil {
		return nil, err
	}
	if ledgerHeight == 0 {
		p.logger.Info("Ledger is empty")
		if conf.config.SharedConfig != nil {
			p.logger.Info("Bootstrapping the ledger and database from SharedConfiguration")
			tx, err := PrepareBootstrapConfigTx(conf.config)
			if err != nil {
				return nil, err
			}
			bootBlock, err := blockcreator.BootstrapBlock(tx)
			if err != nil {
				return nil, err
			}
			if err = p.blockProcessor.Bootstrap(bootBlock, conf.config.SharedConfig.Ledger); err != nil {
				return nil, err
			}
			ledgerHeight = 1 // genesis block generated
		} else if conf.config.JoinBlock != nil {
			p.logger.Infof("Bootstrapping the ledger and database from the cluster using a join block, number: %d",
				conf.config.JoinBlock.GetHeader().GetBaseHeader().GetNumber())
		} else {
			return nil, errors.New("missing bootstrap, no SharedConfig or JoinBlock")
		}
	}

	p.blockCreator, err = blockcreator.New(
		&blockcreator.Config{
			TxBatchQueue: p.txBatchQueue,
			Logger:       conf.logger,
			BlockStore:   conf.blockStore,
			PendingTxs:   p.pendingTxs,
		},
	)
	if err != nil {
		return nil, err
	}

	p.peerTransport, err = comm.NewHTTPTransport(&comm.Config{
		LocalConf:    localConfig,
		Logger:       conf.logger,
		LedgerReader: conf.blockStore,
	})
	if err != nil {
		return nil, err
	}

	var clusterConfig *types.ClusterConfig
	// A 'normal start' is when the server has the most current config known to it in the DB (and ledger), and has no
	// join-block. This can happen when:
	// - the server starts from genesis, or
	// - had a join-block in the past to join the cluster but the join-block was removed after the server had caught up.
	normalStart := conf.config.JoinBlock == nil
	// A 'join start' is when the server starts with a join block, either with an empty ledger, or a ledger that is
	// behind the join-block. This means that the join-block has the most recent config, and not the DB.
	joinStart := !normalStart && (ledgerHeight < conf.config.JoinBlock.GetHeader().GetBaseHeader().GetNumber())
	// A 'completed join start' is when the server starts with a join block, but the ledger is at or beyond the
	// join-block. This means that the join process had completed and that the DB (and ledger) has the most recent
	// config.
	completedJoinStart := !normalStart && (ledgerHeight >= conf.config.JoinBlock.GetHeader().GetBaseHeader().GetNumber())

	switch {
	case normalStart, completedJoinStart:
		clusterConfig, _, err = conf.db.GetConfig()
		if err != nil {
			return nil, err
		}
		conf.logger.Debugf("Using cluster config from DB: %+v", clusterConfig)

	case joinStart:
		clusterConfig = conf.config.JoinBlock.GetPayload().(*types.Block_ConfigTxEnvelope).ConfigTxEnvelope.GetPayload().NewConfig
		conf.logger.Debugf("Using cluster config from join-block: %+v", clusterConfig)

	default:
		return nil, errors.New("programming error, one of: 'normalStart || completedJoinStart || joinStart' must be true!")
	}

	// We disable the MPTrie after we know what the cluster config is. It is enabled by default.
	if clusterConfig.GetLedgerConfig() != nil && clusterConfig.LedgerConfig.StateMerklePatriciaTrieDisabled {
		conf.stateTrieStore.SetDisabled(true)
	}

	if err = p.peerTransport.SetClusterConfig(clusterConfig); err != nil {
		return nil, err
	}

	repConfig := &replication.Config{
		LocalConf:            localConfig,
		ClusterConfig:        clusterConfig,
		LedgerReader:         conf.blockStore,
		Transport:            p.peerTransport,
		BlockOneQueueBarrier: p.blockOneQueueBarrier,
		PendingTxs:           p.pendingTxs,
		ConfigValidator:      txValidator.ConfigValidator(),
		Logger:               conf.logger,
		Metrics:              p.metrics,
	}
	if joinStart {
		repConfig.JoinBlock = conf.config.JoinBlock
	}

	p.blockReplicator, err = replication.NewBlockReplicator(repConfig)
	if err != nil {
		return nil, err
	}

	if err = p.peerTransport.SetConsensusListener(p.blockReplicator); err != nil {
		return nil, err
	}
	p.blockCreator.RegisterReplicator(p.blockReplicator)

	if err = p.blockProcessor.RegisterBlockCommitListener(commitListenerName, p); err != nil {
		return nil, err
	}

	go p.txReorderer.Start()
	p.txReorderer.WaitTillStart()

	go p.blockCreator.Start()
	p.blockCreator.WaitTillStart()

	err = p.peerTransport.Start() // Starts internal goroutine
	if err != nil {
		return nil, err
	}

	p.blockReplicator.Start() // Starts internal goroutine

	go p.blockProcessor.Start()
	p.blockProcessor.WaitTillStart()

	p.blockStore = conf.blockStore

	return p, nil
}

// SubmitTransaction enqueue the transaction to the transaction queue
// If the timeout is set to 0, the submission would be treated as async while
// a non-zero timeout would be treated as a sync submission. When a timeout
// occurs with the sync submission, a timeout error will be returned
func (t *transactionProcessor) SubmitTransaction(tx interface{}, timeout time.Duration) (*types.TxReceiptResponse, error) {
	submitTimer := t.metrics.NewLatencyTimer("tx-submit")

	var txID string
	switch tx.(type) {
	case *types.DataTxEnvelope:
		txID = tx.(*types.DataTxEnvelope).Payload.TxId
	case *types.UserAdministrationTxEnvelope:
		txID = tx.(*types.UserAdministrationTxEnvelope).Payload.TxId
	case *types.DBAdministrationTxEnvelope:
		txID = tx.(*types.DBAdministrationTxEnvelope).Payload.TxId
	case *types.ConfigTxEnvelope:
		txID = tx.(*types.ConfigTxEnvelope).Payload.TxId
	default:
		return nil, errors.Errorf("unexpected transaction type")
	}

	if err := constants.SafeURLSegmentNZ(txID); err != nil {
		return nil, &internalerror.BadRequestError{ErrMsg: errors.WithMessage(err, "bad TxId").Error()}
	}

	if err := t.IsLeader(); err != nil {
		return nil, err
	}

	// We attempt to insert the txID atomically.
	// If we succeed, then future TX fill fail at this point.
	// However, if the TX already exists in the block store, then we will fail the subsequent check.
	// Since a TX will be removed from the pending queue only after it is inserted to the block store,
	// then it is guaranteed that we won't use the same txID twice.
	// TODO: add limit on the number of pending sync tx
	promise := queue.NewCompletionPromise(timeout)
	if existed := t.pendingTxs.Add(txID, promise); existed {
		return nil, &internalerror.DuplicateTxIDError{TxID: txID}
	}

	duplicate, err := t.blockStore.DoesTxIDExist(txID)
	if err != nil || duplicate {
		t.pendingTxs.DeleteWithNoAction(txID)
		if err == nil {
			err = &internalerror.DuplicateTxIDError{TxID: txID}
		}
		return nil, err
	}

	// Avoids marshaling the TX in production mode
	if t.logger.IsDebug() {
		if jsonBytes, err := json.MarshalIndent(tx, "", "\t"); err != nil {
			t.logger.Debugf("failed to marshal transaction: %v", err)
		} else {
			t.logger.Debugf("enqueuing transaction %s\n", jsonBytes)
		}
	}

	enqueueTimer := t.metrics.NewLatencyTimer("tx-enqueue")
	if timeout <= 0 {
		// Enqueue will block until the queue is not full
		t.txQueue.Enqueue(tx)
	} else {
		// EnqueueWithTimeout will block until the queue is not full or timeout occurs
		if success := t.txQueue.EnqueueWithTimeout(tx, timeout); !success {
			t.pendingTxs.DeleteWithNoAction(txID)
			return nil, &internalerror.TimeoutErr{ErrMsg: "timeout has occurred while inserting the transaction to the queue"}
		}
	}
	enqueueTimer.Observe()
	t.logger.Debug("transaction is enqueued for re-ordering")
	t.metrics.QueueSize("tx", t.txQueue.Size())
	t.metrics.QueueSize("pending", t.pendingTxs.Size())

	receipt, err := promise.Wait()

	if err != nil {
		return nil, err
	}

	submitTimer.Observe()
	return &types.TxReceiptResponse{
		Receipt: receipt,
	}, nil
}

func (t *transactionProcessor) PostBlockCommitProcessing(block *types.Block) error {
	t.logger.Debugf("received commit event for block[%d]", block.GetHeader().GetBaseHeader().GetNumber())

	var txIDs []string

	switch block.Payload.(type) {
	case *types.Block_DataTxEnvelopes:
		dataTxEnvs := block.GetDataTxEnvelopes().Envelopes
		for _, tx := range dataTxEnvs {
			txIDs = append(txIDs, tx.Payload.TxId)
		}

	case *types.Block_UserAdministrationTxEnvelope:
		userTxEnv := block.GetUserAdministrationTxEnvelope()
		txIDs = append(txIDs, userTxEnv.Payload.TxId)

	case *types.Block_DbAdministrationTxEnvelope:
		dbTxEnv := block.GetDbAdministrationTxEnvelope()
		txIDs = append(txIDs, dbTxEnv.Payload.TxId)

	case *types.Block_ConfigTxEnvelope:
		configTxEnv := block.GetConfigTxEnvelope()
		txIDs = append(txIDs, configTxEnv.Payload.TxId)

	default:
		return errors.Errorf("unexpected transaction envelope in the block")
	}

	go t.pendingTxs.DoneWithReceipt(txIDs, block.Header)

	return nil
}

func (t *transactionProcessor) Close() error {
	// It is safe to use without locks because all the following calls are protected internally.
	t.txReorderer.Stop()
	t.blockCreator.Stop()
	_ = t.blockReplicator.Close()
	t.peerTransport.Close()
	t.blockProcessor.Stop()

	return nil
}

func (t *transactionProcessor) IsLeader() *internalerror.NotLeaderError {
	// It is safe to use without locks because the following call is protected internally.
	return t.blockReplicator.IsLeader()
}

// ClusterStatus returns the leader NodeID, and the active nodes NodeIDs.
// Note: leader is always in active.
func (t *transactionProcessor) ClusterStatus() (leader string, active []string) {
	// It is safe to use without locks because the following call is protected internally.
	leaderID, activePeers := t.blockReplicator.GetClusterStatus()
	for _, peer := range activePeers {
		active = append(active, peer.NodeId)
		if peer.RaftId == leaderID {
			leader = peer.NodeId
		}
	}

	return
}

func PrepareBootstrapConfigTx(conf *config.Configurations) (*types.ConfigTxEnvelope, error) {
	certs, err := readCerts(conf)
	if err != nil {
		return nil, err
	}

	inNodes := false
	var nodes []*types.NodeConfig
	for _, node := range conf.SharedConfig.Nodes {
		nc := &types.NodeConfig{
			Id:      node.NodeID,
			Address: node.Host,
			Port:    node.Port,
		}
		if cert, ok := certs.nodeCertificates[node.NodeID]; ok {
			nc.Certificate = cert
		} else {
			return nil, errors.Errorf("Cannot find certificate for node: %s", node.NodeID)
		}
		nodes = append(nodes, nc)

		if node.NodeID == conf.LocalConfig.Server.Identity.ID {
			inNodes = true
		}
	}
	if !inNodes {
		return nil, errors.Errorf("Cannot find local Server.Identity.ID [%s] in SharedConfig.Nodes: %v", conf.LocalConfig.Server.Identity.ID, conf.SharedConfig.Nodes)
	}

	var maxRaftID uint64
	for _, m := range conf.SharedConfig.Consensus.Members {
		if m.RaftId > maxRaftID {
			maxRaftID = m.RaftId
		}
	}

	clusterConfig := &types.ClusterConfig{
		Nodes: nodes,
		Admins: []*types.Admin{
			{
				Id:          conf.SharedConfig.Admin.ID,
				Certificate: certs.adminCert,
			},
		},
		CertAuthConfig: certs.caCerts,
		ConsensusConfig: &types.ConsensusConfig{
			Algorithm: conf.SharedConfig.Consensus.Algorithm,
			Members:   make([]*types.PeerConfig, len(conf.SharedConfig.Consensus.Members)),
			Observers: make([]*types.PeerConfig, len(conf.SharedConfig.Consensus.Observers)),
			RaftConfig: &types.RaftConfig{
				TickInterval:         conf.SharedConfig.Consensus.RaftConfig.TickInterval,
				ElectionTicks:        conf.SharedConfig.Consensus.RaftConfig.ElectionTicks,
				HeartbeatTicks:       conf.SharedConfig.Consensus.RaftConfig.HeartbeatTicks,
				MaxInflightBlocks:    conf.SharedConfig.Consensus.RaftConfig.MaxInflightBlocks,
				SnapshotIntervalSize: conf.SharedConfig.Consensus.RaftConfig.SnapshotIntervalSize,
				MaxRaftId:            maxRaftID,
			},
		},
		LedgerConfig: &types.LedgerConfig{
			StateMerklePatriciaTrieDisabled: conf.SharedConfig.Ledger.StateMerklePatriciaTrieDisabled,
		},
	}

	inMembers := false
	for i, m := range conf.SharedConfig.Consensus.Members {
		clusterConfig.ConsensusConfig.Members[i] = &types.PeerConfig{
			NodeId:   m.NodeId,
			RaftId:   m.RaftId,
			PeerHost: m.PeerHost,
			PeerPort: m.PeerPort,
		}
		if m.NodeId == conf.LocalConfig.Server.Identity.ID {
			inMembers = true
		}
	}

	inObservers := false
	for i, m := range conf.SharedConfig.Consensus.Observers {
		clusterConfig.ConsensusConfig.Observers[i] = &types.PeerConfig{
			NodeId:   m.NodeId,
			RaftId:   m.RaftId,
			PeerHost: m.PeerHost,
			PeerPort: m.PeerPort,
		}
		if m.NodeId == conf.LocalConfig.Server.Identity.ID {
			inObservers = true
		}
	}

	if !inMembers && !inObservers {
		return nil, errors.Errorf("Cannot find local Server.Identity.ID [%s] in SharedConfig.Consensus Members or Observers: %v",
			conf.LocalConfig.Server.Identity.ID, conf.SharedConfig.Consensus)
	}
	if inObservers && inMembers {
		return nil, errors.Errorf("local Server.Identity.ID [%s] cannot be in SharedConfig.Consensus both Members and Observers: %v",
			conf.LocalConfig.Server.Identity.ID, conf.SharedConfig.Consensus)
	}
	// TODO add support for observers, see issue: https://github.ibm.com/blockchaindb/server/issues/403
	if inObservers {
		return nil, errors.Errorf("not supported yet: local Server.Identity.ID [%s] is in SharedConfig.Consensus.Observers: %v",
			conf.LocalConfig.Server.Identity.ID, conf.SharedConfig.Consensus)
	}

	return &types.ConfigTxEnvelope{
		Payload: &types.ConfigTx{
			TxId:      uuid.New().String(),
			NewConfig: clusterConfig,
		},
		// TODO: we can make the node itself sign the transaction
	}, nil
}
