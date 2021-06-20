// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/config"
	"github.com/IBM-Blockchain/bcdb-server/internal/blockcreator"
	"github.com/IBM-Blockchain/bcdb-server/internal/blockprocessor"
	"github.com/IBM-Blockchain/bcdb-server/internal/blockstore"
	"github.com/IBM-Blockchain/bcdb-server/internal/comm"
	internalerror "github.com/IBM-Blockchain/bcdb-server/internal/errors"
	"github.com/IBM-Blockchain/bcdb-server/internal/mptrie"
	"github.com/IBM-Blockchain/bcdb-server/internal/provenance"
	"github.com/IBM-Blockchain/bcdb-server/internal/queue"
	"github.com/IBM-Blockchain/bcdb-server/internal/replication"
	"github.com/IBM-Blockchain/bcdb-server/internal/txreorderer"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
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
	pendingTxs           *pendingTxs
	logger               *logger.SugarLogger
	sync.Mutex
}

type txProcessorConfig struct {
	config          *config.Configurations
	db              worldstate.DB
	blockStore      *blockstore.Store
	provenanceStore *provenance.Store
	stateTrieStore  mptrie.Store
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

	p.txReorderer = txreorderer.New(
		&txreorderer.Config{
			TxQueue:            p.txQueue,
			TxBatchQueue:       p.txBatchQueue,
			MaxTxCountPerBatch: localConfig.BlockCreation.MaxTransactionCountPerBlock,
			BatchTimeout:       localConfig.BlockCreation.BlockTimeout,
			Logger:             conf.logger,
		},
	)

	var err error

	p.blockProcessor = blockprocessor.New(
		&blockprocessor.Config{
			BlockOneQueueBarrier: p.blockOneQueueBarrier,
			BlockStore:           conf.blockStore,
			ProvenanceStore:      conf.provenanceStore,
			StateTrieStore:       conf.stateTrieStore,
			DB:                   conf.db,
			Logger:               conf.logger,
		},
	)

	ledgerHeight, err := conf.blockStore.Height()
	if err != nil {
		return nil, err
	}
	if ledgerHeight == 0 {
		p.logger.Info("Bootstrapping the ledger and database")
		tx, err := PrepareBootstrapConfigTx(conf.config)
		if err != nil {
			return nil, err
		}
		bootBlock, err := blockcreator.BootstrapBlock(tx)
		if err != nil {
			return nil, err
		}
		if err = p.blockProcessor.Bootstrap(bootBlock); err != nil {
			return nil, err
		}
	}

	p.blockCreator, err = blockcreator.New(
		&blockcreator.Config{
			TxBatchQueue: p.txBatchQueue,
			Logger:       conf.logger,
			BlockStore:   conf.blockStore,
		},
	)
	if err != nil {
		return nil, err
	}

	p.peerTransport = comm.NewHTTPTransport(&comm.Config{
		LocalConf: localConfig,
		Logger:    conf.logger,
	})

	clusterConfig, _, err := conf.db.GetConfig()
	if err != nil {
		return nil, err
	}
	conf.logger.Debugf("cluster config: %+v", clusterConfig)
	if err = p.peerTransport.UpdateClusterConfig(clusterConfig); err != nil {
		return nil, err
	}

	p.blockReplicator, err = replication.NewBlockReplicator(
		&replication.Config{
			LocalConf:            localConfig,
			ClusterConfig:        clusterConfig,
			LedgerReader:         conf.blockStore,
			Transport:            p.peerTransport,
			BlockOneQueueBarrier: p.blockOneQueueBarrier,
			Logger:               conf.logger,
		},
	)
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

	p.peerTransport.Start() // Starts internal goroutine

	p.blockReplicator.Start() // Starts internal goroutine

	go p.blockProcessor.Start()
	p.blockProcessor.WaitTillStart()

	p.pendingTxs = &pendingTxs{
		txs: make(map[string]*promise),
	}
	p.blockStore = conf.blockStore

	return p, nil
}

// submitTransaction enqueue the transaction to the transaction queue
// If the timeout is set to 0, the submission would be treated as async while
// a non-zero timeout would be treated as a sync submission. When a timeout
// occurs with the sync submission, a timeout error will be returned
func (t *transactionProcessor) submitTransaction(tx interface{}, timeout time.Duration) (*types.TxReceiptResponse, error) {
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

	t.Lock()
	duplicate, err := t.isTxIDDuplicate(txID)
	if err != nil {
		t.Unlock()
		return nil, err
	}
	if duplicate {
		t.Unlock()
		return nil, &internalerror.DuplicateTxIDError{TxID: txID}
	}

	if t.txQueue.IsFull() {
		t.Unlock()
		return nil, fmt.Errorf("transaction queue is full. It means the server load is high. Try after sometime")
	}

	jsonBytes, err := json.MarshalIndent(tx, "", "\t")
	if err != nil {
		t.Unlock()
		return nil, fmt.Errorf("failed to marshal transaction: %v", err)
	}
	t.logger.Debugf("enqueuing transaction %s\n", string(jsonBytes))

	t.txQueue.Enqueue(tx)
	t.logger.Debug("transaction is enqueued for re-ordering")

	var p *promise
	if timeout > 0 {
		p = &promise{
			receipt: make(chan *types.TxReceipt),
			timeout: timeout,
		}
	}

	// TODO: add limit on the number of pending sync tx
	t.pendingTxs.add(txID, p)
	t.Unlock()

	receipt, err := p.wait()

	if err != nil {
		return nil, err
	}

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

	t.pendingTxs.removeAndSendReceipt(txIDs, block.Header)

	return nil
}

func (t *transactionProcessor) isTxIDDuplicate(txID string) (bool, error) {
	if t.pendingTxs.has(txID) {
		return true, nil
	}

	isTxIDAlreadyCommitted, err := t.blockStore.DoesTxIDExist(txID)
	if err != nil {
		return false, err
	}
	return isTxIDAlreadyCommitted, nil
}

func (t *transactionProcessor) close() error {
	t.Lock()
	defer t.Unlock()

	t.txReorderer.Stop()
	t.blockCreator.Stop()
	t.blockReplicator.Close()
	t.peerTransport.Close()
	t.blockProcessor.Stop()

	return nil
}

func (t *transactionProcessor) IsLeader() *internalerror.NotLeaderError {
	t.Lock()
	defer t.Unlock()

	return t.blockReplicator.IsLeader()
}

type pendingTxs struct {
	txs map[string]*promise
	sync.RWMutex
}

func (p *pendingTxs) add(txID string, subMethod *promise) {
	p.Lock()
	defer p.Unlock()

	p.txs[txID] = subMethod
}

func (p *pendingTxs) removeAndSendReceipt(txIDs []string, blockHeader *types.BlockHeader) {
	p.Lock()
	defer p.Unlock()

	for txIndex, txID := range txIDs {
		p.txs[txID].done(
			&types.TxReceipt{
				Header:  blockHeader,
				TxIndex: uint64(txIndex),
			},
		)

		delete(p.txs, txID)
	}
}

func (p *pendingTxs) has(txID string) bool {
	p.RLock()
	defer p.RUnlock()

	_, ok := p.txs[txID]
	return ok
}

func (p *pendingTxs) isEmpty() bool {
	p.RLock()
	defer p.RUnlock()

	return len(p.txs) == 0
}

type promise struct {
	receipt chan *types.TxReceipt
	timeout time.Duration
}

func (s *promise) wait() (*types.TxReceipt, error) {
	if s == nil {
		return nil, nil
	}

	ticker := time.NewTicker(s.timeout)
	select {
	case <-ticker.C:
		s.close()
		return nil, &internalerror.TimeoutErr{
			ErrMsg: "timeout has occurred while waiting for the transaction receipt",
		}
	case r := <-s.receipt:
		ticker.Stop()
		s.close()
		return r, nil
	}
}

func (s *promise) done(r *types.TxReceipt) {
	if s == nil {
		return
	}

	s.receipt <- r
}

func (s *promise) close() {
	if s == nil {
		return
	}

	close(s.receipt)
	s = nil
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
			},
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
