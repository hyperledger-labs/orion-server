// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"bytes"
	"crypto/x509"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/IBM-Blockchain/bcdb-server/config"
	"github.com/IBM-Blockchain/bcdb-server/internal/blockprocessor"
	"github.com/IBM-Blockchain/bcdb-server/internal/blockstore"
	"github.com/IBM-Blockchain/bcdb-server/internal/identity"
	"github.com/IBM-Blockchain/bcdb-server/internal/mptrie"
	mptrieStore "github.com/IBM-Blockchain/bcdb-server/internal/mptrie/store"
	"github.com/IBM-Blockchain/bcdb-server/internal/mtree"
	"github.com/IBM-Blockchain/bcdb-server/internal/provenance"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate/leveldb"
	"github.com/IBM-Blockchain/bcdb-server/pkg/crypto"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server/testutils"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

type txProcessorTestEnv struct {
	dbPath         string
	db             *leveldb.LevelDB
	blockStore     *blockstore.Store
	stateTrieStore mptrie.Store
	blockStorePath string
	txProcessor    *transactionProcessor
	userID         string
	userCert       *x509.Certificate
	userSigner     crypto.Signer
	cleanup        func()
}

func newTxProcessorTestEnv(t *testing.T, cryptoDir string, conf *config.Configurations) *txProcessorTestEnv {
	dir := conf.LocalConfig.Server.Database.LedgerDirectory

	c := &logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	lg, err := logger.New(c)
	require.NoError(t, err)

	dbPath := constructWorldStatePath(dir)
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    lg,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, rmErr)
		}
		t.Fatalf("error while creating leveldb, %v", err)
	}

	blockStorePath := constructBlockStorePath(dir)
	blockStore, err := blockstore.Open(
		&blockstore.Config{
			StoreDir: blockStorePath,
			Logger:   lg,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, rmErr)
		}
		t.Fatalf("error while creating blockstore, %v", err)
	}

	provenanceStorePath := constructProvenanceStorePath(dir)
	provenanceStore, err := provenance.Open(
		&provenance.Config{
			StoreDir: provenanceStorePath,
			Logger:   lg,
		},
	)

	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, rmErr)
		}
		t.Fatalf("error while creating provenancestore, %v", err)
	}

	stateTrieStore, err := mptrieStore.Open(
		&mptrieStore.Config{
			StoreDir: constructStateTrieStorePath(dir),
			Logger:   lg,
		},
	)

	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, rmErr)
		}
		t.Fatalf("error while creating state trie store, %v", err)
	}

	userCert, userSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "testUser")

	txProcConf := &txProcessorConfig{
		config:          conf,
		db:              db,
		blockStore:      blockStore,
		provenanceStore: provenanceStore,
		stateTrieStore:  stateTrieStore,
		logger:          lg,
	}
	txProcessor, err := newTransactionProcessor(txProcConf)
	require.NoError(t, err)

	cleanup := func() {
		if err := txProcessor.close(); err != nil {
			t.Errorf("error while closing the transaction processor")
		}

		if err := provenanceStore.Close(); err != nil {
			t.Errorf("error while closing the provenance store")
		}

		if err := db.Close(); err != nil {
			t.Errorf("error while closing the db instance, %v", err)
		}

		if err := stateTrieStore.Close(); err != nil {
			t.Errorf("error while closing the state trie store, %v", err)
		}

		if err := blockStore.Close(); err != nil {
			t.Errorf("error while closing blockstore, %v", err)
		}

		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("error while removing directory %s, %v", dir, err)
		}
	}

	require.Eventually(t, func() bool { return txProcessor.IsLeader() == nil }, 30*time.Second, 100*time.Millisecond)

	return &txProcessorTestEnv{
		dbPath:         dbPath,
		db:             db,
		blockStore:     blockStore,
		stateTrieStore: stateTrieStore,
		blockStorePath: blockStorePath,
		txProcessor:    txProcessor,
		userID:         "testUser",
		userCert:       userCert,
		userSigner:     userSigner,
		cleanup:        cleanup,
	}
}

func setupTxProcessor(t *testing.T, env *txProcessorTestEnv, dbName string) {
	h, err := env.txProcessor.blockStore.Height()
	require.NoError(t, err)
	require.Equal(t, uint64(1), h)

	user := &types.User{
		Id:          env.userID,
		Certificate: env.userCert.Raw,
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				dbName: types.Privilege_ReadWrite,
			},
		},
	}

	u, err := proto.Marshal(user)
	require.NoError(t, err)

	createUser := map[string]*worldstate.DBUpdates{
		worldstate.UsersDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   string(identity.UserNamespace) + env.userID,
					Value: u,
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    1,
						},
					},
				},
			},
		},
	}
	require.NoError(t, env.db.Commit(createUser, 2))
	height, err := env.blockStore.Height()
	require.NoError(t, err)
	require.Equal(t, uint64(1), height)
}

func TestTransactionProcessor(t *testing.T) {
	t.Parallel()

	t.Run("commit a data transaction asynchronously", func(t *testing.T) {
		t.Parallel()
		cryptoDir, conf := testConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
		defer os.RemoveAll(conf.LocalConfig.Server.Database.LedgerDirectory)
		env := newTxProcessorTestEnv(t, cryptoDir, conf)
		defer env.cleanup()

		setupTxProcessor(t, env, worldstate.DefaultDBName)

		tx := testutils.SignedDataTxEnvelope(t, []crypto.Signer{env.userSigner}, &types.DataTx{
			MustSignUserIds: []string{"testUser"},
			TxId:            "tx1",
			DbOperations: []*types.DBOperation{
				{
					DbName:    worldstate.DefaultDBName,
					DataReads: []*types.DataRead{},
					DataWrites: []*types.DataWrite{
						{
							Key:   "test-key1",
							Value: []byte("test-value1"),
						},
					},
				},
			},
		})

		resp, err := env.txProcessor.submitTransaction(tx, 0)
		require.NoError(t, err)
		require.Nil(t, resp.GetReceipt())

		assertTestKey1InDB := func() bool {
			val, metadata, err := env.db.Get(worldstate.DefaultDBName, "test-key1")
			if err != nil {
				return false
			}
			return bytes.Equal([]byte("test-value1"), val) &&
				proto.Equal(
					&types.Metadata{
						Version: &types.Version{
							BlockNum: 2,
							TxNum:    0,
						},
					},
					metadata,
				)
		}
		require.Eventually(
			t,
			assertTestKey1InDB,
			2*time.Second,
			100*time.Millisecond,
		)

		height, err := env.blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(2), height)

		genesisHash, err := env.blockStore.GetHash(1)
		require.NoError(t, err)
		require.NotNil(t, genesisHash)
		genesisHashBase, err := env.blockStore.GetBaseHeaderHash(1)
		require.NoError(t, err)
		require.NotNil(t, genesisHashBase)
		genesisHeader, err := env.blockStore.GetHeader(1)
		require.NoError(t, err)
		require.NotNil(t, genesisHeader)

		expectedBlock := &types.Block{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number:                 2,
					PreviousBaseHeaderHash: genesisHashBase,
					LastCommittedBlockHash: genesisHash,
					LastCommittedBlockNum:  1,
				},
				SkipchainHashes: [][]byte{genesisHash},
				ValidationInfo: []*types.ValidationInfo{
					{
						Flag: types.Flag_VALID,
					},
				},
			},
			Payload: &types.Block_DataTxEnvelopes{
				DataTxEnvelopes: &types.DataTxEnvelopes{
					Envelopes: []*types.DataTxEnvelope{
						tx,
					},
				},
			},
		}

		root, err := mtree.BuildTreeForBlockTx(expectedBlock)
		require.NoError(t, err)
		expectedBlock.Header.TxMerkelTreeRootHash = root.Hash()

		stateTrie, err := mptrie.NewTrie(genesisHeader.StateMerkelTreeRootHash, env.stateTrieStore)
		require.NoError(t, err)
		expectedBlock.Header.StateMerkelTreeRootHash = applyTxsOnTrie(t, env, expectedBlock.Payload.(*types.Block_DataTxEnvelopes).DataTxEnvelopes, stateTrie)

		block, err := env.blockStore.Get(2)
		require.NoError(t, err)
		require.True(t, block.GetConsensusMetadata().GetRaftTerm() > 0)
		require.True(t, block.GetConsensusMetadata().GetRaftIndex() > 0)
		block.ConsensusMetadata = nil
		require.True(t, proto.Equal(expectedBlock, block), "expected: %+v, actual: %+v", expectedBlock, block)

		noPendingTxs := func() bool {
			return env.txProcessor.pendingTxs.isEmpty()
		}
		require.Eventually(t, noPendingTxs, time.Second*2, time.Millisecond*100)
	})

	t.Run("commit a data transaction synchronously", func(t *testing.T) {
		t.Parallel()
		cryptoDir, conf := testConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
		defer os.RemoveAll(conf.LocalConfig.Server.Database.LedgerDirectory)
		env := newTxProcessorTestEnv(t, cryptoDir, conf)
		defer env.cleanup()

		setupTxProcessor(t, env, worldstate.DefaultDBName)

		tx := testutils.SignedDataTxEnvelope(t, []crypto.Signer{env.userSigner}, &types.DataTx{
			MustSignUserIds: []string{"testUser"},
			TxId:            "tx1",
			DbOperations: []*types.DBOperation{
				{
					DbName:    worldstate.DefaultDBName,
					DataReads: []*types.DataRead{},
					DataWrites: []*types.DataWrite{
						{
							Key:   "test-key1",
							Value: []byte("test-value1"),
						},
					},
				},
			},
		})

		resp, err := env.txProcessor.submitTransaction(tx, 5*time.Second)
		require.NoError(t, err)
		require.True(t, env.txProcessor.pendingTxs.isEmpty())

		height, err := env.blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(2), height)

		genesisHash, err := env.blockStore.GetHash(1)
		require.NoError(t, err)
		require.NotNil(t, genesisHash)
		genesisHashBase, err := env.blockStore.GetBaseHeaderHash(1)
		require.NoError(t, err)
		require.NotNil(t, genesisHashBase)

		genesisHeader, err := env.blockStore.GetHeader(1)
		require.NoError(t, err)
		require.NotNil(t, genesisHeader)

		expectedBlockHeader := &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number:                 2,
				PreviousBaseHeaderHash: genesisHashBase,
				LastCommittedBlockHash: genesisHash,
				LastCommittedBlockNum:  1,
			},
			SkipchainHashes: [][]byte{genesisHash},
			ValidationInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		}

		expectedBlock := &types.Block{
			Header: expectedBlockHeader,
			Payload: &types.Block_DataTxEnvelopes{
				DataTxEnvelopes: &types.DataTxEnvelopes{
					Envelopes: []*types.DataTxEnvelope{
						tx,
					},
				},
			},
		}

		stateTrie, err := mptrie.NewTrie(genesisHeader.StateMerkelTreeRootHash, env.stateTrieStore)
		require.NoError(t, err)
		expectedBlock.Header.StateMerkelTreeRootHash = applyTxsOnTrie(t, env, expectedBlock.Payload.(*types.Block_DataTxEnvelopes).DataTxEnvelopes, stateTrie)

		root, err := mtree.BuildTreeForBlockTx(expectedBlock)
		require.NoError(t, err)
		expectedBlock.Header.TxMerkelTreeRootHash = root.Hash()
		block, err := env.blockStore.Get(2)
		require.NoError(t, err)
		require.True(t, block.GetConsensusMetadata().GetRaftTerm() > 0)
		require.True(t, block.GetConsensusMetadata().GetRaftIndex() > 0)
		block.ConsensusMetadata = nil
		require.True(t, proto.Equal(expectedBlock, block))

		expectedRespPayload := &types.TxReceiptResponse{
			Receipt: &types.TxReceipt{
				Header:  expectedBlockHeader,
				TxIndex: 0,
			},
		}
		require.True(t, proto.Equal(expectedRespPayload, resp))

		val, metadata, err := env.db.Get(worldstate.DefaultDBName, "test-key1")
		require.NoError(t, err)
		require.Equal(t, []byte("test-value1"), val)
		require.True(t, proto.Equal(
			&types.Metadata{
				Version: &types.Version{
					BlockNum: 2,
					TxNum:    0,
				},
			},
			metadata,
		))
	})

	t.Run("duplicate txID with the already committed transaction", func(t *testing.T) {
		t.Parallel()
		cryptoDir, conf := testConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
		defer os.RemoveAll(conf.LocalConfig.Server.Database.LedgerDirectory)
		env := newTxProcessorTestEnv(t, cryptoDir, conf)
		defer env.cleanup()

		setupTxProcessor(t, env, worldstate.DefaultDBName)

		dataTx := testutils.SignedDataTxEnvelope(t, []crypto.Signer{env.userSigner}, &types.DataTx{
			MustSignUserIds: []string{"testUser"},
			TxId:            "tx1",
			DbOperations: []*types.DBOperation{
				{
					DbName: worldstate.DefaultDBName,
				},
			},
		})

		resp, err := env.txProcessor.submitTransaction(dataTx, 0)
		require.NoError(t, err)
		require.Nil(t, resp.GetReceipt())
		noPendingTxs := func() bool {
			return env.txProcessor.pendingTxs.isEmpty()
		}
		require.Eventually(t, noPendingTxs, time.Second*2, time.Millisecond*100)

		userTx := testutils.SignedUserAdministrationTxEnvelope(t, env.userSigner, &types.UserAdministrationTx{
			UserId: "testUser",
			TxId:   "tx1",
		})
		resp, err = env.txProcessor.submitTransaction(userTx, 0)
		require.EqualError(t, err, "the transaction has a duplicate txID [tx1]")
		require.Nil(t, resp)
	})

	t.Run("duplicate txID with either pending or already committed transaction", func(t *testing.T) {
		t.Parallel()
		cryptoDir, conf := testConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
		defer os.RemoveAll(conf.LocalConfig.Server.Database.LedgerDirectory)
		env := newTxProcessorTestEnv(t, cryptoDir, conf)
		defer env.cleanup()

		setupTxProcessor(t, env, worldstate.DefaultDBName)

		dbTx := testutils.SignedDBAdministrationTxEnvelope(t, env.userSigner, &types.DBAdministrationTx{
			UserId: "testUser",
			TxId:   "tx1",
		})
		configTx := testutils.SignedConfigTxEnvelope(t, env.userSigner, &types.ConfigTx{
			UserId: "testUser",
			TxId:   "tx1",
		})
		userTx := testutils.SignedUserAdministrationTxEnvelope(t, env.userSigner, &types.UserAdministrationTx{
			UserId: "testUser",
			TxId:   "tx2",
		})

		resp, err := env.txProcessor.submitTransaction(dbTx, 0)
		require.NoError(t, err)
		require.Nil(t, resp.GetReceipt())

		resp, err = env.txProcessor.submitTransaction(configTx, 0)
		require.EqualError(t, err, "the transaction has a duplicate txID [tx1]")
		require.Nil(t, resp)

		resp, err = env.txProcessor.submitTransaction(userTx, 0)
		require.NoError(t, err)
		require.Nil(t, resp.GetReceipt())

		noPendingTxs := func() bool {
			return env.txProcessor.pendingTxs.isEmpty()
		}
		require.Eventually(t, noPendingTxs, time.Second*2, time.Millisecond*100)
	})

	t.Run("duplicate txID with either pending or already committed transaction", func(t *testing.T) {
		t.Parallel()
		cryptoDir, conf := testConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
		defer os.RemoveAll(conf.LocalConfig.Server.Database.LedgerDirectory)
		env := newTxProcessorTestEnv(t, cryptoDir, conf)
		defer env.cleanup()

		setupTxProcessor(t, env, worldstate.DefaultDBName)

		resp, err := env.txProcessor.submitTransaction([]byte("hello"), 0)
		require.EqualError(t, err, "unexpected transaction type")
		require.Nil(t, resp)
	})
}

func TestPendingTxs(t *testing.T) {
	t.Run("async tx", func(t *testing.T) {
		pendingTxs := &pendingTxs{
			txs: make(map[string]*promise),
		}

		var p *promise
		require.True(t, pendingTxs.isEmpty())
		pendingTxs.add("tx1", p)
		require.True(t, pendingTxs.has("tx1"))
		require.False(t, pendingTxs.has("tx2"))
		pendingTxs.add("tx2", p)
		require.True(t, pendingTxs.has("tx2"))
		pendingTxs.removeAndSendReceipt([]string{"tx1", "tx2"}, nil)
		require.True(t, pendingTxs.isEmpty())
	})

	t.Run("sync tx", func(t *testing.T) {
		pendingTxs := &pendingTxs{
			txs: make(map[string]*promise),
		}

		p := &promise{
			receipt: make(chan *types.TxReceipt),
			timeout: 5 * time.Second,
		}
		pendingTxs.add("tx3", p)

		blockHeader := &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number:                5,
				LastCommittedBlockNum: 1,
			},
		}
		expectedReceipt := &types.TxReceipt{
			Header:  blockHeader,
			TxIndex: 0,
		}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			actualReceipt, err := p.wait()
			require.NoError(t, err)
			require.True(t, proto.Equal(expectedReceipt, actualReceipt))
		}()

		pendingTxs.removeAndSendReceipt([]string{"tx3"}, blockHeader)
		wg.Wait()
	})

	t.Run("sync tx with timeout", func(t *testing.T) {
		pendingTxs := &pendingTxs{
			txs: make(map[string]*promise),
		}

		p := &promise{
			receipt: make(chan *types.TxReceipt),
			timeout: 1 * time.Millisecond,
		}
		pendingTxs.add("tx3", p)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			receipt, err := p.wait()
			require.EqualError(t, err, "timeout has occurred while waiting for the transaction receipt")
			require.Nil(t, receipt)
		}()

		wg.Wait()
		require.False(t, pendingTxs.isEmpty())
	})
}

func testConfiguration(t *testing.T) (string, *config.Configurations) {
	ledgerDir, err := ioutil.TempDir("/tmp", "server")
	require.NoError(t, err)

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"testUser", "bdb-node-1", "admin"})

	return cryptoDir, &config.Configurations{
		LocalConfig: &config.LocalConfiguration{
			Server: config.ServerConf{
				Identity: config.IdentityConf{
					ID:              "bdb-node-1",
					CertificatePath: path.Join(cryptoDir, "bdb-node-1.pem"),
					KeyPath:         path.Join(cryptoDir, "bdb-node-1.key"),
				},
				Network: config.NetworkConf{
					Address: "127.0.0.1",
					Port:    0,
				},
				Database: config.DatabaseConf{
					Name:            "leveldb",
					LedgerDirectory: ledgerDir,
				},
				QueueLength: config.QueueLengthConf{
					Transaction:               1000,
					ReorderedTransactionBatch: 100,
					Block:                     100,
				},
				LogLevel: "debug",
			},
			BlockCreation: config.BlockCreationConf{
				MaxBlockSize:                2,
				MaxTransactionCountPerBlock: 1,
				BlockTimeout:                50 * time.Millisecond,
			},
			Replication: config.ReplicationConf{
				WALDir:  path.Join(ledgerDir, "raft", "wal"),
				SnapDir: path.Join(ledgerDir, "raft", "snap"),
			},
		},
		SharedConfig: &config.SharedConfiguration{
			Nodes: []config.NodeConf{
				{
					NodeID:          "bdb-node-1",
					Host:            "127.0.0.1",
					Port:            33000,
					CertificatePath: path.Join(cryptoDir, "bdb-node-1.pem"),
				},
			},
			Consensus: &config.ConsensusConf{
				Algorithm: "raft",
				Members: []*config.PeerConf{
					{
						NodeId:   "bdb-node-1",
						RaftId:   1,
						PeerHost: "127.0.0.1",
						PeerPort: 34000,
					},
				},
				RaftConfig: &config.RaftConf{
					TickInterval:         "100ms",
					ElectionTicks:        100,
					HeartbeatTicks:       10,
					MaxInflightBlocks:    10,
					SnapshotIntervalSize: math.MaxUint64,
				},
			},
			CAConfig: config.CAConfiguration{
				RootCACertsPath: []string{path.Join(cryptoDir, testutils.RootCAFileName+".pem")},
			},
			Admin: config.AdminConf{
				ID:              "admin",
				CertificatePath: path.Join(cryptoDir, "admin.pem"),
			},
		},
	}
}

func applyTxsOnTrie(t *testing.T, env *txProcessorTestEnv, payload interface{}, stateTrie *mptrie.MPTrie) []byte {
	tempBlock := &types.Block{
		Header: &types.BlockHeader{
			ValidationInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
	}

	switch payload.(type) {
	case *types.DataTxEnvelopes:
		tempBlock.Payload = &types.Block_DataTxEnvelopes{payload.(*types.DataTxEnvelopes)}
		tempBlock.Header.ValidationInfo = make([]*types.ValidationInfo, len(payload.(*types.DataTxEnvelopes).Envelopes))
		for i, _ := range payload.(*types.DataTxEnvelopes).Envelopes {
			tempBlock.Header.ValidationInfo[i] = &types.ValidationInfo{
				Flag: types.Flag_VALID,
			}
		}
	case *types.ConfigTxEnvelope:
		tempBlock.Payload = &types.Block_ConfigTxEnvelope{payload.(*types.ConfigTxEnvelope)}
		tempBlock.Header.ValidationInfo = []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}
	case *types.DBAdministrationTxEnvelope:
		tempBlock.Payload = &types.Block_DbAdministrationTxEnvelope{payload.(*types.DBAdministrationTxEnvelope)}
		tempBlock.Header.ValidationInfo = []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}
	case *types.UserAdministrationTxEnvelope:
		tempBlock.Payload = &types.Block_UserAdministrationTxEnvelope{payload.(*types.UserAdministrationTxEnvelope)}
		tempBlock.Header.ValidationInfo = []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}
	}

	dbUpdates, err := blockprocessor.ConstructDBUpdatesForBlock(tempBlock, env.txProcessor.blockProcessor)
	require.NoError(t, err)

	for dbName, update := range dbUpdates {
		for _, dbwrite := range update.Writes {
			key, err := mptrie.ConstructCompositeKey(dbName, dbwrite.Key)
			require.NoError(t, err)
			// TODO: should we add Metadata to value
			value := dbwrite.Value
			err = stateTrie.Update(key, value)
			require.NoError(t, err)
		}
		for _, dbdelete := range update.Deletes {
			key, err := mptrie.ConstructCompositeKey(dbName, dbdelete)
			require.NoError(t, err)
			_, err = stateTrie.Delete(key)
			require.NoError(t, err)
		}
	}

	stateTrieRoot, err := stateTrie.Hash()
	require.NoError(t, err)
	require.NoError(t, env.stateTrieStore.RollbackChanges())
	return stateTrieRoot
}
