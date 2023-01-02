// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"bytes"
	"crypto/x509"
	"math"
	"os"
	"path"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/internal/blockprocessor"
	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	internalerror "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/mptrie"
	mptrieStore "github.com/hyperledger-labs/orion-server/internal/mptrie/store"
	"github.com/hyperledger-labs/orion-server/internal/mtree"
	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/state"
	"github.com/hyperledger-labs/orion-server/pkg/types"
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

	userCert, userSigner := testutils.LoadTestCrypto(t, cryptoDir, "testUser")

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
		if err := txProcessor.Close(); err != nil {
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

	if conf.JoinBlock == nil {
		require.Eventually(t, func() bool { return txProcessor.IsLeader() == nil }, 30*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool {
			leader, active := txProcessor.ClusterStatus()
			return len(leader) > 0 && len(active) > 0
		}, 30*time.Second, 100*time.Millisecond)
	}

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
	t.Run("commit a data transaction asynchronously", func(t *testing.T) {
		cryptoDir, conf := testConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
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

		resp, err := env.txProcessor.SubmitTransaction(tx, 0)
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
		expectedBlock.Header.TxMerkleTreeRootHash = root.Hash()

		stateTrie, err := mptrie.NewTrie(genesisHeader.StateMerkleTreeRootHash, env.stateTrieStore)
		require.NoError(t, err)
		expectedBlock.Header.StateMerkleTreeRootHash = applyTxsOnTrie(t, env, expectedBlock.Payload.(*types.Block_DataTxEnvelopes).DataTxEnvelopes, stateTrie)

		block, err := env.blockStore.Get(2)
		require.NoError(t, err)
		require.True(t, block.GetConsensusMetadata().GetRaftTerm() > 0)
		require.True(t, block.GetConsensusMetadata().GetRaftIndex() > 0)
		block.ConsensusMetadata = nil
		require.True(t, proto.Equal(expectedBlock, block), "expected: %+v, actual: %+v", expectedBlock, block)

		noPendingTxs := func() bool {
			return env.txProcessor.pendingTxs.Empty()
		}
		require.Eventually(t, noPendingTxs, time.Second*2, time.Millisecond*100)
	})

	t.Run("commit a data transaction synchronously", func(t *testing.T) {
		cryptoDir, conf := testConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
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

		resp, err := env.txProcessor.SubmitTransaction(tx, 5*time.Second)
		require.NoError(t, err)
		require.True(t, env.txProcessor.pendingTxs.Empty())

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

		stateTrie, err := mptrie.NewTrie(genesisHeader.StateMerkleTreeRootHash, env.stateTrieStore)
		require.NoError(t, err)
		expectedBlock.Header.StateMerkleTreeRootHash = applyTxsOnTrie(t, env, expectedBlock.Payload.(*types.Block_DataTxEnvelopes).DataTxEnvelopes, stateTrie)

		root, err := mtree.BuildTreeForBlockTx(expectedBlock)
		require.NoError(t, err)
		expectedBlock.Header.TxMerkleTreeRootHash = root.Hash()
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
		cryptoDir, conf := testConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
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

		resp, err := env.txProcessor.SubmitTransaction(dataTx, 0)
		require.NoError(t, err)
		require.Nil(t, resp.GetReceipt())
		noPendingTxs := func() bool {
			return env.txProcessor.pendingTxs.Empty()
		}
		require.Eventually(t, noPendingTxs, time.Second*2, time.Millisecond*100)

		userTx := testutils.SignedUserAdministrationTxEnvelope(t, env.userSigner, &types.UserAdministrationTx{
			UserId: "testUser",
			TxId:   "tx1",
		})
		resp, err = env.txProcessor.SubmitTransaction(userTx, 0)
		require.EqualError(t, err, "the transaction has a duplicate txID [tx1]")
		require.Nil(t, resp)
	})

	t.Run("duplicate txID with either pending or already committed transaction", func(t *testing.T) {
		cryptoDir, conf := testConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
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

		resp, err := env.txProcessor.SubmitTransaction(dbTx, 0)
		require.NoError(t, err)
		require.Nil(t, resp.GetReceipt())

		resp, err = env.txProcessor.SubmitTransaction(configTx, 0)
		require.EqualError(t, err, "the transaction has a duplicate txID [tx1]")
		require.Nil(t, resp)

		resp, err = env.txProcessor.SubmitTransaction(userTx, 0)
		require.NoError(t, err)
		require.Nil(t, resp.GetReceipt())

		noPendingTxs := func() bool {
			return env.txProcessor.pendingTxs.Empty()
		}
		require.Eventually(t, noPendingTxs, time.Second*2, time.Millisecond*100)
	})

	t.Run("unexpected transaction type", func(t *testing.T) {
		cryptoDir, conf := testConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
		env := newTxProcessorTestEnv(t, cryptoDir, conf)
		defer env.cleanup()

		setupTxProcessor(t, env, worldstate.DefaultDBName)

		resp, err := env.txProcessor.SubmitTransaction([]byte("hello"), 0)
		require.EqualError(t, err, "unexpected transaction type")
		require.Nil(t, resp)
	})

	t.Run("bad TxId", func(t *testing.T) {
		cryptoDir, conf := testConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
		env := newTxProcessorTestEnv(t, cryptoDir, conf)
		defer env.cleanup()

		setupTxProcessor(t, env, worldstate.DefaultDBName)

		tx := testutils.SignedDataTxEnvelope(t, []crypto.Signer{env.userSigner}, &types.DataTx{
			MustSignUserIds: []string{"testUser"},
			TxId:            "txid/is/not/a/url-segment",
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

		resp, err := env.txProcessor.SubmitTransaction(tx, 5*time.Second)
		require.EqualError(t, err, "bad TxId: un-safe for a URL segment: \"txid/is/not/a/url-segment\"")
		require.IsType(t, &internalerror.BadRequestError{}, err)
		require.Nil(t, resp)
	})

	t.Run("create with a join block", func(t *testing.T) {
		cryptoDir, conf := testJoinConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
		env := newTxProcessorTestEnv(t, cryptoDir, conf)
		require.NotNil(t, env)
	})

	t.Run("get cluster status", func(t *testing.T) {
		cryptoDir, conf := testConfiguration(t)
		require.NotEqual(t, "", cryptoDir)
		env := newTxProcessorTestEnv(t, cryptoDir, conf)
		defer env.cleanup()

		setupTxProcessor(t, env, worldstate.DefaultDBName)

		require.Eventually(t, func() bool {
			return env.txProcessor.IsLeader() == nil
		}, 30*time.Second, 100*time.Millisecond)

		require.Eventually(t, func() bool {
			leader, active := env.txProcessor.ClusterStatus()
			return (leader == "bdb-node-1") && (active[0] == leader)
		}, 30*time.Second, 100*time.Millisecond)
	})
}

func testConfiguration(t *testing.T) (string, *config.Configurations) {
	ledgerDir := t.TempDir()

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"testUser", "bdb-node-1", "admin"})

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
				LogLevel: "info",
			},
			BlockCreation: config.BlockCreationConf{
				MaxBlockSize:                2,
				MaxTransactionCountPerBlock: 1,
				BlockTimeout:                50 * time.Millisecond,
			},
			Replication: config.ReplicationConf{
				WALDir:  path.Join(ledgerDir, "raft", "wal"),
				SnapDir: path.Join(ledgerDir, "raft", "snap"),
				Network: config.NetworkConf{
					Address: "127.0.0.1",
					Port:    0,
				},
			},
		},
		SharedConfig: &config.SharedConfiguration{
			Nodes: []*config.NodeConf{
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

func testJoinConfiguration(t *testing.T) (string, *config.Configurations) {
	ledgerDir := t.TempDir()

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"testUser", "bdb-node-1", "bdb-node-2", "admin"})

	clusterConfig := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				Id:          "bdb-node-1",
				Address:     "127.0.0.1",
				Port:        6091,
				Certificate: []byte("bogus-cert"),
			},
			{
				Id:          "bdb-node-2",
				Address:     "127.0.0.1",
				Port:        6092,
				Certificate: []byte("bogus-cert"),
			},
		},
		Admins: []*types.Admin{
			{Id: "admin", Certificate: []byte("something")},
		},
		CertAuthConfig: &types.CAConfig{
			Roots: [][]byte{[]byte("bogus-ca-cert")},
		},
		ConsensusConfig: &types.ConsensusConfig{
			Algorithm: "raft",
			Members: []*types.PeerConfig{
				{
					NodeId:   "bdb-node-1",
					RaftId:   1,
					PeerHost: "127.0.0.1",
					PeerPort: 70901,
				},
				{
					NodeId:   "bdb-node-2",
					RaftId:   2,
					PeerHost: "127.0.0.1",
					PeerPort: 70902,
				},
			},
			RaftConfig: &types.RaftConfig{
				TickInterval:         "20ms",
				ElectionTicks:        100,
				HeartbeatTicks:       10,
				MaxInflightBlocks:    50,
				SnapshotIntervalSize: 1000000,
				MaxRaftId:            2,
			},
		},
	}

	joinBlock := &types.Block{
		Header: &types.BlockHeader{BaseHeader: &types.BlockHeaderBase{Number: 10}},
		Payload: &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					UserId:    "admin",
					TxId:      "txid",
					NewConfig: clusterConfig,
				},
			},
		},
	}

	return cryptoDir, &config.Configurations{
		LocalConfig: &config.LocalConfiguration{
			Server: config.ServerConf{
				Identity: config.IdentityConf{
					ID:              "bdb-node-2",
					CertificatePath: path.Join(cryptoDir, "bdb-node-2.pem"),
					KeyPath:         path.Join(cryptoDir, "bdb-node-2.key"),
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
				LogLevel: "info",
			},
			BlockCreation: config.BlockCreationConf{
				MaxBlockSize:                2,
				MaxTransactionCountPerBlock: 1,
				BlockTimeout:                50 * time.Millisecond,
			},
			Replication: config.ReplicationConf{
				WALDir:  path.Join(ledgerDir, "raft", "wal"),
				SnapDir: path.Join(ledgerDir, "raft", "snap"),
				Network: config.NetworkConf{
					Address: "127.0.0.1",
					Port:    0,
				},
			},
		},
		SharedConfig: nil,
		JoinBlock:    joinBlock,
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
			key, err := state.ConstructCompositeKey(dbName, dbwrite.Key)
			require.NoError(t, err)
			// TODO: should we add Metadata to value
			value := dbwrite.Value
			err = stateTrie.Update(key, value)
			require.NoError(t, err)
		}
		for _, dbdelete := range update.Deletes {
			key, err := state.ConstructCompositeKey(dbName, dbdelete)
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
