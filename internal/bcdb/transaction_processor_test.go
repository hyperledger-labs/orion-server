package bcdb

import (
	"bytes"
	"crypto/x509"
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/internal/blockstore"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/mtree"
	"github.ibm.com/blockchaindb/server/internal/provenance"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/internal/worldstate/leveldb"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type txProcessorTestEnv struct {
	serverDir      string
	db             *leveldb.LevelDB
	blockStore     *blockstore.Store
	blockStorePath string
	txProcessor    *transactionProcessor
	nodeID         string
	userID         string
	userCert       *x509.Certificate
	userSigner     crypto.Signer
	cryptoDir      string
	cleanup        func()
}

func newTxProcessorTestEnv(t *testing.T) *txProcessorTestEnv {
	dir, err := ioutil.TempDir("/tmp", "transactionProcessor")
	require.NoError(t, err)

	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	dbPath := constructWorldStatePath(dir)
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    logger,
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
			Logger:   logger,
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
			Logger:   logger,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, rmErr)
		}
		t.Fatalf("error while creating provenancestore, %v", err)
	}

	nodeID := "test-node-id1"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"testUser", nodeID, "admin"})
	userCert, userSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "testUser")
	_, nodeSigner := testutils.LoadTestClientCrypto(t, cryptoDir, nodeID)

	txProcConf := &txProcessorConfig{
		nodeID:             nodeID,
		db:                 db,
		blockStore:         blockStore,
		provenanceStore:    provenanceStore,
		txQueueLength:      100,
		txBatchQueueLength: 100,
		blockQueueLength:   100,
		maxTxCountPerBatch: 1,
		batchTimeout:       50 * time.Millisecond,
		signer:             nodeSigner,
		logger:             logger,
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

		if err := blockStore.Close(); err != nil {
			t.Errorf("error while closing blockstore, %v", err)
		}

		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("error while removing directory %s, %v", dir, err)
		}
	}

	return &txProcessorTestEnv{
		serverDir:      dir,
		db:             db,
		blockStorePath: blockStorePath,
		blockStore:     blockStore,
		txProcessor:    txProcessor,
		nodeID:         nodeID,
		userID:         "testUser",
		userCert:       userCert,
		userSigner:     userSigner,
		cryptoDir:      cryptoDir,
		cleanup:        cleanup,
	}
}

func setupTxProcessor(t *testing.T, env *txProcessorTestEnv, conf *config.Configurations, dbName string) {
	configTx, err := prepareConfigTx(conf)
	require.NoError(t, err)
	resp, err := env.txProcessor.submitTransaction(configTx, 5*time.Second)
	require.NoError(t, err)

	txHash, err := calculateTxHash(configTx, &types.ValidationInfo{Flag: types.Flag_VALID})
	require.NoError(t, err)
	txMerkelRootHash, err := crypto.ConcatenateHashes(txHash, nil)
	require.NoError(t, err)

	expectedRespPayload := &types.TxResponse{
		Header: &types.ResponseHeader{
			NodeID: env.txProcessor.nodeID,
		},
		Receipt: &types.TxReceipt{
			Header: &types.BlockHeader{
				BaseHeader: &types.BlockHeaderBase{
					Number:                1,
					LastCommittedBlockNum: 0,
				},
				TxMerkelTreeRootHash: txMerkelRootHash,
				ValidationInfo: []*types.ValidationInfo{
					{
						Flag: types.Flag_VALID,
					},
				},
			},
			TxIndex: 0,
		},
	}
	require.True(t, proto.Equal(expectedRespPayload, resp.GetPayload()))
	require.True(t, env.txProcessor.pendingTxs.isEmpty())

	user := &types.User{
		ID:          env.userID,
		Certificate: env.userCert.Raw,
		Privilege: &types.Privilege{
			DBPermission: map[string]types.Privilege_Access{
				dbName: types.Privilege_ReadWrite,
			},
		},
	}

	u, err := proto.Marshal(user)
	require.NoError(t, err)

	createUser := []*worldstate.DBUpdates{
		{
			DBName: worldstate.UsersDBName,
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
		env := newTxProcessorTestEnv(t)
		defer env.cleanup()

		conf := testConfiguration(t, env)
		defer os.RemoveAll(conf.Node.Database.LedgerDirectory)

		setupTxProcessor(t, env, conf, worldstate.DefaultDBName)

		tx := testutils.SignedDataTxEnvelope(t, env.userSigner, &types.DataTx{
			UserID:    "testUser",
			DBName:    worldstate.DefaultDBName,
			TxID:      "tx1",
			DataReads: []*types.DataRead{},
			DataWrites: []*types.DataWrite{
				{
					Key:   "test-key1",
					Value: []byte("test-value1"),
				},
			},
		})

		resp, err := env.txProcessor.submitTransaction(tx, 0)
		require.NoError(t, err)
		require.Nil(t, resp)

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
		block, err := env.blockStore.Get(2)
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedBlock, block))

		noPendingTxs := func() bool {
			return env.txProcessor.pendingTxs.isEmpty()
		}
		require.Eventually(t, noPendingTxs, time.Second*2, time.Millisecond*100)
	})

	t.Run("commit a data transaction synchronously", func(t *testing.T) {
		t.Parallel()
		env := newTxProcessorTestEnv(t)
		defer env.cleanup()

		conf := testConfiguration(t, env)
		defer os.RemoveAll(conf.Node.Database.LedgerDirectory)

		setupTxProcessor(t, env, conf, worldstate.DefaultDBName)

		tx := testutils.SignedDataTxEnvelope(t, env.userSigner, &types.DataTx{
			UserID:    "testUser",
			DBName:    worldstate.DefaultDBName,
			TxID:      "tx1",
			DataReads: []*types.DataRead{},
			DataWrites: []*types.DataWrite{
				{
					Key:   "test-key1",
					Value: []byte("test-value1"),
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

		root, err := mtree.BuildTreeForBlockTx(expectedBlock)
		require.NoError(t, err)
		expectedBlock.Header.TxMerkelTreeRootHash = root.Hash()
		block, err := env.blockStore.Get(2)
		require.NoError(t, err)
		require.True(t, proto.Equal(expectedBlock, block))

		expectedRespPayload := &types.TxResponse{
			Header: &types.ResponseHeader{
				NodeID: env.txProcessor.nodeID,
			},
			Receipt: &types.TxReceipt{
				Header:  expectedBlockHeader,
				TxIndex: 0,
			},
		}
		require.True(t, proto.Equal(expectedRespPayload, resp.GetPayload()))

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
		env := newTxProcessorTestEnv(t)
		defer env.cleanup()

		conf := testConfiguration(t, env)
		defer os.RemoveAll(conf.Node.Database.LedgerDirectory)

		setupTxProcessor(t, env, conf, worldstate.DefaultDBName)

		dataTx := testutils.SignedDataTxEnvelope(t, env.userSigner, &types.DataTx{
			UserID: "testUser",
			DBName: worldstate.DefaultDBName,
			TxID:   "tx1",
		})

		resp, err := env.txProcessor.submitTransaction(dataTx, 0)
		require.NoError(t, err)
		require.Nil(t, resp)
		noPendingTxs := func() bool {
			return env.txProcessor.pendingTxs.isEmpty()
		}
		require.Eventually(t, noPendingTxs, time.Second*2, time.Millisecond*100)

		userTx := testutils.SignedUserAdministrationTxEnvelope(t, env.userSigner, &types.UserAdministrationTx{
			UserID: "testUser",
			TxID:   "tx1",
		})
		resp, err = env.txProcessor.submitTransaction(userTx, 0)
		require.EqualError(t, err, "the transaction has a duplicate txID [tx1]")
		require.Nil(t, resp)
	})

	t.Run("duplicate txID with either pending or already committed transaction", func(t *testing.T) {
		t.Parallel()
		env := newTxProcessorTestEnv(t)
		defer env.cleanup()

		conf := testConfiguration(t, env)
		defer os.RemoveAll(conf.Node.Database.LedgerDirectory)

		setupTxProcessor(t, env, conf, worldstate.DefaultDBName)

		dbTx := testutils.SignedDBAdministrationTxEnvelope(t, env.userSigner, &types.DBAdministrationTx{
			UserID: "testUser",
			TxID:   "tx1",
		})
		configTx := testutils.SignedConfigTxEnvelope(t, env.userSigner, &types.ConfigTx{
			UserID: "testUser",
			TxID:   "tx1",
		})
		userTx := testutils.SignedUserAdministrationTxEnvelope(t, env.userSigner, &types.UserAdministrationTx{
			UserID: "testUser",
			TxID:   "tx2",
		})

		resp, err := env.txProcessor.submitTransaction(dbTx, 0)
		require.NoError(t, err)
		require.Nil(t, resp)

		resp, err = env.txProcessor.submitTransaction(configTx, 0)
		require.EqualError(t, err, "the transaction has a duplicate txID [tx1]")
		require.Nil(t, resp)

		resp, err = env.txProcessor.submitTransaction(userTx, 0)
		require.NoError(t, err)
		require.Nil(t, resp)

		noPendingTxs := func() bool {
			return env.txProcessor.pendingTxs.isEmpty()
		}
		require.Eventually(t, noPendingTxs, time.Second*2, time.Millisecond*100)
	})

	t.Run("duplicate txID with either pending or already committed transaction", func(t *testing.T) {
		t.Parallel()
		env := newTxProcessorTestEnv(t)
		defer env.cleanup()

		conf := testConfiguration(t, env)
		defer os.RemoveAll(conf.Node.Database.LedgerDirectory)

		setupTxProcessor(t, env, conf, worldstate.DefaultDBName)

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

func testConfiguration(t *testing.T, env *txProcessorTestEnv) *config.Configurations {
	return &config.Configurations{
		Node: config.NodeConf{
			Identity: config.IdentityConf{
				ID:              env.nodeID,
				CertificatePath: path.Join(env.cryptoDir, env.nodeID+".pem"),
				KeyPath:         path.Join(env.cryptoDir, env.nodeID+".key"),
			},
			Network: config.NetworkConf{
				Address: "127.0.0.1",
				Port:    0,
			},
			Database: config.DatabaseConf{
				Name:            "leveldb",
				LedgerDirectory: env.serverDir,
			},
			QueueLength: config.QueueLengthConf{
				Transaction:               1000,
				ReorderedTransactionBatch: 100,
				Block:                     100,
			},
			LogLevel: "debug",
		},
		Consensus: config.ConsensusConf{
			Algorithm:                   "raft",
			MaxBlockSize:                2,
			MaxTransactionCountPerBlock: 1,
			BlockTimeout:                50 * time.Millisecond,
		},
		Admin: config.AdminConf{
			ID:              "admin",
			CertificatePath: path.Join(env.cryptoDir, "admin.pem"),
		},
		RootCA: config.RootCAConf{
			CertificatePath: path.Join(env.cryptoDir, testutils.RootCAFileName+".pem"),
		},
	}
}

func calculateTxHash(msg proto.Message, valInfo proto.Message) ([]byte, error) {
	payloadBytes, err := json.Marshal(msg)
	if err != nil {
		return nil, errors.Wrapf(err, "can't serialize msg to json %v", msg)
	}
	valBytes, err := json.Marshal(valInfo)
	if err != nil {
		return nil, errors.Wrapf(err, "can't validationInfo msg to json %v", msg)
	}
	finalBytes := append(payloadBytes, valBytes...)
	return crypto.ComputeSHA256Hash(finalBytes)
}