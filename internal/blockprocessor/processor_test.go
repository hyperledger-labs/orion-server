// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"bytes"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/blockprocessor/mocks"
	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/mptrie"
	mptrieStore "github.com/hyperledger-labs/orion-server/internal/mptrie/store"
	"github.com/hyperledger-labs/orion-server/internal/mtree"
	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/internal/queue"
	"github.com/hyperledger-labs/orion-server/internal/txvalidation"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/state"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type testEnv struct {
	blockProcessor      *BlockProcessor
	stopBlockProcessing chan struct{}
	db                  worldstate.DB
	dbPath              string
	blockStore          *blockstore.Store
	blockStorePath      string
	userID              string
	userCert            *x509.Certificate
	userSigner          crypto.Signer
	genesisConfig       *types.ClusterConfig
	genesisBlock        *types.Block
	cleanup             func(bool)
}

func newTestEnv(t *testing.T) *testEnv {
	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("/tmp", "validatorAndCommitter")
	require.NoError(t, err)

	dbPath := filepath.Join(dir, "leveldb")
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    logger,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, err)
		}
		t.Fatalf("error while creating the leveldb instance, %v", err)
	}

	blockStorePath := filepath.Join(dir, "blockstore")
	blockStore, err := blockstore.Open(
		&blockstore.Config{
			StoreDir: blockStorePath,
			Logger:   logger,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, err)
		}
		t.Fatalf("error while creating the block store, %v", err)
	}

	provenanceStorePath := filepath.Join(dir, "provenancestore")
	provenanceStore, err := provenance.Open(
		&provenance.Config{
			StoreDir: provenanceStorePath,
			Logger:   logger,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, err)
		}
		t.Fatalf("error while creating the block store, %v", err)
	}

	mptrieStorePath := filepath.Join(dir, "statetriestore")
	mptrieStore, err := mptrieStore.Open(
		&mptrieStore.Config{
			StoreDir: mptrieStorePath,
			Logger:   logger,
		},
	)

	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, err)
		}
		t.Fatalf("error while creating the block store, %v", err)
	}

	txValidator := txvalidation.NewValidator(
		&txvalidation.Config{
			DB:     db,
			Logger: logger,
		},
	)

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"testUser", "node1", "admin1"})
	userCert, userSigner := testutils.LoadTestCrypto(t, cryptoDir, "testUser")
	nodeCert, _ := testutils.LoadTestCrypto(t, cryptoDir, "node1")
	adminCert, _ := testutils.LoadTestCrypto(t, cryptoDir, "admin1")
	caCert, _ := testutils.LoadTestCA(t, cryptoDir, testutils.RootCAFileName)

	b := New(&Config{
		BlockOneQueueBarrier: queue.NewOneQueueBarrier(logger),
		BlockStore:           blockStore,
		StateTrieStore:       mptrieStore,
		ProvenanceStore:      provenanceStore,
		DB:                   db,
		TxValidator:          txValidator,
		Logger:               logger,
	})

	genesisConfig := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				Id:          "node1",
				Address:     "127.0.0.1",
				Port:        6090,
				Certificate: nodeCert.Raw,
			},
		},
		Admins: []*types.Admin{
			{
				Id:          "admin1",
				Certificate: adminCert.Raw,
			},
		},
		CertAuthConfig: &types.CAConfig{
			Roots: [][]byte{caCert.Raw},
		},
		ConsensusConfig: &types.ConsensusConfig{
			Algorithm: "raft",
			Members: []*types.PeerConfig{
				{
					NodeId:   "node1",
					RaftId:   1,
					PeerHost: "127.0.0.1",
					PeerPort: 7090,
				},
			},
			RaftConfig: &types.RaftConfig{
				TickInterval:   "100ms",
				ElectionTicks:  100,
				HeartbeatTicks: 10,
			},
		},
	}
	genesisBlock := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number: 1,
			},
		},
		Payload: &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					TxId:      "configTx1",
					NewConfig: genesisConfig,
				},
			},
		},
	}

	cleanup := func(stopBlockProcessor bool) {
		if stopBlockProcessor {
			b.Stop()
		}

		if err := provenanceStore.Close(); err != nil {
			t.Errorf("failed to close the provenance store")
		}

		if err := db.Close(); err != nil {
			t.Errorf("failed to close the db instance, %v", err)
		}

		if err := blockStore.Close(); err != nil {
			t.Errorf("failed to close the blockstore, %v", err)
		}

		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("failed to remove directory %s, %v", dir, err)
		}
	}

	env := &testEnv{
		blockProcessor: b,
		db:             db,
		dbPath:         dir,
		blockStore:     blockStore,
		blockStorePath: blockStorePath,
		userID:         "testUser",
		userCert:       userCert,
		userSigner:     userSigner,
		genesisConfig:  genesisConfig,
		genesisBlock:   genesisBlock,
		cleanup:        cleanup,
	}

	go env.blockProcessor.Start()
	env.blockProcessor.WaitTillStart()

	return env
}

func setup(t *testing.T, env *testEnv) {
	reply, err := env.blockProcessor.blockOneQueueBarrier.EnqueueWait(env.genesisBlock)
	require.NoError(t, err)
	require.NotNil(t, reply)
	require.Equal(t, env.genesisConfig, reply)

	identityQuerier := identity.NewQuerier(env.db)
	assertConfigHasCommitted := func() bool {
		exist, err := identityQuerier.DoesUserExist("admin1")
		if err != nil || !exist {
			return false
		}
		return true
	}
	require.Eventually(t, assertConfigHasCommitted, 2*time.Second, 100*time.Millisecond)

	user := &types.User{
		Id:          env.userID,
		Certificate: env.userCert.Raw,
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				worldstate.DefaultDBName: types.Privilege_ReadWrite,
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
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
			},
		},
	}

	require.NoError(t, env.db.Commit(createUser, 1))

	assertUserCreation := func() bool {
		exist, err := identityQuerier.DoesUserExist("testUser")
		if err != nil || !exist {
			return false
		}
		return true
	}
	require.Eventually(t, assertUserCreation, 2*time.Second, 100*time.Millisecond)
}

func TestValidatorAndCommitter(t *testing.T) {
	t.Run("enqueue-one-block", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup(true)

		setup(t, env)

		tx := createSampleTx(t, "dataTx1", []string{"key1", "key1"}, [][]byte{[]byte("value-1"), []byte("value-2")}, env.userSigner)

		testCases := []struct {
			block               *types.Block
			dbName              string
			key                 string
			expectedValue       []byte
			expectedMetadata    *types.Metadata
			expectedBlockHeight uint64
		}{
			{
				block:         createSampleBlock(2, tx[:1]),
				key:           "key1",
				expectedValue: []byte("value-1"),
				expectedMetadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 2,
						TxNum:    0,
					},
				},
				expectedBlockHeight: 2,
			},
			{
				block:         createSampleBlock(3, tx[1:]),
				key:           "key1",
				expectedValue: []byte("value-2"),
				expectedMetadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 3,
						TxNum:    0,
					},
				},
				expectedBlockHeight: 3,
			},
		}

		for _, tt := range testCases {
			reply, err := env.blockProcessor.blockOneQueueBarrier.EnqueueWait(tt.block)
			require.NoError(t, err)
			require.Nil(t, reply) // May not be nil when we implement dynamic config

			assertCommittedValue := func() bool {
				val, metadata, err := env.db.Get(worldstate.DefaultDBName, tt.key)
				if err != nil ||
					!bytes.Equal(tt.expectedValue, val) ||
					!proto.Equal(tt.expectedMetadata, metadata) {
					return false
				}
				return true
			}
			require.Eventually(t, assertCommittedValue, 2*time.Second, 100*time.Millisecond)

			height, err := env.blockStore.Height()
			require.NoError(t, err)
			require.Equal(t, tt.expectedBlockHeight, height)

			block, err := env.blockStore.Get(tt.block.GetHeader().GetBaseHeader().GetNumber())
			require.NoError(t, err)
			require.True(t, proto.Equal(tt.block, block))
		}
	})

	t.Run("enqueue-more-than-one-block", func(t *testing.T) {
		t.Parallel()

		env := newTestEnv(t)
		defer env.cleanup(true)

		setup(t, env)

		genesisHash, err := env.blockStore.GetHash(uint64(1))
		require.NoError(t, err)

		tx := createSampleTx(t, "dataTx1", []string{"key1", "key1"}, [][]byte{[]byte("value-1"), []byte("value-2")}, env.userSigner)
		testCases := []struct {
			blocks              []*types.Block
			key                 string
			expectedValue       []byte
			expectedMetadata    *types.Metadata
			expectedBlockHeight uint64
			expectedBlocks      []*types.Block
		}{
			{
				blocks: []*types.Block{
					createSampleBlock(2, tx[:1]),
					createSampleBlock(3, tx[1:]),
				},
				key:           "key1",
				expectedValue: []byte("value-2"),
				expectedMetadata: &types.Metadata{
					Version: &types.Version{
						BlockNum: 3,
						TxNum:    0,
					},
				},
				expectedBlockHeight: 3,
				expectedBlocks: []*types.Block{
					createSampleBlock(2, tx[:1]),
					createSampleBlock(3, tx[1:]),
				},
			},
		}

		for _, tt := range testCases {
			stateTrieRootOrg, err := env.blockProcessor.committer.stateTrie.Hash()
			require.NoError(t, err)
			for _, block := range tt.expectedBlocks {
				// Because we update SkipchainHashes, TxMerkelTreeRootHash and StateMerkelTreeRootHash during process, we want to precalculate them
				// for the expected blocks
				block.Header.SkipchainHashes = calculateBlockHashes(t, genesisHash, tt.expectedBlocks, block.Header.BaseHeader.Number)
				root, err := mtree.BuildTreeForBlockTx(block)
				require.NoError(t, err)
				block.Header.TxMerkelTreeRootHash = root.Hash()

				dbsUpdates, err := ConstructDBUpdatesForBlock(block, env.blockProcessor)
				require.NoError(t, err)
				require.NoError(t, env.blockProcessor.committer.applyBlockOnStateTrie(dbsUpdates))
				block.Header.StateMerkelTreeRootHash, err = env.blockProcessor.committer.stateTrie.Hash()
				require.NoError(t, err)
			}
			env.blockProcessor.committer.stateTrie, err = mptrie.NewTrie(stateTrieRootOrg, env.blockProcessor.committer.stateTrieStore)
		}

		for _, tt := range testCases {
			for _, block := range tt.blocks {
				reply, err := env.blockProcessor.blockOneQueueBarrier.EnqueueWait(block)
				require.NoError(t, err)
				require.Nil(t, reply) // May not be nil when we implement dynamic config
			}

			assertCommittedValue := func() bool {
				stateTrieKey, err := state.ConstructCompositeKey(worldstate.DefaultDBName, "key1")
				if err != nil {
					return false
				}
				proof, err := env.blockProcessor.committer.stateTrie.GetProof(stateTrieKey, false)
				if err != nil || proof == nil {
					return false
				}
				kvHash, err := state.CalculateKeyValueHash(stateTrieKey, tt.expectedValue)
				if err != nil {
					return false
				}
				valid, err := proof.Verify(kvHash, tt.expectedBlocks[tt.expectedBlockHeight-2].Header.StateMerkelTreeRootHash, false)
				if err != nil || !valid {
					return false
				}
				val, metadata, err := env.db.Get(worldstate.DefaultDBName, "key1")
				if err != nil ||
					!bytes.Equal(tt.expectedValue, val) ||
					!proto.Equal(tt.expectedMetadata, metadata) {
					return false
				}
				return true
			}
			require.Eventually(t, assertCommittedValue, 2*time.Second, 100*time.Millisecond)

			height, err := env.blockStore.Height()
			require.NoError(t, err)
			require.Equal(t, tt.expectedBlockHeight, height)

			for index, expectedBlock := range tt.blocks {
				precalculatedSkipListBlock := tt.expectedBlocks[index]
				block, err := env.blockStore.Get(expectedBlock.GetHeader().GetBaseHeader().GetNumber())
				expectedBlockJSON, _ := json.Marshal(expectedBlock)
				blockJSON, _ := json.Marshal(block)
				precalculatedSkipListBlockJSON, _ := json.Marshal(precalculatedSkipListBlock)
				require.NoError(t, err)
				require.True(t, proto.Equal(expectedBlock, block), "Expected - before save\t %s\nBlock\t\t\t %s\n", expectedBlockJSON, blockJSON)
				require.True(t, proto.Equal(precalculatedSkipListBlock, block), "Expected - fully precalculated\t %s\nBlock\t\t\t %s\n", precalculatedSkipListBlockJSON, blockJSON)
				require.Equal(t, precalculatedSkipListBlock.Header.SkipchainHashes, block.Header.SkipchainHashes, "Expected\t %s\nBlock\t\t %s\n", precalculatedSkipListBlockJSON, blockJSON)
				root, err := mtree.BuildTreeForBlockTx(expectedBlock)
				require.NoError(t, err)
				require.Equal(t, root.Hash(), block.Header.TxMerkelTreeRootHash)
				require.Equal(t, expectedBlock.Header.StateMerkelTreeRootHash, block.Header.StateMerkelTreeRootHash)
			}
		}
	})
}

func TestFailureAndRecovery(t *testing.T) {
	t.Run("blockstore is ahead of stateDB by 1 block -- will recover successfully", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup(false)

		setup(t, env)

		block2 := createSampleBlock(2, createSampleTx(t, "dataTx1", []string{"key1"}, [][]byte{[]byte("value-1")}, env.userSigner))
		block2.Header.ValidationInfo = []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}
		require.NoError(t, env.blockProcessor.committer.blockStore.Commit(block2))

		blockStoreHeight, err := env.blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(2), blockStoreHeight)

		stateDBHeight, err := env.db.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(1), stateDBHeight)

		// before committing the block to the stateDB, mimic node crash
		// by stopping the block processor goroutine
		env.blockProcessor.Stop()

		// mimic node restart by starting the block processor goroutine
		env.blockProcessor.started = make(chan struct{})
		env.blockProcessor.stop = make(chan struct{})
		env.blockProcessor.stopped = make(chan struct{})
		env.blockProcessor.blockOneQueueBarrier = queue.NewOneQueueBarrier(env.blockProcessor.logger)
		defer env.blockProcessor.Stop()
		go env.blockProcessor.Start()
		env.blockProcessor.WaitTillStart()

		assertStateDBHeight := func() bool {
			stateDBHeight, err = env.db.Height()
			if err != nil || stateDBHeight != uint64(2) {
				return false
			}

			return true
		}
		require.Eventually(t, assertStateDBHeight, 2*time.Second, 100*time.Millisecond)
	})

	t.Run("blockstore is behind stateDB by 1 block -- will result in panic", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup(false)

		setup(t, env)

		block2 := createSampleBlock(2, createSampleTx(t, "dataTx1", []string{"key1"}, [][]byte{[]byte("value-1")}, env.userSigner))
		block2.Header.ValidationInfo = []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}

		dbsUpdates, provenanceData, err := env.blockProcessor.committer.constructDBAndProvenanceEntries(block2)
		require.NoError(t, err)
		require.NoError(t, env.blockProcessor.committer.commitToProvenanceStore(2, provenanceData))
		require.NoError(t, env.blockProcessor.committer.commitToStateDB(2, dbsUpdates))

		blockStoreHeight, err := env.blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(1), blockStoreHeight)

		stateDBHeight, err := env.db.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(2), stateDBHeight)

		env.blockProcessor.Stop()

		env.blockProcessor.stop = make(chan struct{})
		env.blockProcessor.stopped = make(chan struct{})
		assertPanic := func() {
			env.blockProcessor.Start()
		}
		require.PanicsWithError(t, "error while recovering node: the height of state database [2] is higher than the height of block store [1]. The node cannot be recovered", assertPanic)
	})

	t.Run("blockstore is ahead of stateDB by 2 blocks -- will result in panic", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup(false)

		setup(t, env)

		tx := createSampleTx(t, "dataTx1", []string{"key1", "key1"}, [][]byte{[]byte("value-1"), []byte("value-2")}, env.userSigner)
		block2 := createSampleBlock(2, tx[:1])
		block2.Header.ValidationInfo = []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}
		require.NoError(t, env.blockProcessor.committer.commitToBlockStore(block2))

		block3 := createSampleBlock(3, tx[1:])
		block3.Header.ValidationInfo = []*types.ValidationInfo{
			{
				Flag: types.Flag_VALID,
			},
		}
		require.NoError(t, env.blockProcessor.committer.commitToBlockStore(block3))

		blockStoreHeight, err := env.blockStore.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(3), blockStoreHeight)

		stateDBHeight, err := env.db.Height()
		require.NoError(t, err)
		require.Equal(t, uint64(1), stateDBHeight)

		env.blockProcessor.Stop()

		env.blockProcessor.stop = make(chan struct{})
		env.blockProcessor.stopped = make(chan struct{})

		env.stopBlockProcessing = make(chan struct{})
		assertPanic := func() {
			env.blockProcessor.Start()
		}
		require.PanicsWithError(t, "error while recovering node: the difference between the height of the block store [3] and the state database [1] cannot be greater than 1 block. The node cannot be recovered", assertPanic)
	})
}

func TestBlockCommitListener(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup(true)

	setup(t, env)
	block2 := createSampleBlock(2, createSampleTx(t, "dataTx1", []string{"key1"}, [][]byte{[]byte("value-1")}, env.userSigner))
	block2.Header.ValidationInfo = []*types.ValidationInfo{
		{
			Flag: types.Flag_VALID,
		},
	}
	expectedBlock := proto.Clone(block2).(*types.Block)
	genesisHash, err := env.blockStore.GetHash(uint64(1))
	expectedBlock.Header.SkipchainHashes = calculateBlockHashes(t, genesisHash, []*types.Block{block2}, 2)
	root, err := mtree.BuildTreeForBlockTx(block2)
	require.NoError(t, err)
	expectedBlock.Header.TxMerkelTreeRootHash = root.Hash()
	stateTrieRootOrg, err := env.blockProcessor.committer.stateTrie.Hash()

	dbsUpdates, err := ConstructDBUpdatesForBlock(block2, env.blockProcessor)
	require.NoError(t, err)
	require.NoError(t, env.blockProcessor.committer.applyBlockOnStateTrie(dbsUpdates))
	expectedBlock.Header.StateMerkelTreeRootHash, err = env.blockProcessor.committer.stateTrie.Hash()
	require.NoError(t, err)
	env.blockProcessor.committer.stateTrie, err = mptrie.NewTrie(stateTrieRootOrg, env.blockProcessor.committer.stateTrieStore)

	listener1 := &mocks.BlockCommitListener{}
	listener1.On("PostBlockCommitProcessing", mock.Anything).Return(func(arg mock.Arguments) {
		receivedBlock := arg.Get(0).(*types.Block)
		require.True(t, proto.Equal(expectedBlock, receivedBlock))
	}).Return(nil)
	listener2 := &mocks.BlockCommitListener{}
	listener2.On("PostBlockCommitProcessing", mock.Anything).Return(func(arg mock.Arguments) {
		receivedBlock := arg.Get(0).(*types.Block)
		require.True(t, proto.Equal(expectedBlock, receivedBlock))
	}).Return(nil)

	env.blockProcessor.RegisterBlockCommitListener("listener1", listener1)
	env.blockProcessor.RegisterBlockCommitListener("listener2", listener2)

	reply, err := env.blockProcessor.blockOneQueueBarrier.EnqueueWait(block2)
	require.NoError(t, err)
	require.Nil(t, reply) // May not be nil when we implement dynamic config

	assertCommittedBlock := func() bool {
		height, err := env.blockStore.Height()
		if err != nil {
			return false
		}
		if height != 2 {
			return false
		}

		block, err := env.blockStore.Get(2)
		if err != nil {
			return false
		}
		return proto.Equal(expectedBlock, block)
	}

	require.Eventually(t, assertCommittedBlock, 2*time.Second, 100*time.Millisecond)
}

func createSampleBlock(blockNumber uint64, env []*types.DataTxEnvelope) *types.Block {
	return &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number: blockNumber,
			},
			ValidationInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
		Payload: &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{
				Envelopes: env,
			},
		},
	}
}

func createSampleTx(t *testing.T, txId string, key []string, value [][]byte, signer crypto.Signer) []*types.DataTxEnvelope {
	envelopes := make([]*types.DataTxEnvelope, 0)
	for i := 0; i < len(key); i++ {
		e := testutils.SignedDataTxEnvelope(t, []crypto.Signer{signer}, &types.DataTx{
			MustSignUserIds: []string{"testUser"},
			TxId:            fmt.Sprintf("%s_%d", txId, i),
			DbOperations: []*types.DBOperation{
				{
					DbName: worldstate.DefaultDBName,
					DataWrites: []*types.DataWrite{
						{
							Key:   key[i],
							Value: value[i],
						},
					},
				},
			},
		})
		envelopes = append(envelopes, e)
	}
	return envelopes
}

func calculateBlockHashes(t *testing.T, genesisHash []byte, blocks []*types.Block, blockNum uint64) [][]byte {
	res := make([][]byte, 0)
	distance := uint64(1)
	blockNum--

	for (blockNum%distance) == 0 && distance <= blockNum {
		index := blockNum - distance
		if index == 0 {
			res = append(res, genesisHash)
		} else {
			headerBytes, err := proto.Marshal(blocks[index-1].Header)
			require.NoError(t, err)
			blockHash, err := crypto.ComputeSHA256Hash(headerBytes)
			require.NoError(t, err)

			res = append(res, blockHash)
		}
		distance *= blockstore.SkipListBase
	}
	return res
}
