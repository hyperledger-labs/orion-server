// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/mptrie/store"
	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

type configQueryTestEnv struct {
	db       *leveldb.LevelDB
	stateQP  *worldstateQueryProcessor
	ledgerQP *ledgerQueryProcessor
	cleanup  func(t *testing.T)
	blocks   []*types.BlockHeader
}

func newConfigQueryTestEnv(t *testing.T) *configQueryTestEnv {
	nodeID := "test-node-id1"

	path, err := ioutil.TempDir("/tmp", "queryProcessor")
	require.NoError(t, err)

	c := &logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: path,
			Logger:    logger,
		},
	)
	if err != nil {
		if err := os.RemoveAll(path); err != nil {
			t.Errorf("failed to remove %s due to %v", path, err)
		}

		t.Fatalf("failed to create a new leveldb instance, %v", err)
	}

	blockStorePath := constructBlockStorePath(path)
	blockStore, err := blockstore.Open(
		&blockstore.Config{
			StoreDir: blockStorePath,
			Logger:   logger,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(path); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", path, rmErr)
		}
		t.Fatalf("error while creating blockstore, %v", err)
	}

	provenanceStorePath := constructProvenanceStorePath(path)
	provenanceStore, err := provenance.Open(
		&provenance.Config{
			StoreDir: provenanceStorePath,
			Logger:   logger,
		},
	)

	trieStorePath := constructStateTrieStorePath(path)
	trieStore, err := store.Open(
		&store.Config{
			StoreDir: trieStorePath,
			Logger:   logger,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(path); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", path, rmErr)
		}
		t.Fatalf("error while creating provenancestore, %v", err)
	}

	cleanup := func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close leveldb: %v", err)
		}
		if err := blockStore.Close(); err != nil {
			t.Errorf("error while closing blockstore, %v", err)
		}
		if err := provenanceStore.Close(); err != nil {
			t.Errorf("error while closing provenancestore, %v", err)
		}
		if err := trieStore.Close(); err != nil {
			t.Errorf("error while closing triestore, %v", err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Fatalf("failed to remove %s due to %v", path, err)
		}
	}

	ledgerProcConfig := &ledgerQueryProcessorConfig{
		db:              db,
		blockStore:      blockStore,
		provenanceStore: provenanceStore,
		trieStore:       trieStore,
		identityQuerier: identity.NewQuerier(db),
		logger:          logger,
	}
	stateProcConfig := &worldstateQueryProcessorConfig{
		nodeID:          nodeID,
		db:              db,
		blockStore:      blockStore,
		identityQuerier: identity.NewQuerier(db),
		logger:          logger,
	}

	return &configQueryTestEnv{
		db:       db,
		stateQP:  newWorldstateQueryProcessor(stateProcConfig),
		ledgerQP: newLedgerQueryProcessor(ledgerProcConfig),
		cleanup:  cleanup,
	}
}

func setupConfigBlockQueryTest(t *testing.T, env *configQueryTestEnv, blocksNum int) {
	instCert, adminCert := generateCrypto(t)

	// The genesis block & config
	genesisConfig := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				Id:          "node1",
				Address:     "127.0.0.1",
				Port:        6090,
				Certificate: instCert,
			},
		},
		Admins: []*types.Admin{
			{
				Id:          "admin1",
				Certificate: adminCert,
			},
		},
		CertAuthConfig: &types.CAConfig{
			Roots: [][]byte{[]byte("bogus-root-ca")},
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
				TickInterval:         "100ms",
				ElectionTicks:        100,
				HeartbeatTicks:       10,
				MaxInflightBlocks:    50,
				SnapshotIntervalSize: 1000000,
				MaxRaftId:            1,
			},
		},
	}
	genesisBlock := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number: 1,
			},
			ValidationInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
		Payload: &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					UserId:               "adminUser",
					TxId:                 "configTx1",
					ReadOldConfigVersion: nil,
					NewConfig:            genesisConfig,
				},
			},
		},
	}
	require.NoError(t, env.ledgerQP.blockStore.Commit(genesisBlock))

	configSerialized, err := proto.Marshal(genesisConfig)
	require.NoError(t, err)

	adminUpdates, err := identity.ConstructDBEntriesForClusterAdmins(nil, genesisConfig.Admins, &types.Version{BlockNum: 1})
	require.NoError(t, err)

	createConfig := map[string]*worldstate.DBUpdates{
		worldstate.ConfigDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   worldstate.ConfigKey,
					Value: configSerialized,
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
						},
					},
				},
			},
		},
		worldstate.UsersDBName: {
			Writes: adminUpdates.Writes,
		},
	}
	require.NoError(t, env.db.Commit(createConfig, 1))
	env.blocks = []*types.BlockHeader{genesisBlock.GetHeader()}

	user := &types.User{
		Id: "testUser",
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				worldstate.DefaultDBName: types.Privilege_ReadWrite,
			},
		},
	}
	userProto, err := proto.Marshal(user)
	require.NoError(t, err)

	createUser := map[string]*worldstate.DBUpdates{
		worldstate.UsersDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   string(identity.UserNamespace) + "testUser",
					Value: userProto,
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

	require.NoError(t, env.db.Commit(createUser, 2))

	// A few data blocks
	for i := uint64(2); i < uint64(blocksNum); i++ {
		key := make([]string, 0)
		value := make([][]byte, 0)
		for j := uint64(0); j < i; j++ {
			key = append(key, fmt.Sprintf("key%d", j))
			value = append(value, []byte(fmt.Sprintf("value_%d_%d", j, i)))
		}
		block := createSampleBlock(i, key, value)
		require.NoError(t, env.ledgerQP.blockStore.Commit(block))

		env.blocks = append(env.blocks, block.GetHeader())
	}

	// another config block
	newConfig := proto.Clone(genesisConfig).(*types.ClusterConfig)
	require.NoError(t, err)
	newConfig.Nodes = append(newConfig.Nodes, &types.NodeConfig{
		Id:          "node2",
		Address:     "127.0.0.1",
		Port:        6091,
		Certificate: []byte("bogus-cert"),
	})
	newConfig.ConsensusConfig.Members = append(newConfig.ConsensusConfig.Members, &types.PeerConfig{
		NodeId:   "node2",
		RaftId:   2,
		PeerHost: "127.0.0.1",
		PeerPort: 7091,
	})
	configBlock := &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number: uint64(blocksNum),
			},
			ValidationInfo: []*types.ValidationInfo{
				{
					Flag: types.Flag_VALID,
				},
			},
		},
		Payload: &types.Block_ConfigTxEnvelope{
			ConfigTxEnvelope: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					UserId:               "adminUser",
					TxId:                 "configTx1",
					ReadOldConfigVersion: nil,
					NewConfig:            newConfig,
				},
			},
		},
	}
	require.NoError(t, env.ledgerQP.blockStore.Commit(configBlock))

	configSerialized, err = proto.Marshal(newConfig)
	require.NoError(t, err)

	createConfig = map[string]*worldstate.DBUpdates{
		worldstate.ConfigDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   worldstate.ConfigKey,
					Value: configSerialized,
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: uint64(blocksNum),
						},
					},
				},
			},
		},
	}
	require.NoError(t, env.db.Commit(createConfig, 1))
	env.blocks = append(env.blocks, configBlock.GetHeader())
}

func TestGetConfigBlock(t *testing.T) {
	env := newConfigQueryTestEnv(t)
	require.NotNil(t, env)
	setupConfigBlockQueryTest(t, env, 10)

	t.Run("getConfigBlock returns genesis config block", func(t *testing.T) {
		blockResp, err := env.stateQP.getConfigBlock("admin1", 1)
		require.NoError(t, err)
		require.NotNil(t, blockResp)

		block := &types.Block{}
		err = proto.Unmarshal(blockResp.GetBlock(), block)
		require.NoError(t, err)

		require.Equal(t, uint64(1), block.GetHeader().GetBaseHeader().GetNumber())
		require.Equal(t, 1, len(block.GetConfigTxEnvelope().GetPayload().GetNewConfig().GetNodes()))
	})

	t.Run("getConfigBlock returns last config block", func(t *testing.T) {
		blockResp, err := env.stateQP.getConfigBlock("admin1", 0)
		require.NoError(t, err)
		require.NotNil(t, blockResp)

		block := &types.Block{}
		err = proto.Unmarshal(blockResp.GetBlock(), block)
		require.NoError(t, err)

		require.Equal(t, uint64(10), block.GetHeader().GetBaseHeader().GetNumber())
		require.Equal(t, 2, len(block.GetConfigTxEnvelope().GetPayload().GetNewConfig().GetNodes()))
	})

	t.Run("getConfigBlock error: not a config block", func(t *testing.T) {
		blockResp, err := env.stateQP.getConfigBlock("admin1", 2)
		require.EqualError(t, err, "block [2] is not a config block")
		require.Nil(t, blockResp)
	})

	t.Run("getConfigBlock error: not an admin user", func(t *testing.T) {
		blockResp, err := env.stateQP.getConfigBlock("testUser", 0)
		require.EqualError(t, err, "the user [testUser] has no permission to read a config block")
		require.Nil(t, blockResp)
	})

	t.Run("getConfigBlock error: not a user", func(t *testing.T) {
		blockResp, err := env.stateQP.getConfigBlock("alice", 0)
		require.EqualError(t, err, "the user [alice] does not exist")
		require.Nil(t, blockResp)
	})
}
