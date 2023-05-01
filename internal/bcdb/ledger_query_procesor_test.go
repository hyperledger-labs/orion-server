// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/blockprocessor"
	"github.com/hyperledger-labs/orion-server/internal/blockstore"
	interrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/mptrie"
	"github.com/hyperledger-labs/orion-server/internal/mptrie/store"
	"github.com/hyperledger-labs/orion-server/internal/mtree"
	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/marshal"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/state"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

type ledgerProcessorTestEnv struct {
	db          *leveldb.LevelDB
	p           *ledgerQueryProcessor
	cleanup     func(t *testing.T)
	blocks      []*types.BlockHeader
	blockTx     []*types.DataTxEnvelopes
	instCert    *x509.Certificate
	adminCert   *x509.Certificate
	userCert    *x509.Certificate
	aliceCert   *x509.Certificate
	aliceSigner crypto.Signer
}

func newLedgerProcessorTestEnv(t *testing.T) *ledgerProcessorTestEnv {
	path := t.TempDir()

	c := &logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	dbPath := constructWorldStatePath(path)
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    logger,
		},
	)
	if err != nil {
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
		t.Fatalf("error while creating provenancestore, %v", err)
	}

	cryptoPath := testutils.GenerateTestCrypto(t, []string{"Instance", "Admin", "testUser", "alice"})
	instCert, _ := testutils.LoadTestCrypto(t, cryptoPath, "Instance")
	adminCert, _ := testutils.LoadTestCrypto(t, cryptoPath, "Admin")
	userCert, _ := testutils.LoadTestCrypto(t, cryptoPath, "testUser")
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoPath, "alice")
	_ = os.RemoveAll(cryptoPath)

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
	}

	conf := &ledgerQueryProcessorConfig{
		db:              db,
		blockStore:      blockStore,
		trieStore:       trieStore,
		identityQuerier: identity.NewQuerier(db),
		logger:          logger,
	}

	return &ledgerProcessorTestEnv{
		db:          db,
		p:           newLedgerQueryProcessor(conf),
		cleanup:     cleanup,
		instCert:    instCert,
		adminCert:   adminCert,
		userCert:    userCert,
		aliceCert:   aliceCert,
		aliceSigner: aliceSigner,
	}
}

func setup(t *testing.T, env *ledgerProcessorTestEnv, blocksNum int) {
	configBlock := &types.Block{
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
					UserId:               "admin1",
					TxId:                 "configTx1",
					ReadOldConfigVersion: nil,
					NewConfig: &types.ClusterConfig{
						Nodes: []*types.NodeConfig{
							{
								Id:          "node1",
								Address:     "127.0.0.1",
								Certificate: env.instCert.Raw,
							},
						},
						Admins: []*types.Admin{
							{
								Id:          "admin1",
								Certificate: env.adminCert.Raw,
							},
						},
					},
				},
			},
		},
	}
	require.NoError(t, env.p.blockStore.AddSkipListLinks(configBlock))
	root, err := mtree.BuildTreeForBlockTx(configBlock)
	require.NoError(t, err)
	configBlock.Header.TxMerkleTreeRootHash = root.Hash()
	_, err = env.p.blockStore.Commit(configBlock)
	require.NoError(t, err)
	env.blocks = []*types.BlockHeader{configBlock.GetHeader()}
	env.blockTx = []*types.DataTxEnvelopes{{}}

	adminUser := &types.User{
		Id: "admin1",
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				worldstate.DefaultDBName: types.Privilege_ReadWrite,
			},
			Admin: true,
		},
		Certificate: env.adminCert.Raw,
	}
	adminUserBytes, err := proto.Marshal(adminUser)
	require.NoError(t, err)

	user := &types.User{
		Id: "testUser",
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				worldstate.DefaultDBName: types.Privilege_ReadWrite,
			},
		},
		Certificate: env.userCert.Raw,
	}

	userBytes, err := proto.Marshal(user)
	require.NoError(t, err)

	aliceUser := &types.User{
		Id: "alice",
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				worldstate.DefaultDBName: types.Privilege_ReadWrite,
			},
		},
		Certificate: env.aliceCert.Raw,
	}

	aliceBytes, err := proto.Marshal(aliceUser)
	require.NoError(t, err)

	createUser := map[string]*worldstate.DBUpdates{
		worldstate.UsersDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   string(identity.UserNamespace) + "testUser",
					Value: userBytes,
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
				{
					Key:   string(identity.UserNamespace) + "admin1",
					Value: adminUserBytes,
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    2,
						},
					},
				},
				{
					Key:   string(identity.UserNamespace) + "alice",
					Value: aliceBytes,
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

	trie, err := mptrie.NewTrie(nil, env.p.trieStore)
	require.NoError(t, err)

	for i := uint64(2); i < uint64(blocksNum); i++ {
		key := make([]string, 0)
		value := make([][]byte, 0)
		for j := uint64(0); j < i; j++ {
			key = append(key, fmt.Sprintf("key%d", j))
			value = append(value, []byte(fmt.Sprintf("value_%d_%d", j, i)))
		}
		block := createSampleBlock(env.aliceSigner, i, key, value)
		require.NoError(t, env.p.blockStore.AddSkipListLinks(block))
		root, err := mtree.BuildTreeForBlockTx(block)
		require.NoError(t, err)
		block.Header.TxMerkleTreeRootHash = root.Hash()
		dataUpdates := createDataUpdatesFromBlock(block)
		blockprocessor.ApplyBlockOnStateTrie(trie, dataUpdates)
		block.Header.StateMerkleTreeRootHash, err = trie.Hash()
		require.NoError(t, err)
		_, err = env.p.blockStore.Commit(block)
		require.NoError(t, err)

		err = trie.Commit(block.GetHeader().GetBaseHeader().GetNumber())
		require.NoError(t, err)

		env.blocks = append(env.blocks, block.GetHeader())
		env.blockTx = append(env.blockTx, block.GetDataTxEnvelopes())
	}
}

func createSampleBlock(aliceSigner crypto.Signer, blockNumber uint64, key []string, value [][]byte) *types.Block {
	envelopes := make([]*types.DataTxEnvelope, 0)
	for i := 0; i < len(key); i++ {
		e := &types.DataTxEnvelope{
			Payload: &types.DataTx{
				MustSignUserIds: []string{"testUser"},
				TxId:            fmt.Sprintf("Tx%d%s", blockNumber, key[i]),
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
			},
			Signatures: make(map[string][]byte),
		}

		e.Signatures["testUser"] = []byte("in must sign so always correct")
		e.Signatures["testUser2"] = []byte("bad sig")
		if aliceSigner != nil {
			eBytes, _ := marshal.DefaultMarshaller().Marshal(e.Payload)
			sig, _ := aliceSigner.Sign(eBytes)
			e.Signatures["alice"] = sig
		}

		envelopes = append(envelopes, e)
	}

	valInfo := make([]*types.ValidationInfo, 0)
	for i := 0; i < len(key); i++ {
		valInfo = append(valInfo, &types.ValidationInfo{
			Flag: types.Flag_VALID,
		})
	}
	return &types.Block{
		Header: &types.BlockHeader{
			BaseHeader: &types.BlockHeaderBase{
				Number: blockNumber,
			},
			ValidationInfo: valInfo,
		},
		Payload: &types.Block_DataTxEnvelopes{
			DataTxEnvelopes: &types.DataTxEnvelopes{
				Envelopes: envelopes,
			},
		},
	}
}

func createProvenanceDataFromBlock(block *types.Block) []*provenance.TxDataForProvenance {
	var provenanceData []*provenance.TxDataForProvenance
	dirtyWriteKeyVersion := make(map[string]*types.Version)
	txsEnvelopes := block.GetDataTxEnvelopes().Envelopes

	for txNum, tx := range txsEnvelopes {
		version := &types.Version{
			BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
			TxNum:    uint64(txNum),
		}

		pData := constructProvenanceEntriesForDataTx(tx.GetPayload(), version, dirtyWriteKeyVersion)
		provenanceData = append(provenanceData, pData...)
	}

	return provenanceData
}

func constructProvenanceEntriesForDataTx(tx *types.DataTx, version *types.Version, dirtyWriteKeyVersion map[string]*types.Version) []*provenance.TxDataForProvenance {
	txpData := make([]*provenance.TxDataForProvenance, len(tx.DbOperations))

	for i, ops := range tx.DbOperations {
		txpData[i] = &provenance.TxDataForProvenance{
			DBName:             ops.DbName,
			UserID:             tx.MustSignUserIds[0],
			TxID:               tx.TxId,
			OldVersionOfWrites: make(map[string]*types.Version),
		}

		for _, read := range ops.DataReads {
			k := &provenance.KeyWithVersion{
				Key:     read.Key,
				Version: read.Version,
			}
			txpData[i].Reads = append(txpData[i].Reads, k)
		}

		for _, write := range ops.DataWrites {
			kv := &types.KVWithMetadata{
				Key:   write.Key,
				Value: write.Value,
				Metadata: &types.Metadata{
					Version:       version,
					AccessControl: write.Acl,
				},
			}
			txpData[i].Writes = append(txpData[i].Writes, kv)

			oldVersion, ok := dirtyWriteKeyVersion[write.Key]
			if !ok {
				continue
			}

			txpData[i].OldVersionOfWrites[write.Key] = oldVersion
		}

		for _, w := range ops.DataWrites {
			dirtyWriteKeyVersion[w.Key] = version
		}
	}

	return txpData
}

func createDataUpdatesFromBlock(block *types.Block) map[string]*worldstate.DBUpdates {
	dataUpdate := make(map[string]*worldstate.DBUpdates)
	txsEnvelopes := block.GetDataTxEnvelopes().Envelopes

	for txNum, tx := range txsEnvelopes {
		version := &types.Version{
			BlockNum: block.GetHeader().GetBaseHeader().GetNumber(),
			TxNum:    uint64(txNum),
		}

		blockprocessor.AddDBEntriesForDataTx(tx.GetPayload(), version, dataUpdate)
	}

	return dataUpdate
}

func TestGetBlock(t *testing.T) {
	env := newLedgerProcessorTestEnv(t)
	defer env.cleanup(t)
	setup(t, env, 20)

	testCases := []struct {
		name                string
		blockNumber         uint64
		expectedBlockHeader *types.BlockHeader
		expectedBlockTx     []*types.DataTxEnvelope
		user                string
		expectedErr         error
	}{
		{
			name:                "Getting block 5 - correct",
			blockNumber:         5,
			expectedBlockHeader: env.blocks[4],
			expectedBlockTx:     env.blockTx[4].Envelopes,
			user:                "testUser",
		},
		{
			name:                "Getting block 17 - correct",
			blockNumber:         17,
			expectedBlockHeader: env.blocks[16],
			expectedBlockTx:     env.blockTx[16].Envelopes,
			user:                "testUser",
		},
		{
			name:                "Getting block 12 - correct",
			blockNumber:         12,
			expectedBlockHeader: env.blocks[11],
			expectedBlockTx:     env.blockTx[11].Envelopes,
			user:                "testUser",
		},
		{
			name:                "Getting block 9 - correct",
			blockNumber:         9,
			expectedBlockHeader: env.blocks[8],
			expectedBlockTx:     env.blockTx[8].Envelopes,
			user:                "testUser",
		},
		{
			name:                "Getting block 21 - not exist",
			blockNumber:         21,
			expectedBlockHeader: nil,
			user:                "testUser",
			expectedErr:         &interrors.NotFoundErr{Message: "block not found: 21"},
		},
		{
			name:                "Getting block 515 - not exist",
			blockNumber:         515,
			expectedBlockHeader: nil,
			user:                "testUser",
			expectedErr:         &interrors.NotFoundErr{Message: "block not found: 515"},
		},
		{
			name:                "Getting block 10 - wrong user",
			blockNumber:         10,
			expectedBlockHeader: nil,
			user:                "userNotExist",
			expectedErr:         &interrors.PermissionErr{ErrMsg: "user userNotExist has no permission to access the ledger"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			payload, err := env.p.getBlockHeader(testCase.user, testCase.blockNumber)
			if testCase.expectedErr == nil {
				require.NoError(t, err)
				if testCase.expectedBlockHeader != nil {
					require.True(t, proto.Equal(testCase.expectedBlockHeader, payload.GetBlockHeader()))
				} else {
					require.Nil(t, payload.GetBlockHeader())
				}
			} else {
				require.Error(t, err)
				require.EqualError(t, err, testCase.expectedErr.Error())
				require.IsType(t, testCase.expectedErr, err)
			}
			augmentedPayload, err := env.p.getAugmentedBlockHeader(testCase.user, testCase.blockNumber)
			if testCase.expectedErr == nil {
				require.NoError(t, err)
				if testCase.expectedBlockHeader != nil {
					require.True(t, proto.Equal(testCase.expectedBlockHeader, augmentedPayload.GetBlockHeader().GetHeader()))
					require.Equal(t, len(testCase.expectedBlockTx), len(augmentedPayload.GetBlockHeader().GetTxIds()))
					for _, tx := range testCase.expectedBlockTx {
						require.Contains(t, augmentedPayload.GetBlockHeader().GetTxIds(), tx.GetPayload().GetTxId())

					}
				} else {
					require.Nil(t, augmentedPayload.GetBlockHeader())
				}
			} else {
				require.Error(t, err)
				require.EqualError(t, err, testCase.expectedErr.Error())
				require.IsType(t, testCase.expectedErr, err)
			}
		})
	}
}

func TestGetPath(t *testing.T) {
	env := newLedgerProcessorTestEnv(t)
	defer env.cleanup(t)
	setup(t, env, 100)

	testCases := []struct {
		name           string
		startNumber    uint64
		endNumber      uint64
		expectedBlocks []*types.BlockHeader
		user           string
		expectedErr    error
	}{
		{
			name:           "path 2 1",
			startNumber:    1,
			endNumber:      2,
			expectedBlocks: []*types.BlockHeader{env.blocks[1], env.blocks[0]},
			user:           "testUser",
		},
		{
			name:           "path 4 1",
			startNumber:    1,
			endNumber:      4,
			expectedBlocks: []*types.BlockHeader{env.blocks[3], env.blocks[2], env.blocks[0]},
			user:           "testUser",
		},
		{
			name:           "path 17 1",
			startNumber:    1,
			endNumber:      17,
			expectedBlocks: []*types.BlockHeader{env.blocks[16], env.blocks[0]},
			user:           "testUser",
		},
		{
			name:           "path 17 2",
			startNumber:    2,
			endNumber:      17,
			expectedBlocks: []*types.BlockHeader{env.blocks[16], env.blocks[8], env.blocks[4], env.blocks[2], env.blocks[1]},
			user:           "testUser",
		},
		{
			name:           "path 90 6",
			startNumber:    6,
			endNumber:      90,
			expectedBlocks: []*types.BlockHeader{env.blocks[89], env.blocks[88], env.blocks[80], env.blocks[64], env.blocks[32], env.blocks[16], env.blocks[8], env.blocks[6], env.blocks[5]},
			user:           "testUser",
		},
		{
			name:           "path 6 6",
			startNumber:    6,
			endNumber:      6,
			expectedBlocks: []*types.BlockHeader{env.blocks[5]},
			user:           "testUser",
		},
		{
			name:           "path 1 1",
			startNumber:    1,
			endNumber:      1,
			expectedBlocks: []*types.BlockHeader{env.blocks[0]},
			user:           "testUser",
		},
		{
			name:           "error: path 17 2 wrong user",
			startNumber:    2,
			endNumber:      17,
			expectedBlocks: nil,
			user:           "userNotExist",
			expectedErr:    &interrors.PermissionErr{ErrMsg: "user userNotExist has no permission to access the ledger"},
		},
		{
			name:           "error: path 2 17 wrong direction",
			startNumber:    17,
			endNumber:      2,
			expectedBlocks: nil,
			user:           "testUser",
			expectedErr:    &interrors.BadRequestError{ErrMsg: "can't find path from start block 17 to end block 2, start must be <= end"},
		},
		{
			name:           "error: path 2 117 end block not in ledger",
			startNumber:    2,
			endNumber:      117,
			expectedBlocks: nil,
			user:           "testUser",
			expectedErr:    &interrors.NotFoundErr{Message: "can't find path in blocks skip list between 117 2: block not found: 117"},
		},
		{
			name:           "error: path 115 117 start block not in ledger",
			startNumber:    2,
			endNumber:      117,
			expectedBlocks: nil,
			user:           "testUser",
			expectedErr:    &interrors.NotFoundErr{Message: "can't find path in blocks skip list between 117 2: block not found: 117"},
		},
		{
			name:           "error: path 0 6 start block out of range",
			startNumber:    0,
			endNumber:      6,
			expectedBlocks: nil,
			user:           "testUser",
			expectedErr:    &interrors.BadRequestError{ErrMsg: "start block number must be >=1"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			payload, err := env.p.getPath(testCase.user, testCase.startNumber, testCase.endNumber)
			if testCase.expectedErr != nil {
				require.Error(t, err)
				require.Nil(t, payload)
				require.EqualError(t, err, testCase.expectedErr.Error())
				require.IsType(t, testCase.expectedErr, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, payload)
				require.Equal(t, len(testCase.expectedBlocks), len(payload.GetBlockHeaders()))
				for idx, expectedBlock := range testCase.expectedBlocks {
					require.True(t, proto.Equal(expectedBlock, payload.GetBlockHeaders()[idx]))
				}
			}
		})
	}
}

func TestGetTxProof(t *testing.T) {
	env := newLedgerProcessorTestEnv(t)
	defer env.cleanup(t)
	setup(t, env, 100)

	testCases := []struct {
		name         string
		blockNumber  uint64
		txIndex      uint64
		expectedRoot []byte
		expectedTx   *types.DataTxEnvelope
		user         string
		expectedErr  error
	}{
		{
			name:         "Getting block 5, tx 2 - correct",
			blockNumber:  5,
			txIndex:      2,
			expectedRoot: env.blocks[4].TxMerkleTreeRootHash,
			expectedTx:   env.blockTx[4].Envelopes[2],
			user:         "testUser",
		},
		{
			name:         "Getting block 17, tx 5 - correct",
			blockNumber:  17,
			txIndex:      5,
			expectedRoot: env.blocks[16].TxMerkleTreeRootHash,
			expectedTx:   env.blockTx[16].Envelopes[5],
			user:         "testUser",
		},
		{
			name:         "Getting block 45, tx 0 - correct",
			blockNumber:  45,
			txIndex:      0,
			expectedRoot: env.blocks[44].TxMerkleTreeRootHash,
			expectedTx:   env.blockTx[44].Envelopes[0],
			user:         "testUser",
		},
		{
			name:         "Getting block 98, tx 90 - correct",
			blockNumber:  98,
			txIndex:      90,
			expectedRoot: env.blocks[97].TxMerkleTreeRootHash,
			expectedTx:   env.blockTx[97].Envelopes[90],
			user:         "testUser",
		},
		{
			name:        "Getting block 88, tx 100 - tx not exist",
			blockNumber: 88,
			txIndex:     100,
			user:        "testUser",
			expectedErr: &interrors.NotFoundErr{Message: "node with index 100 is not part of merkle tree (0, 87)"},
		},
		{
			name:        "Getting block 515 - not exist",
			blockNumber: 515,
			user:        "testUser",
			expectedErr: &interrors.NotFoundErr{Message: "requested block number [515] cannot be greater than the last committed block number [99]"},
		},
		{
			name:        "Getting block 40 - wrong user",
			blockNumber: 40,
			user:        "userNotExist",
			expectedErr: &interrors.PermissionErr{ErrMsg: "user userNotExist has no permission to access the ledger"},
		},
		{
			name:        "Getting block 77 - wrong user",
			blockNumber: 77,
			user:        "userNotExist",
			expectedErr: &interrors.PermissionErr{ErrMsg: "user userNotExist has no permission to access the ledger"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			payload, err := env.p.getTxProof(testCase.user, testCase.blockNumber, testCase.txIndex)
			if testCase.expectedErr == nil {
				require.NoError(t, err)
				txBytes, err := json.Marshal(testCase.expectedTx)
				require.NoError(t, err)
				valInfoBytes, err := json.Marshal(env.blocks[testCase.blockNumber-1].ValidationInfo[testCase.txIndex])
				require.NoError(t, err)
				txBytes = append(txBytes, valInfoBytes...)
				txHash, err := crypto.ComputeSHA256Hash(txBytes)
				require.NoError(t, err)
				var currRoot []byte
				for i, h := range payload.Hashes {
					if i == 0 {
						require.Equal(t, txHash, h)
						currRoot = txHash
					} else {
						currRoot, err = crypto.ConcatenateHashes(currRoot, h)
						require.NoError(t, err)
					}
				}
				require.Equal(t, testCase.expectedRoot, currRoot)
			} else {
				require.Error(t, err)
				require.EqualError(t, err, testCase.expectedErr.Error())
				require.IsType(t, testCase.expectedErr, err)
			}
		})
	}
}

func TestGetDataProof(t *testing.T) {
	env := newLedgerProcessorTestEnv(t)
	defer env.cleanup(t)
	setup(t, env, 100)

	testCases := []struct {
		name        string
		blockNumber uint64
		key         string
		value       []byte
		isDeleted   bool
		isValid     bool
		user        string
		expectedErr error
	}{
		{
			name:        "get regular data",
			blockNumber: 5,
			key:         "key3",
			value:       []byte(fmt.Sprintf("value_%d_%d", 3, 5)),
			isDeleted:   false,
			isValid:     true,
			user:        "testUser",
			expectedErr: nil,
		},
		{
			name:        "get regular data",
			blockNumber: 95,
			key:         "key13",
			value:       []byte(fmt.Sprintf("value_%d_%d", 13, 95)),
			isDeleted:   false,
			isValid:     true,
			user:        "testUser",
			expectedErr: nil,
		},
		{
			name:        "get regular data but with isDeleted true",
			blockNumber: 95,
			key:         "key13",
			value:       []byte(fmt.Sprintf("value_%d_%d", 13, 95)),
			isDeleted:   true,
			isValid:     false,
			user:        "testUser",
			expectedErr: &interrors.NotFoundErr{Message: "no proof for block 95, db bdb, key key13, isDeleted true found"},
		},
		{
			name:        "get already updated old value",
			blockNumber: 95,
			key:         "key13",
			value:       []byte(fmt.Sprintf("value_%d_%d", 13, 5)),
			isDeleted:   false,
			isValid:     false,
			user:        "testUser",
			expectedErr: nil,
		},
		{
			name:        "get for non exist key",
			blockNumber: 95,
			key:         "keyyyy13",
			value:       []byte(fmt.Sprintf("value_%d_%d", 13, 5)),
			isDeleted:   false,
			isValid:     false,
			user:        "testUser",
			expectedErr: &interrors.NotFoundErr{Message: "no proof for block 95, db bdb, key keyyyy13, isDeleted false found"},
		},
		{
			name:        "get proof from block 515 - not exist",
			blockNumber: 515,
			user:        "testUser",
			expectedErr: &interrors.NotFoundErr{Message: "block not found: 515"},
		},
		{
			name:        "get proof from block 40 - wrong user",
			blockNumber: 40,
			user:        "userNotExist",
			expectedErr: &interrors.PermissionErr{ErrMsg: "user userNotExist has no permission to access the ledger"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			proof, err := env.p.getDataProof(testCase.user, testCase.blockNumber, worldstate.DefaultDBName, testCase.key, testCase.isDeleted)
			if testCase.expectedErr == nil {
				require.NoError(t, err)
				require.NotNil(t, proof)
				mpTrieProof := state.NewProof(proof.Path)
				trieKey, err := state.ConstructCompositeKey(worldstate.DefaultDBName, testCase.key)
				require.NoError(t, err)
				kvHash, err := state.CalculateKeyValueHash(trieKey, testCase.value)
				rootHash := env.blocks[testCase.blockNumber-1].StateMerkleTreeRootHash
				require.NoError(t, err)
				isValid, err := mpTrieProof.Verify(kvHash, rootHash, testCase.isDeleted)
				require.NoError(t, err)
				require.Equal(t, testCase.isValid, isValid)
			} else {
				require.Error(t, err)
				require.EqualError(t, err, testCase.expectedErr.Error())
				require.IsType(t, testCase.expectedErr, err)
			}
		})
	}
}

func TestGetTxReceipt(t *testing.T) {
	env := newLedgerProcessorTestEnv(t)
	defer env.cleanup(t)
	setup(t, env, 20)

	testCases := []struct {
		name        string
		txId        string
		blockNumber uint64
		txIndex     uint64
		user        string
		expectedErr error
	}{
		{
			name:        "Getting receipt for Tx5key3 - correct",
			txId:        "Tx5key3",
			blockNumber: 5,
			txIndex:     3,
			user:        "testUser",
		},
		{
			name:        "Getting receipt for Tx15key13 - correct",
			txId:        "Tx15key13",
			blockNumber: 15,
			txIndex:     13,
			user:        "testUser",
		},
		{
			name:        "Getting receipt for Tx9key7 - correct",
			txId:        "Tx9key7",
			blockNumber: 9,
			txIndex:     7,
			user:        "testUser",
		},
		{
			name:        "Getting receipt for Tx19key17 - correct",
			txId:        "Tx19key17",
			blockNumber: 19,
			txIndex:     17,
			user:        "testUser",
		},
		{
			name:        "Getting receipt for Tx15key20 - no tx exist",
			txId:        "Tx15key20",
			blockNumber: 0,
			txIndex:     0,
			user:        "testUser",
			expectedErr: &interrors.NotFoundErr{Message: "txID not found: Tx15key20"},
		},
		{
			name:        "Getting receipt for Tx9key7 - no user exist",
			txId:        "Tx9key7",
			blockNumber: 0,
			txIndex:     0,
			user:        "nonExistUser",
			expectedErr: &interrors.PermissionErr{ErrMsg: "user nonExistUser has no permission to access the ledger"},
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			receipt, err := env.p.getTxReceipt(tt.user, tt.txId)
			if tt.expectedErr == nil {
				require.NoError(t, err)
				require.Equal(t, tt.txIndex, receipt.GetReceipt().GetTxIndex())
				require.True(t, proto.Equal(env.blocks[tt.blockNumber-1], receipt.GetReceipt().GetHeader()))
			} else {
				require.Error(t, err)
				require.EqualError(t, err, tt.expectedErr.Error())
				require.IsType(t, tt.expectedErr, err)
			}
		})
	}
}

func TestGetTx(t *testing.T) {
	env := newLedgerProcessorTestEnv(t)
	defer env.cleanup(t)
	setup(t, env, 20)

	testCases := []struct {
		name        string
		blockNumber uint64
		txIndex     uint64
		user        string

		expectedGetTxResponse *types.GetTxResponse
		expectedErr           error
	}{
		{
			name:        "Getting block 5, tx 2 - correct, user in must-sign",
			blockNumber: 5,
			txIndex:     2,
			user:        "testUser",
		},
		{
			name:        "Getting block 5, tx 2 - correct, user in signatures",
			blockNumber: 5,
			txIndex:     2,
			user:        "alice",
		},
		{
			name:        "Getting block 1, tx 0, config - correct, admin",
			blockNumber: 1,
			txIndex:     0,
			user:        "admin1",
		},
		{
			name:        "Getting block 1, tx 0, config - not admin",
			blockNumber: 1,
			txIndex:     0,
			user:        "testUser",
			expectedErr: &interrors.PermissionErr{ErrMsg: "user testUser has no permission to access the tx"},
		},
		{
			name:        "Getting block 10 - non existing user",
			blockNumber: 10,
			txIndex:     0,
			user:        "userNotExist",
			expectedErr: &interrors.PermissionErr{ErrMsg: "user userNotExist has no permission to access the tx"},
		},
		{
			name:        "Getting block 10 - another signed user - bad signature",
			blockNumber: 10,
			txIndex:     0,
			user:        "testUser2",
			expectedErr: &interrors.PermissionErr{ErrMsg: "user testUser2 has no permission to access the tx"},
		},
		{
			name:        "Getting block 10 - index out of range",
			blockNumber: 10,
			txIndex:     3000,
			user:        "testUser",
			expectedErr: &interrors.BadRequestError{ErrMsg: "transaction index out of range: 3000"},
		},
		{
			name:        "Getting block 200 - out of range",
			blockNumber: 200,
			txIndex:     0,
			user:        "testUser",
			expectedErr: &interrors.NotFoundErr{Message: "requested block number [200] cannot be greater than the last committed block number [19]"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			payload, err := env.p.getTx(testCase.user, testCase.blockNumber, testCase.txIndex)
			if testCase.expectedErr == nil {
				require.NoError(t, err)
				require.NotNil(t, payload)
				//TODO
			} else {
				require.Error(t, err)
				require.EqualError(t, err, testCase.expectedErr.Error())
				require.IsType(t, testCase.expectedErr, err)
			}
		})
	}
}
