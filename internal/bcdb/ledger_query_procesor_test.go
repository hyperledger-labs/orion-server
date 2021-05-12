// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/internal/blockstore"
	interrors "github.com/IBM-Blockchain/bcdb-server/internal/errors"
	"github.com/IBM-Blockchain/bcdb-server/internal/identity"
	"github.com/IBM-Blockchain/bcdb-server/internal/mtree"
	"github.com/IBM-Blockchain/bcdb-server/internal/provenance"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate/leveldb"
	"github.com/IBM-Blockchain/bcdb-server/pkg/crypto"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server/testutils"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type ledgerProcessorTestEnv struct {
	db      *leveldb.LevelDB
	p       *ledgerQueryProcessor
	cleanup func(t *testing.T)
	blocks  []*types.BlockHeader
	blockTx []*types.DataTxEnvelopes
}

func newLedgerProcessorTestEnv(t *testing.T) *ledgerProcessorTestEnv {
	path, err := ioutil.TempDir("/tmp", "ledgerQueryProcessor")
	require.NoError(t, err)

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
			t.Errorf("error while closing blockstore, %v", err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Fatalf("failed to remove %s due to %v", path, err)
		}
	}

	conf := &ledgerQueryProcessorConfig{
		db:              db,
		blockStore:      blockStore,
		provenanceStore: provenanceStore,
		identityQuerier: identity.NewQuerier(db),
		logger:          logger,
	}

	return &ledgerProcessorTestEnv{
		db:      db,
		p:       newLedgerQueryProcessor(conf),
		cleanup: cleanup,
	}
}

func setup(t *testing.T, env *ledgerProcessorTestEnv, blocksNum int) {
	instCert, adminCert := generateCrypto(t)
	//	dcCert, _ := pem.Decode(cert)

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
					UserID:               "adminUser",
					ReadOldConfigVersion: nil,
					NewConfig: &types.ClusterConfig{
						Nodes: []*types.NodeConfig{
							{
								ID:          "node1",
								Address:     "127.0.0.1",
								Certificate: instCert,
							},
						},
						Admins: []*types.Admin{
							{
								ID:          "admin1",
								Certificate: adminCert,
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
	configBlock.Header.TxMerkelTreeRootHash = root.Hash()
	require.NoError(t, env.p.blockStore.Commit(configBlock))
	env.blocks = []*types.BlockHeader{configBlock.GetHeader()}
	env.blockTx = []*types.DataTxEnvelopes{{}}

	user := &types.User{
		ID: "testUser",
		Privilege: &types.Privilege{
			DBPermission: map[string]types.Privilege_Access{
				worldstate.DefaultDBName: types.Privilege_ReadWrite,
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
					Key:   string(identity.UserNamespace) + "testUser",
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

	dirtyWriteKeyVersion := make(map[string]*types.Version)

	for i := uint64(2); i < uint64(blocksNum); i++ {
		key := make([]string, 0)
		value := make([][]byte, 0)
		for j := uint64(0); j < i; j++ {
			key = append(key, fmt.Sprintf("key%d", j))
			value = append(value, []byte(fmt.Sprintf("value_%d", j)))
		}
		block := createSampleBlock(i, key, value)
		require.NoError(t, env.p.blockStore.AddSkipListLinks(block))
		root, err := mtree.BuildTreeForBlockTx(block)
		require.NoError(t, err)
		block.Header.TxMerkelTreeRootHash = root.Hash()
		require.NoError(t, env.p.blockStore.Commit(block))

		pData := createProvenanceDataFromBlock(block, dirtyWriteKeyVersion)
		err = env.p.provenanceStore.Commit(block.GetHeader().GetBaseHeader().GetNumber(), pData)
		require.NoError(t, err)

		env.blocks = append(env.blocks, block.GetHeader())
		env.blockTx = append(env.blockTx, block.GetDataTxEnvelopes())
	}
}

func createSampleBlock(blockNumber uint64, key []string, value [][]byte) *types.Block {
	envelopes := make([]*types.DataTxEnvelope, 0)
	for i := 0; i < len(key); i++ {
		e := &types.DataTxEnvelope{
			Payload: &types.DataTx{
				MustSignUserIDs: []string{"testUser"},
				TxID:            fmt.Sprintf("Tx%d%s", blockNumber, key[i]),
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataWrites: []*types.DataWrite{
							{
								Key:   key[i],
								Value: value[i],
							},
						},
					},
				},
			},
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

func createProvenanceDataFromBlock(block *types.Block, dirtyWriteKeyVersion map[string]*types.Version) []*provenance.TxDataForProvenance {
	var provenanceData []*provenance.TxDataForProvenance
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
	txpData := make([]*provenance.TxDataForProvenance, len(tx.DBOperations))

	for i, ops := range tx.DBOperations {
		txpData[i] = &provenance.TxDataForProvenance{
			DBName:             ops.DBName,
			UserID:             tx.MustSignUserIDs[0],
			TxID:               tx.TxID,
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
					AccessControl: write.ACL,
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

func TestGetBlock(t *testing.T) {
	t.Parallel()
	env := newLedgerProcessorTestEnv(t)
	defer env.cleanup(t)
	setup(t, env, 20)

	testCases := []struct {
		name                string
		blockNumber         uint64
		expectedBlockHeader *types.BlockHeader
		user                string
		expectedErr         error
	}{
		{
			name:                "Getting block 5 - correct",
			blockNumber:         5,
			expectedBlockHeader: env.blocks[4],
			user:                "testUser",
		},
		{
			name:                "Getting block 17 - correct",
			blockNumber:         17,
			expectedBlockHeader: env.blocks[16],
			user:                "testUser",
		},
		{
			name:                "Getting block 12 - correct",
			blockNumber:         12,
			expectedBlockHeader: env.blocks[11],
			user:                "testUser",
		},
		{
			name:                "Getting block 9 - correct",
			blockNumber:         9,
			expectedBlockHeader: env.blocks[8],
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
		})
	}
}

func TestGetPath(t *testing.T) {
	t.Parallel()
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
			name:           "path 17 2 wrong user",
			startNumber:    2,
			endNumber:      17,
			expectedBlocks: nil,
			user:           "userNotExist",
			expectedErr:    &interrors.PermissionErr{ErrMsg: "user userNotExist has no permission to access the ledger"},
		},
		{
			name:           "path 2 17 wrong direction",
			startNumber:    17,
			endNumber:      2,
			expectedBlocks: nil,
			user:           "testUser",
			expectedErr:    errors.New("can't find path from smaller block 2 to bigger 17"),
		},
		{
			name:           "path 2 117 end block not in ledger",
			startNumber:    2,
			endNumber:      117,
			expectedBlocks: nil,
			user:           "testUser",
			expectedErr:    &interrors.NotFoundErr{Message: "can't find path in blocks skip list between 117 2: block not found: 117"},
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

func TestGetProof(t *testing.T) {
	t.Parallel()
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
			expectedRoot: env.blocks[4].TxMerkelTreeRootHash,
			expectedTx:   env.blockTx[4].Envelopes[2],
			user:         "testUser",
		},
		{
			name:         "Getting block 17, tx 5 - correct",
			blockNumber:  17,
			txIndex:      5,
			expectedRoot: env.blocks[16].TxMerkelTreeRootHash,
			expectedTx:   env.blockTx[16].Envelopes[5],
			user:         "testUser",
		},
		{
			name:         "Getting block 45, tx 0 - correct",
			blockNumber:  45,
			txIndex:      0,
			expectedRoot: env.blocks[44].TxMerkelTreeRootHash,
			expectedTx:   env.blockTx[44].Envelopes[0],
			user:         "testUser",
		},
		{
			name:         "Getting block 98, tx 90 - correct",
			blockNumber:  98,
			txIndex:      90,
			expectedRoot: env.blocks[97].TxMerkelTreeRootHash,
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
			payload, err := env.p.getProof(testCase.user, testCase.blockNumber, testCase.txIndex)
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

func TestGetTxReceipt(t *testing.T) {
	t.Parallel()
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
			expectedErr: &interrors.NotFoundErr{Message: "TxID not found: Tx15key20"},
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

func generateCrypto(t *testing.T) ([]byte, []byte) {
	rootCAPemCert, caPrivKey, err := testutils.GenerateRootCA("BCDB RootCA", "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, rootCAPemCert)
	require.NotNil(t, caPrivKey)

	keyPair, err := tls.X509KeyPair(rootCAPemCert, caPrivKey)
	require.NoError(t, err)
	require.NotNil(t, keyPair)

	instCertPem, _, err := testutils.IssueCertificate("BCDB Instance", "127.0.0.1", keyPair)
	require.NoError(t, err)

	adminCertPem, _, err := testutils.IssueCertificate("BCDB Admin", "127.0.0.1", keyPair)
	require.NoError(t, err)
	return instCertPem, adminCertPem
}
