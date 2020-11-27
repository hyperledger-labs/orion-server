package backend

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/blockstore"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/mtree"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/internal/worldstate/leveldb"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type ledgerProcessorTestEnv struct {
	db      *leveldb.LevelDB
	p       *ledgerQueryProcessor
	cleanup func(t *testing.T)
	blocks  []*types.BlockHeader
	blockTx []*types.DataTxEnvelopes
}

func newLedgerProcessorTestEnv(t *testing.T) *ledgerProcessorTestEnv {
	nodeID := "test-node-id1"
	cryptoPath := testutils.GenerateTestClientCrypto(t, []string{nodeID})
	_, nodeSigner := testutils.LoadTestClientCrypto(t, cryptoPath, nodeID)

	path, err := ioutil.TempDir("/tmp", "ledgerQueryProcessor")
	require.NoError(t, err)

	c := &logger.Config{
		Level:         "debug",
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

	cleanup := func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close leveldb: %v", err)
		}
		if err := blockStore.Close(); err != nil {
			t.Errorf("error while closing blockstore, %v", err)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Fatalf("failed to remove %s due to %v", path, err)
		}
	}

	conf := &ledgerQueryProcessorConfig{
		nodeID:          nodeID,
		signer:          nodeSigner,
		db:              db,
		blockStore:      blockStore,
		identityQuerier: identity.NewQuerier(db),
		logger:          logger,
	}

	return &ledgerProcessorTestEnv{
		db:      db,
		p:       newLedgerQueryProcessor(conf),
		cleanup: cleanup,
	}
}

func setup(t *testing.T, env *ledgerProcessorTestEnv) {
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

	for i := uint64(2); i < uint64(100); i++ {
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

		env.blocks = append(env.blocks, block.GetHeader())
		env.blockTx = append(env.blockTx, block.GetDataTxEnvelopes())
	}
}

func createSampleBlock(blockNumber uint64, key []string, value [][]byte) *types.Block {
	envelopes := make([]*types.DataTxEnvelope, 0)
	for i := 0; i < len(key); i++ {
		e := &types.DataTxEnvelope{
			Payload: &types.DataTx{
				UserID: "testUser",
				DBName: worldstate.DefaultDBName,
				DataWrites: []*types.DataWrite{
					{
						Key:   key[i],
						Value: value[i],
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

func TestGetBlock(t *testing.T) {
	t.Parallel()
	env := newLedgerProcessorTestEnv(t)
	defer env.cleanup(t)
	setup(t, env)

	testCases := []struct {
		name          string
		blockNumber   uint64
		expectedBlock *types.BlockHeader
		user          string
		isError       bool
		errorMsg      string
	}{
		{
			name:          "Getting block 5 - correct",
			blockNumber:   5,
			expectedBlock: env.blocks[4],
			user:          "testUser",
			isError:       false,
		},
		{
			name:          "Getting block 17 - correct",
			blockNumber:   17,
			expectedBlock: env.blocks[16],
			user:          "testUser",
			isError:       false,
		},
		{
			name:          "Getting block 45 - correct",
			blockNumber:   45,
			expectedBlock: env.blocks[44],
			user:          "testUser",
			isError:       false,
		},
		{
			name:          "Getting block 98 - correct",
			blockNumber:   98,
			expectedBlock: env.blocks[97],
			user:          "testUser",
			isError:       false,
		},
		{
			name:          "Getting block 101 - not exist",
			blockNumber:   101,
			expectedBlock: nil,
			user:          "testUser",
			isError:       false,
		},
		{
			name:          "Getting block 515 - not exist",
			blockNumber:   515,
			expectedBlock: nil,
			user:          "testUser",
			isError:       false,
		},
		{
			name:          "Getting block 40 - wrong user",
			blockNumber:   40,
			expectedBlock: nil,
			user:          "userNotExist",
			isError:       true,
			errorMsg:      "user userNotExist doesn't has permision to access ledger",
		},
		{
			name:          "Getting block 77 - wrong user",
			blockNumber:   77,
			expectedBlock: nil,
			user:          "userNotExist",
			isError:       true,
			errorMsg:      "user userNotExist doesn't has permision to access ledger",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			res, err := env.p.getBlockHeader(testCase.user, testCase.blockNumber)
			if !testCase.isError {
				require.NoError(t, err)
				if testCase.expectedBlock != nil {
					require.True(t, proto.Equal(testCase.expectedBlock, res.GetPayload().GetBlockHeader()))
				} else {
					require.Nil(t, res.GetPayload().GetBlockHeader())
				}
			} else {
				require.Error(t, err)
				require.Contains(t, testCase.errorMsg, err.Error())
			}
		})
	}
}

func TestGetPath(t *testing.T) {
	t.Parallel()
	env := newLedgerProcessorTestEnv(t)
	defer env.cleanup(t)
	setup(t, env)

	testCases := []struct {
		name           string
		startNumber    uint64
		endNumber      uint64
		expectedBlocks []*types.BlockHeader
		user           string
		isError        bool
		errorMsg       string
	}{
		{
			name:           "path 2 1",
			startNumber:    1,
			endNumber:      2,
			expectedBlocks: []*types.BlockHeader{env.blocks[1], env.blocks[0]},
			user:           "testUser",
			isError:        false,
			errorMsg:       "",
		},
		{
			name:           "path 4 1",
			startNumber:    1,
			endNumber:      4,
			expectedBlocks: []*types.BlockHeader{env.blocks[3], env.blocks[2], env.blocks[0]},
			user:           "testUser",
			isError:        false,
			errorMsg:       "",
		},
		{
			name:           "path 17 1",
			startNumber:    1,
			endNumber:      17,
			expectedBlocks: []*types.BlockHeader{env.blocks[16], env.blocks[0]},
			user:           "testUser",
			isError:        false,
			errorMsg:       "",
		},
		{
			name:           "path 17 2",
			startNumber:    2,
			endNumber:      17,
			expectedBlocks: []*types.BlockHeader{env.blocks[16], env.blocks[8], env.blocks[4], env.blocks[2], env.blocks[1]},
			user:           "testUser",
			isError:        false,
			errorMsg:       "",
		},
		{
			name:           "path 90 6",
			startNumber:    6,
			endNumber:      90,
			expectedBlocks: []*types.BlockHeader{env.blocks[89], env.blocks[88], env.blocks[80], env.blocks[64], env.blocks[32], env.blocks[16], env.blocks[8], env.blocks[6], env.blocks[5]},
			user:           "testUser",
			isError:        false,
			errorMsg:       "",
		},
		{
			name:           "path 17 2 wrong user",
			startNumber:    2,
			endNumber:      17,
			expectedBlocks: nil,
			user:           "userNotExist",
			isError:        true,
			errorMsg:       "user userNotExist doesn't has permision to access ledger",
		},
		{
			name:           "path 2 17 wrong direction",
			startNumber:    17,
			endNumber:      2,
			expectedBlocks: nil,
			user:           "testUser",
			isError:        true,
			errorMsg:       "can't find path from smaller block 2 to bigger 17",
		},
		{
			name:           "path 2 117 end block not in ledger",
			startNumber:    2,
			endNumber:      117,
			expectedBlocks: nil,
			user:           "testUser",
			isError:        true,
			errorMsg:       "can't find path in blocks skip list between 117 2, end block not exist",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			path, err := env.p.getPath(testCase.user, testCase.startNumber, testCase.endNumber)
			if testCase.isError {
				require.Error(t, err)
				require.Nil(t, path)
				require.Contains(t, err.Error(), testCase.errorMsg)
			} else {
				require.NoError(t, err)
				require.NotNil(t, path)
				require.Equal(t, len(testCase.expectedBlocks), len(path.GetPayload().GetBlockHeaders()))
				for idx, expectedBlock := range testCase.expectedBlocks {
					require.True(t, proto.Equal(expectedBlock, path.GetPayload().GetBlockHeaders()[idx]))
				}
			}
		})
	}
}

func TestGetProof(t *testing.T) {
	t.Parallel()
	env := newLedgerProcessorTestEnv(t)
	defer env.cleanup(t)
	setup(t, env)

	testCases := []struct {
		name         string
		blockNumber  uint64
		txIndex      uint64
		expectedRoot []byte
		expectedTx   *types.DataTxEnvelope
		user         string
		isError      bool
		errorMsg     string
	}{
		{
			name:         "Getting block 5, tx 2 - correct",
			blockNumber:  5,
			txIndex:      2,
			expectedRoot: env.blocks[4].TxMerkelTreeRootHash,
			expectedTx:   env.blockTx[4].Envelopes[2],
			user:         "testUser",
			isError:      false,
		},
		{
			name:         "Getting block 17, tx 5 - correct",
			blockNumber:  17,
			txIndex:      5,
			expectedRoot: env.blocks[16].TxMerkelTreeRootHash,
			expectedTx:   env.blockTx[16].Envelopes[5],
			user:         "testUser",
			isError:      false,
		},
		{
			name:         "Getting block 45, tx 0 - correct",
			blockNumber:  45,
			txIndex:      0,
			expectedRoot: env.blocks[44].TxMerkelTreeRootHash,
			expectedTx:   env.blockTx[44].Envelopes[0],
			user:         "testUser",
			isError:      false,
		},
		{
			name:         "Getting block 98, tx 90 - correct",
			blockNumber:  98,
			txIndex:      90,
			expectedRoot: env.blocks[97].TxMerkelTreeRootHash,
			expectedTx:   env.blockTx[97].Envelopes[90],
			user:         "testUser",
			isError:      false,
		},
		{
			name:        "Getting block 88, tx 100 - tx not exist",
			blockNumber: 88,
			txIndex:     100,
			user:        "testUser",
			isError:     true,
			errorMsg:    ": node with index 100 is not part of merkle tree (0, 87)",
		},
		{
			name:        "Getting block 515 - not exist",
			blockNumber: 515,
			user:        "testUser",
			isError:     true,
			errorMsg:    "requested block number [515] cannot be greater than the last committed block number [99]",
		},
		{
			name:        "Getting block 40 - wrong user",
			blockNumber: 40,
			user:        "userNotExist",
			isError:     true,
			errorMsg:    "user userNotExist doesn't has permision to access ledger",
		},
		{
			name:        "Getting block 77 - wrong user",
			blockNumber: 77,
			user:        "userNotExist",
			isError:     true,
			errorMsg:    "user userNotExist doesn't has permision to access ledger",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			proof, err := env.p.getProof(testCase.user, testCase.blockNumber, testCase.txIndex)
			if !testCase.isError {
				require.NoError(t, err)
				txBytes, err := json.Marshal(testCase.expectedTx)
				require.NoError(t, err)
				valInfoBytes, err := json.Marshal(env.blocks[testCase.blockNumber-1].ValidationInfo[testCase.txIndex])
				require.NoError(t, err)
				txBytes = append(txBytes, valInfoBytes...)
				currRoot, err := crypto.ComputeSHA256Hash(txBytes)
				require.NoError(t, err)
				for _, h := range proof.Payload.Hashes {
					currRoot, err = crypto.ConcatenateHashes(currRoot, h)
					require.NoError(t, err)
				}
				require.Equal(t, testCase.expectedRoot, currRoot)
			} else {
				require.Error(t, err)
				require.Contains(t, testCase.errorMsg, err.Error())
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
