// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package datatxtest

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hyperledger-labs/orion-server/config"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
)

var baseNodePort uint32
var basePeerPort uint32
var portMutex sync.Mutex

func init() {
	baseNodePort = 11123
	basePeerPort = 11134
}

func getPorts(num uint32) (node uint32, peer uint32) {
	portMutex.Lock()
	defer portMutex.Unlock()

	node = baseNodePort
	peer = basePeerPort
	baseNodePort += num
	basePeerPort += num

	return
}

func TestDataTx(t *testing.T) {
	dir, err := ioutil.TempDir("", "int-test")
	require.NoError(t, err)

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())
	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	s := c.Servers[0]

	// creating two databases -- db1 and db2
	setup.CreateDatabases(t, s, []string{"db1", "db2"}, nil)

	// creating two users -- alice and bob
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "alice")
	bobCert, bobSigner := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "bob")
	setup.CreateUsers(t, s, []*types.UserWrite{
		{
			User: &types.User{
				Id:          "alice",
				Certificate: aliceCert.Raw,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_ReadWrite,
						"db2": types.Privilege_ReadWrite,
					},
				},
			},
		},
		{
			User: &types.User{
				Id:          "bob",
				Certificate: bobCert.Raw,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_ReadWrite,
						"db2": types.Privilege_ReadWrite,
					},
				},
			},
		},
	})

	t.Run("blind write to a single database", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataWrites: []*types.DataWrite{
						{
							Key:   "key1",
							Value: []byte("key1value1"),
						},
						{
							Key:   "key2",
							Value: []byte("key2value1"),
							Acl: &types.AccessControl{
								ReadWriteUsers: map[string]bool{"alice": true},
							},
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.NoError(t, err)

		for _, keyIndex := range []string{"1", "2"} {
			resp, err := s.QueryData(t, "db1", "key"+keyIndex, "alice")
			require.NoError(t, err)
			require.Equal(t, []byte("key"+keyIndex+"value1"), resp.GetResponse().GetValue())
		}
	})

	t.Run("blind write to an existing key in a single database", func(t *testing.T) {
		resp, err := s.QueryData(t, "db1", "key1", "alice")
		require.NoError(t, err)
		require.Equal(t, []byte("key1value1"), resp.GetResponse().GetValue())

		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataWrites: []*types.DataWrite{
						{
							Key:   "key1",
							Value: []byte("key1value2"),
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.NoError(t, err)

		resp, err = s.QueryData(t, "db1", "key1", "alice")
		require.NoError(t, err)
		require.Equal(t, []byte("key1value2"), resp.GetResponse().GetValue())
	})

	t.Run("write only if not exist", func(t *testing.T) {
		resp, err := s.QueryData(t, "db1", "key10", "alice")
		require.NoError(t, err)
		require.Nil(t, resp.GetResponse().GetValue())

		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key:     "key10",
							Version: nil,
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "key10",
							Value: []byte("key10value1"),
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.NoError(t, err)

		resp, err = s.QueryData(t, "db1", "key10", "alice")
		require.NoError(t, err)
		require.Equal(t, []byte("key10value1"), resp.GetResponse().GetValue())
	})

	t.Run("invalid: version 0 is not the same as nil", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key: "key11",
							Version: &types.Version{
								BlockNum: 0,
								TxNum:    0,
							},
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "key11",
							Value: []byte("key10value1"),
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, Reason: mvcc conflict has occurred as the committed state for the key [key11] in database [db1] changed")
	})

	t.Run("read-write-delete to a single database", func(t *testing.T) {
		resp, err := s.QueryData(t, "db1", "key1", "alice")
		require.NoError(t, err)
		verKey1 := resp.GetResponse().GetMetadata().GetVersion()
		require.NotNil(t, verKey1)

		resp, err = s.QueryData(t, "db1", "key2", "alice")
		require.NoError(t, err)
		verKey2 := resp.GetResponse().GetMetadata().GetVersion()
		require.NotNil(t, verKey2)

		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key:     "key1",
							Version: verKey1,
						},
						{
							Key:     "key2",
							Version: verKey2,
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "key1",
							Value: []byte("key1value2"),
						},
					},
					DataDeletes: []*types.DataDelete{
						{
							Key: "key2",
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.NoError(t, err)

		resp, err = s.QueryData(t, "db1", "key1", "alice")
		require.NoError(t, err)
		require.Equal(t, []byte("key1value2"), resp.GetResponse().GetValue())

		resp, err = s.QueryData(t, "db1", "key2", "alice")
		require.NoError(t, err)
		require.Nil(t, resp.GetResponse().GetValue())
	})

	t.Run("blind write to multiple databases", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataWrites: []*types.DataWrite{
						{
							Key:   "key3",
							Value: []byte("key3value1"),
							Acl: &types.AccessControl{
								ReadWriteUsers:     map[string]bool{"alice": true, "bob": true},
								SignPolicyForWrite: types.AccessControl_ALL,
							},
						},
					},
				},
				{
					DbName: "db2",
					DataWrites: []*types.DataWrite{
						{
							Key:   "key4",
							Value: []byte("key4value1"),
							Acl: &types.AccessControl{
								ReadWriteUsers:     map[string]bool{"alice": true, "bob": true},
								SignPolicyForWrite: types.AccessControl_ALL,
							},
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.NoError(t, err)

		resp, err := s.QueryData(t, "db1", "key3", "alice")
		require.NoError(t, err)
		require.Equal(t, []byte("key3value1"), resp.GetResponse().GetValue())

		resp, err = s.QueryData(t, "db2", "key4", "alice")
		require.NoError(t, err)
		require.Equal(t, []byte("key4value1"), resp.GetResponse().GetValue())
	})

	t.Run("read-write-delete to multiple databases and multi-users", func(t *testing.T) {
		resp, err := s.QueryData(t, "db1", "key3", "alice")
		require.NoError(t, err)
		verKey3 := resp.GetResponse().GetMetadata().GetVersion()
		require.NotNil(t, verKey3)

		resp, err = s.QueryData(t, "db2", "key4", "alice")
		require.NoError(t, err)
		verKey4 := resp.GetResponse().GetMetadata().GetVersion()
		require.NotNil(t, verKey4)

		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
				"bob",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key:     "key3",
							Version: verKey3,
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "key3",
							Value: []byte("key3value2"),
							Acl: &types.AccessControl{
								ReadWriteUsers:     map[string]bool{"alice": true, "bob": true},
								SignPolicyForWrite: types.AccessControl_ALL,
							},
						},
						{
							Key:   "key5",
							Value: []byte("key5constantValue"),
							Acl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
					DataDeletes: []*types.DataDelete{
						{
							Key: "key1",
						},
					},
				},
				{
					DbName: "db2",
					DataReads: []*types.DataRead{
						{
							Key:     "key4",
							Version: verKey4,
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "key4",
							Value: []byte("key4value2"),
							Acl: &types.AccessControl{
								ReadWriteUsers:     map[string]bool{"alice": true, "bob": true},
								SignPolicyForWrite: types.AccessControl_ALL,
							},
						},
						{
							Key:   "key6",
							Value: []byte("key6constantValue"),
							Acl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"bob": true,
								},
							},
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx), "bob": testutils.SignatureFromTx(t, bobSigner, dataTx)},
		})
		require.NoError(t, err)

		resp, err = s.QueryData(t, "db1", "key3", "bob")
		require.NoError(t, err)
		require.Equal(t, []byte("key3value2"), resp.GetResponse().GetValue())

		resp, err = s.QueryData(t, "db1", "key5", "alice")
		require.NoError(t, err)
		require.Equal(t, []byte("key5constantValue"), resp.GetResponse().GetValue())

		resp, err = s.QueryData(t, "db2", "key4", "alice")
		require.NoError(t, err)
		require.Equal(t, []byte("key4value2"), resp.GetResponse().GetValue())

		resp, err = s.QueryData(t, "db2", "key6", "bob")
		require.NoError(t, err)
		require.Equal(t, []byte("key6constantValue"), resp.GetResponse().GetValue())
	})

	t.Run("invalid: mvcc conflict due to nil version", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key: "key3",
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "key7",
							Value: []byte("key7value1"),
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, Reason: mvcc conflict has occurred as the committed state for the key [key3] in database [db1] changed")
	})

	t.Run("invalid: mvcc conflict due to lower version", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key: "key3",
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    10,
							},
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "key7",
							Value: []byte("key7value1"),
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, Reason: mvcc conflict has occurred as the committed state for the key [key3] in database [db1] changed")
	})

	t.Run("invalid: mvcc conflict due to higher version", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key: "key3",
							Version: &types.Version{
								BlockNum: 1000,
								TxNum:    10,
							},
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "key7",
							Value: []byte("key7value1"),
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, Reason: mvcc conflict has occurred as the committed state for the key [key3] in database [db1] changed")
	})

	t.Run("invalid: key being deleted does not exist error", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataDeletes: []*types.DataDelete{
						{
							Key: "key8",
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_INCORRECT_ENTRIES, Reason: the key [key8] does not exist in the database and hence, it cannot be deleted")
	})

	t.Run("invalid: key being updated is also deleted error", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{"alice"},
			TxId:            uuid.New().String(),
			DbOperations: []*types.DBOperation{{
				DbName: "db1",
				DataWrites: []*types.DataWrite{
					{
						Key:   "key3",
						Value: []byte("key3value3"),
					},
				},
				DataDeletes: []*types.DataDelete{
					{
						Key: "key3",
					},
				},
			},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_INCORRECT_ENTRIES, Reason: the key [key3] is being updated as well as deleted. Only one operation per key is allowed within a transaction")
	})

	t.Run("invalid: modifying read-only key would result in an error", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{"alice"},
			TxId:            uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataWrites: []*types.DataWrite{
						{
							Key:   "key5",
							Value: []byte("key5value2"),
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_NO_PERMISSION, Reason: no user can write or delete the key [key5]")
	})

	t.Run("invalid: only users in read acl can read", func(t *testing.T) {
		resp, err := s.QueryData(t, "db1", "key5", "alice")
		require.NoError(t, err)
		require.Equal(t, []byte("key5constantValue"), resp.GetResponse().GetValue())

		_, err = s.QueryData(t, "db1", "key5", "bob")
		require.EqualError(t, err, "error while processing 'GET /data/db1/key5' because the user [bob] has no permission to read key [key5] from database [db1]")
	})

	t.Run("invalid: inadequate signature error", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{"alice"},
			TxId:            uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db2",
					DataWrites: []*types.DataWrite{
						{
							Key:   "key4",
							Value: []byte("key3value2"),
						},
					},
				},
			},
		}

		_, err = s.SubmitTransaction(t, constants.PostDataTx, &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_NO_PERMISSION, Reason: not all required users in [alice,bob] have signed the transaction to write/delete key [key4] present in the database [db2]")
	})
}

func TestAsyncDataTx(t *testing.T) {
	dir, err := ioutil.TempDir("", "int-test")
	require.NoError(t, err)

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort,
		BasePeerPort:        pPort,
		BlockCreationOverride: &config.BlockCreationConf{
			MaxBlockSize:                1024 * 1024,
			MaxTransactionCountPerBlock: 100,
			BlockTimeout:                1 * time.Second,
		},
	}
	c, err := setup.NewCluster(setupConfig)
	require.NoError(t, err)
	defer c.ShutdownAndCleanup()

	require.NoError(t, c.Start())
	leaderIndex := -1
	require.Eventually(t, func() bool {
		leaderIndex = c.AgreedLeader(t, 0)
		return leaderIndex >= 0
	}, 30*time.Second, 100*time.Millisecond)

	s := c.Servers[0]

	// creating two databases -- db1 and db2
	setup.CreateDatabases(t, s, []string{"db1", "db2"}, nil)

	// creating two users -- alice and bob
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "alice")
	bobCert, bobSigner := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "bob")
	setup.CreateUsers(t, s, []*types.UserWrite{
		{
			User: &types.User{
				Id:          "alice",
				Certificate: aliceCert.Raw,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_ReadWrite,
						"db2": types.Privilege_ReadWrite,
					},
				},
			},
		},
		{
			User: &types.User{
				Id:          "bob",
				Certificate: bobCert.Raw,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_ReadWrite,
						"db2": types.Privilege_ReadWrite,
					},
				},
			},
		},
	})

	t.Run("multiple independent blind writes", func(t *testing.T) {
		var txEnvs []*types.DataTxEnvelope
		for i := 1; i <= 1000; i++ {
			dataTx := &types.DataTx{
				MustSignUserIds: []string{
					"alice",
				},
				TxId: uuid.New().String(),
				DbOperations: []*types.DBOperation{
					{
						DbName: "db1",
						DataWrites: []*types.DataWrite{
							{
								Key:   fmt.Sprintf("keyA%d", i),
								Value: []byte(fmt.Sprintf("keyA%d-value1", i)),
							},
						},
					},
				},
			}
			txEnv := &types.DataTxEnvelope{
				Payload:    dataTx,
				Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
			}
			txEnvs = append(txEnvs, txEnv)
		}

		for _, txEnv := range txEnvs {
			err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
			require.NoError(t, err)
		}

		allIn := func() bool {
			for i := 1; i <= 1000; i++ {
				resp, err := s.QueryData(t, "db1", fmt.Sprintf("keyA%d", i), "alice")
				if err != nil {
					return false
				}
				if resp.GetResponse().GetValue() == nil {
					return false
				}
				require.Equal(t, []byte(fmt.Sprintf("keyA%d-value1", i)), resp.GetResponse().GetValue())
			}
			return true
		}

		require.Eventually(t, allIn, 30*time.Second, 100*time.Millisecond)
	})

	t.Run("multiple blind writes to the same key on the same block", func(t *testing.T) {
		var txEnvs []*types.DataTxEnvelope

		for i := 1; i <= 100; i++ {
			for j := 1; j <= 5; j++ {
				dataTx := &types.DataTx{
					MustSignUserIds: []string{
						"alice",
					},
					TxId: uuid.New().String(),
					DbOperations: []*types.DBOperation{
						{
							DbName: "db1",
							DataWrites: []*types.DataWrite{
								{
									Key:   fmt.Sprintf("keyB%d", j),
									Value: []byte(fmt.Sprintf("keyB%d-value%d", j, i)),
								},
							},
						},
					},
				}
				txEnv := &types.DataTxEnvelope{
					Payload:    dataTx,
					Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
				}
				txEnvs = append(txEnvs, txEnv)
			}
		}

		for _, txEnv := range txEnvs {
			err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
			require.NoError(t, err)
		}

		blockHeaders := map[uint64][]*types.ValidationInfo{}
		firstBlockNum := uint64(100000000)
		allIn := func() bool {
			for _, txEnv := range txEnvs {
				resp, err := s.QueryTxReceipt(t, txEnv.GetPayload().GetTxId(), "alice")
				if err != nil {
					return false
				}
				if resp.GetResponse().GetReceipt() == nil {
					return false
				}
				blockNum := resp.GetResponse().GetReceipt().GetHeader().GetBaseHeader().GetNumber()
				blockHeaders[blockNum] = resp.GetResponse().GetReceipt().GetHeader().GetValidationInfo()
				if blockNum < firstBlockNum {
					firstBlockNum = blockNum
				}
			}
			return true
		}
		require.Eventually(t, allIn, 30*time.Second, 100*time.Millisecond)

		for i := firstBlockNum; i < uint64(len(blockHeaders))+firstBlockNum; i++ {
			validTxs := 0
			for _, v := range blockHeaders[i] {
				if v.GetFlag() == types.Flag_VALID {
					validTxs++
				}
			}
			if len(blockHeaders[i]) < 5 {
				require.Equal(t, len(blockHeaders[i]), validTxs)
			} else {
				require.Equal(t, 5, validTxs)
			}
		}

		for j := 1; j <= 5; j++ {
			resp, err := s.QueryData(t, "db1", fmt.Sprintf("keyB%d", j), "alice")
			require.NoError(t, err)
			require.NotNil(t, resp.GetResponse().GetValue())
			t.Logf("key%d : %s", j, string(resp.GetResponse().GetValue()))
		}
	})

	// Note: this may break once we implement early rejection optimization
	t.Run("multiple mvcc conflicts on inserting the same key in the same block", func(t *testing.T) {
		var txEnvs []*types.DataTxEnvelope

		for i := 1; i <= 100; i++ {
			for j := 1; j <= 5; j++ {
				dataTx := &types.DataTx{
					MustSignUserIds: []string{
						"alice",
					},
					TxId: uuid.New().String(),
					DbOperations: []*types.DBOperation{
						{
							DbName: "db1",
							DataReads: []*types.DataRead{
								{
									Key:     fmt.Sprintf("keyC%d", j),
									Version: nil, // an insert
								},
							},
							DataWrites: []*types.DataWrite{
								{
									Key:   fmt.Sprintf("keyC%d", j),
									Value: []byte(fmt.Sprintf("keyC%d-value%d", j, i)),
								},
							},
						},
					},
				}
				txEnv := &types.DataTxEnvelope{
					Payload:    dataTx,
					Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
				}
				txEnvs = append(txEnvs, txEnv)
			}
		}

		for _, txEnv := range txEnvs {
			err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
			require.NoError(t, err)
		}

		allIn := func() bool {
			for _, txEnv := range txEnvs {
				resp, err := s.QueryTxReceipt(t, txEnv.GetPayload().GetTxId(), "alice")
				if err != nil {
					return false
				}
				if resp.GetResponse().GetReceipt() == nil {
					return false
				}

			}
			return true
		}

		require.Eventually(t, allIn, 30*time.Second, 100*time.Millisecond)

		for j := 1; j <= 5; j++ {
			resp, err := s.QueryData(t, "db1", fmt.Sprintf("keyC%d", j), "alice")
			require.NoError(t, err)
			require.Equal(t, []byte(fmt.Sprintf("keyC%d-value1", j)), resp.GetResponse().GetValue())
		}
	})

	t.Run("multiple writes only if not exist", func(t *testing.T) {
		var txEnvs []*types.DataTxEnvelope
		for i := 1; i <= 1000; i++ {
			resp, err := s.QueryData(t, "db1", fmt.Sprintf("keyD%d", i), "alice")
			require.NoError(t, err)
			require.Nil(t, resp.GetResponse().GetValue())

			dataTx := &types.DataTx{
				MustSignUserIds: []string{
					"alice",
				},
				TxId: uuid.New().String(),
				DbOperations: []*types.DBOperation{
					{
						DbName: "db1",
						DataWrites: []*types.DataWrite{
							{
								Key:   fmt.Sprintf("keyD%d", i),
								Value: []byte(fmt.Sprintf("keyD%d-value1", i)),
							},
						},
					},
				},
			}

			txEnv := &types.DataTxEnvelope{
				Payload:    dataTx,
				Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
			}
			txEnvs = append(txEnvs, txEnv)
		}

		for _, txEnv := range txEnvs {
			err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
			require.NoError(t, err)
		}

		allIn := func() bool {
			for i := 1; i <= 1000; i++ {
				resp, err := s.QueryData(t, "db1", fmt.Sprintf("keyD%d", i), "alice")
				if err != nil {
					return false
				}
				if resp.GetResponse().GetValue() == nil {
					return false
				}
				require.Equal(t, []byte(fmt.Sprintf("keyD%d-value1", i)), resp.GetResponse().GetValue())
			}
			return true
		}

		require.Eventually(t, allIn, 30*time.Second, 100*time.Millisecond)
	})

	t.Run("invalid: mvcc conflict version 0 is not the same as nil", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key: "keyE",
							Version: &types.Version{
								BlockNum: 0,
								TxNum:    0,
							},
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "keyE",
							Value: []byte("keyE-value"),
						},
					},
				},
			},
		}
		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		}

		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)

		txId := txEnv.GetPayload().GetTxId()
		var resp *types.TxReceiptResponseEnvelope
		require.Eventually(t, func() bool {
			resp, err = s.QueryTxReceipt(t, txId, "alice")
			return err == nil && resp != nil
		}, 30*time.Second, 100*time.Millisecond)

		intTxId, _ := strconv.Atoi(txId)
		require.Equal(t, types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, resp.GetResponse().GetReceipt().GetHeader().GetValidationInfo()[intTxId].GetFlag())
	})

	t.Run("read-write-delete to a single database", func(t *testing.T) {
		resp, err := s.QueryData(t, "db1", "keyA1", "alice")
		require.NoError(t, err)
		verKey1 := resp.GetResponse().GetMetadata().GetVersion()
		require.NotNil(t, verKey1)

		resp, err = s.QueryData(t, "db1", "keyA2", "alice")
		require.NoError(t, err)
		verKey2 := resp.GetResponse().GetMetadata().GetVersion()
		require.NotNil(t, verKey2)

		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key:     "keyA1",
							Version: verKey1,
						},
						{
							Key:     "keyA2",
							Version: verKey2,
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "keyA1",
							Value: []byte("keyA1-value2"),
						},
					},
					DataDeletes: []*types.DataDelete{
						{
							Key: "keyA2",
						},
					},
				},
			},
		}

		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		}

		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			resp, err = s.QueryData(t, "db1", "keyA1", "alice")
			return err == nil && resp != nil && bytes.Compare([]byte("keyA1-value2"), resp.GetResponse().GetValue()) == 0
		}, 30*time.Second, 100*time.Millisecond)

		require.Eventually(t, func() bool {
			resp, err = s.QueryData(t, "db1", "keyA2", "alice")
			return err == nil && resp != nil && len(resp.GetResponse().GetValue()) == 0
		}, 30*time.Second, 100*time.Millisecond)
	})

	t.Run("blind write to multiple databases", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataWrites: []*types.DataWrite{
						{
							Key:   "keyF1",
							Value: []byte("keyF1-value1"),
							Acl: &types.AccessControl{
								ReadWriteUsers:     map[string]bool{"alice": true, "bob": true},
								SignPolicyForWrite: types.AccessControl_ALL,
							},
						},
					},
				},
				{
					DbName: "db2",
					DataWrites: []*types.DataWrite{
						{
							Key:   "keyF2",
							Value: []byte("keyF2-value1"),
							Acl: &types.AccessControl{
								ReadWriteUsers:     map[string]bool{"alice": true, "bob": true},
								SignPolicyForWrite: types.AccessControl_ALL,
							},
						},
					},
				},
			},
		}

		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		}

		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			resp, err := s.QueryData(t, "db1", "keyF1", "alice")
			return err == nil && resp != nil && bytes.Compare([]byte("keyF1-value1"), resp.GetResponse().GetValue()) == 0
		}, 30*time.Second, 100*time.Millisecond)

		require.Eventually(t, func() bool {
			resp, err := s.QueryData(t, "db2", "keyF2", "alice")
			return err == nil && resp != nil && bytes.Compare([]byte("keyF2-value1"), resp.GetResponse().GetValue()) == 0
		}, 30*time.Second, 100*time.Millisecond)

	})

	t.Run("read-write-delete to multiple databases and multi-users", func(t *testing.T) {
		resp, err := s.QueryData(t, "db1", "keyF1", "alice")
		require.NoError(t, err)
		verKeyF1 := resp.GetResponse().GetMetadata().GetVersion()
		require.NotNil(t, verKeyF1)

		resp, err = s.QueryData(t, "db2", "keyF2", "alice")
		require.NoError(t, err)
		verKeyF2 := resp.GetResponse().GetMetadata().GetVersion()
		require.NotNil(t, verKeyF2)

		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
				"bob",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key:     "keyF1",
							Version: verKeyF1,
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "keyF1",
							Value: []byte("keyF1-value2"),
							Acl: &types.AccessControl{
								ReadWriteUsers:     map[string]bool{"alice": true, "bob": true},
								SignPolicyForWrite: types.AccessControl_ALL,
							},
						},
						{
							Key:   "keyF3",
							Value: []byte("keyF3-constantValue"),
							Acl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"alice": true,
								},
							},
						},
					},
					DataDeletes: []*types.DataDelete{
						{
							Key: "keyA1",
						},
					},
				},
				{
					DbName: "db2",
					DataReads: []*types.DataRead{
						{
							Key:     "keyF2",
							Version: verKeyF2,
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "keyF2",
							Value: []byte("keyF2-value2"),
							Acl: &types.AccessControl{
								ReadWriteUsers:     map[string]bool{"alice": true, "bob": true},
								SignPolicyForWrite: types.AccessControl_ALL,
							},
						},
						{
							Key:   "keyF4",
							Value: []byte("keyF4-constantValue"),
							Acl: &types.AccessControl{
								ReadUsers: map[string]bool{
									"bob": true,
								},
							},
						},
					},
				},
			},
		}
		txEnv := &types.DataTxEnvelope{
			Payload: dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx),
				"bob": testutils.SignatureFromTx(t, bobSigner, dataTx)},
		}
		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			resp, err = s.QueryData(t, "db1", "keyF1", "bob")
			return err == nil && resp != nil && bytes.Compare([]byte("keyF1-value2"), resp.GetResponse().GetValue()) == 0
		}, 30*time.Second, 100*time.Millisecond)

		require.Eventually(t, func() bool {
			resp, err = s.QueryData(t, "db1", "keyF3", "alice")
			return err == nil && resp != nil && bytes.Compare([]byte("keyF3-constantValue"), resp.GetResponse().GetValue()) == 0
		}, 30*time.Second, 100*time.Millisecond)

		require.Eventually(t, func() bool {
			resp, err = s.QueryData(t, "db2", "keyF2", "alice")
			return err == nil && resp != nil && bytes.Compare([]byte("keyF2-value2"), resp.GetResponse().GetValue()) == 0
		}, 30*time.Second, 100*time.Millisecond)

		require.Eventually(t, func() bool {
			resp, err = s.QueryData(t, "db2", "keyF4", "bob")
			return err == nil && resp != nil && bytes.Compare([]byte("keyF4-constantValue"), resp.GetResponse().GetValue()) == 0
		}, 30*time.Second, 100*time.Millisecond)
	})

	t.Run("invalid: mvcc conflict due to nil version", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key: "keyF1",
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "keyF5",
							Value: []byte("keyF5-value1"),
						},
					},
				},
			},
		}

		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		}
		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)

		txId := txEnv.GetPayload().GetTxId()
		var resp *types.TxReceiptResponseEnvelope

		require.Eventually(t, func() bool {
			resp, err = s.QueryTxReceipt(t, txId, "alice")
			return err == nil && resp != nil
		}, 30*time.Second, 100*time.Millisecond)

		intTxId, _ := strconv.Atoi(txId)
		require.Equal(t, types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, resp.GetResponse().GetReceipt().GetHeader().GetValidationInfo()[intTxId].GetFlag())
	})

	t.Run("invalid: mvcc conflict due to lower version", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key: "keyF1",
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    10,
							},
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "keyF5",
							Value: []byte("keyF5-value1"),
						},
					},
				},
			},
		}

		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		}
		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)

		txId := txEnv.GetPayload().GetTxId()
		var resp *types.TxReceiptResponseEnvelope

		require.Eventually(t, func() bool {
			resp, err = s.QueryTxReceipt(t, txId, "alice")
			return err == nil && resp != nil
		}, 30*time.Second, 100*time.Millisecond)

		intTxId, _ := strconv.Atoi(txId)
		require.Equal(t, types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, resp.GetResponse().GetReceipt().GetHeader().GetValidationInfo()[intTxId].GetFlag())
	})

	t.Run("invalid: mvcc conflict due to higher version", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataReads: []*types.DataRead{
						{
							Key: "keyF1",
							Version: &types.Version{
								BlockNum: 1000,
								TxNum:    10,
							},
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "keyF5",
							Value: []byte("keyF5-value1"),
						},
					},
				},
			},
		}

		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		}
		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)

		txId := txEnv.GetPayload().GetTxId()
		var resp *types.TxReceiptResponseEnvelope

		require.Eventually(t, func() bool {
			resp, err = s.QueryTxReceipt(t, txId, "alice")
			return err == nil && resp != nil
		}, 30*time.Second, 100*time.Millisecond)

		intTxId, _ := strconv.Atoi(txId)
		require.Equal(t, types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE, resp.GetResponse().GetReceipt().GetHeader().GetValidationInfo()[intTxId].GetFlag())
	})

	t.Run("invalid: key being deleted does not exist error", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{
				"alice",
			},
			TxId: uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataDeletes: []*types.DataDelete{
						{
							Key: "keyG",
						},
					},
				},
			},
		}
		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		}
		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)

		txId := txEnv.GetPayload().GetTxId()
		var resp *types.TxReceiptResponseEnvelope

		require.Eventually(t, func() bool {
			resp, err = s.QueryTxReceipt(t, txId, "alice")
			return err == nil && resp != nil
		}, 30*time.Second, 100*time.Millisecond)

		intTxId, _ := strconv.Atoi(txId)
		require.Equal(t, types.Flag_INVALID_INCORRECT_ENTRIES, resp.GetResponse().GetReceipt().GetHeader().GetValidationInfo()[intTxId].GetFlag())
	})

	t.Run("invalid: key being updated is also deleted error", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{"alice"},
			TxId:            uuid.New().String(),
			DbOperations: []*types.DBOperation{{
				DbName: "db1",
				DataWrites: []*types.DataWrite{
					{
						Key:   "keyF1",
						Value: []byte("keyF1-value3"),
					},
				},
				DataDeletes: []*types.DataDelete{
					{
						Key: "keyF1",
					},
				},
			},
			},
		}

		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		}
		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)

		txId := txEnv.GetPayload().GetTxId()
		var resp *types.TxReceiptResponseEnvelope

		require.Eventually(t, func() bool {
			resp, err = s.QueryTxReceipt(t, txId, "alice")
			return err == nil && resp != nil
		}, 30*time.Second, 100*time.Millisecond)

		intTxId, _ := strconv.Atoi(txId)
		require.Equal(t, types.Flag_INVALID_INCORRECT_ENTRIES, resp.GetResponse().GetReceipt().GetHeader().GetValidationInfo()[intTxId].GetFlag())
	})

	t.Run("invalid: modifying read-only key would result in an error", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{"alice"},
			TxId:            uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db1",
					DataWrites: []*types.DataWrite{
						{
							Key:   "keyF3",
							Value: []byte("keyF3-constantValue2"),
						},
					},
				},
			},
		}

		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		}
		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)

		txId := txEnv.GetPayload().GetTxId()
		var resp *types.TxReceiptResponseEnvelope

		require.Eventually(t, func() bool {
			resp, err = s.QueryTxReceipt(t, txId, "alice")
			return err == nil && resp != nil
		}, 30*time.Second, 100*time.Millisecond)

		intTxId, _ := strconv.Atoi(txId)
		require.Equal(t, types.Flag_INVALID_NO_PERMISSION, resp.GetResponse().GetReceipt().GetHeader().GetValidationInfo()[intTxId].GetFlag())
	})

	t.Run("invalid: only users in read acl can read", func(t *testing.T) {
		resp, err := s.QueryData(t, "db1", "keyF3", "alice")
		require.NoError(t, err)
		require.Equal(t, []byte("keyF3-constantValue"), resp.GetResponse().GetValue())

		_, err = s.QueryData(t, "db1", "keyF3", "bob")
		require.EqualError(t, err, "error while processing 'GET /data/db1/keyF3' because the user [bob] has no permission to read key [keyF3] from database [db1]")
	})

	t.Run("invalid: inadequate signature error", func(t *testing.T) {
		dataTx := &types.DataTx{
			MustSignUserIds: []string{"alice"},
			TxId:            uuid.New().String(),
			DbOperations: []*types.DBOperation{
				{
					DbName: "db2",
					DataWrites: []*types.DataWrite{
						{
							Key:   "keyF2",
							Value: []byte("keyF2-value2"),
						},
					},
				},
			},
		}
		txEnv := &types.DataTxEnvelope{
			Payload:    dataTx,
			Signatures: map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx)},
		}
		err = s.SubmitTransactionAsync(t, constants.PostDataTx, txEnv)
		require.NoError(t, err)

		txId := txEnv.GetPayload().GetTxId()
		var resp *types.TxReceiptResponseEnvelope

		require.Eventually(t, func() bool {
			resp, err = s.QueryTxReceipt(t, txId, "alice")
			return err == nil && resp != nil
		}, 30*time.Second, 100*time.Millisecond)

		intTxId, _ := strconv.Atoi(txId)
		require.Equal(t, types.Flag_INVALID_NO_PERMISSION, resp.GetResponse().GetReceipt().GetHeader().GetValidationInfo()[intTxId].GetFlag())
	})

}
