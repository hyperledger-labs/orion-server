// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package datatxtest

import (
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
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
			Payload:              dataTx,
			Signatures:           map[string][]byte{"alice": testutils.SignatureFromTx(t, aliceSigner, dataTx), "bob": testutils.SignatureFromTx(t, bobSigner, dataTx)},
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     []byte{},
			XXX_sizecache:        0,
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
