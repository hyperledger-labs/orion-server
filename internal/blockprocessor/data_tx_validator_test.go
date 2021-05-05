// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package blockprocessor

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/identity"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestValidateDataTx(t *testing.T) {
	t.Parallel()

	alice := "alice"
	bob := "bob"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{alice, bob, "bogusUser"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, alice)
	bobCert, bobSigner := testutils.LoadTestClientCrypto(t, cryptoDir, bob)
	bogusCert, _ := testutils.LoadTestClientCrypto(t, cryptoDir, "bogusUser")

	addUserWithCorrectPrivilege := func(db worldstate.DB) {
		a := &types.User{
			ID:          alice,
			Certificate: aliceCert.Raw,
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					worldstate.DefaultDBName: types.Privilege_ReadWrite,
					"db1":                    types.Privilege_ReadWrite,
				},
			},
		}
		aliceSerialized, err := proto.Marshal(a)
		require.NoError(t, err)

		b := &types.User{
			ID:          bob,
			Certificate: bobCert.Raw,
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					worldstate.DefaultDBName: types.Privilege_ReadWrite,
					"db1":                    types.Privilege_ReadWrite,
				},
			},
		}
		bobSerialized, err := proto.Marshal(b)
		require.NoError(t, err)

		userAdd := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(identity.UserNamespace) + alice,
						Value: aliceSerialized,
					},
					{
						Key:   string(identity.UserNamespace) + bob,
						Value: bobSerialized,
					},
				},
			},
		}

		require.NoError(t, db.Commit(userAdd, 1))
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		txEnv          *types.DataTxEnvelope
		pendingOps     *pendingOperations
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: unallowed character in the first database name",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: "db1/name",
						DataWrites: []*types.DataWrite{
							{
								Key: "key1",
							},
						},
					},
					{
						DBName: "db2",
						DataWrites: []*types.DataWrite{
							{
								Key: "key1",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database name [db1/name] is not valid",
			},
		},
		{
			name: "invalid: database does not exist",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
					},
					{
						DBName: "db1",
						DataWrites: []*types.DataWrite{
							{
								Key: "key1",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
				ReasonIfInvalid: "the database [db1] does not exist in the cluster",
			},
		},
		{
			name: "invalid: system database name cannot be used in a transaction",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
					},
					{
						DBName: worldstate.ConfigDBName,
						DataWrites: []*types.DataWrite{
							{
								Key: "key1",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the database [" + worldstate.ConfigDBName + "] is a system database and no user can write to a system database via data transaction. Use appropriate transaction type to modify the system database",
			},
		},
		{
			name: "Invalid signature from must sign user",
			setup: func(db worldstate.DB) {
				user := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, alice, bogusCert.Raw, nil, nil, nil),
							constructUserForTest(t, bob, bobCert.Raw, nil, nil, nil),
						},
					},
				}

				require.NoError(t, db.Commit(user, 1))
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice, bob},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
					},
					{
						DBName: worldstate.ConfigDBName,
						DataWrites: []*types.DataWrite{
							{
								Key: "key1",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_UNAUTHORISED,
				ReasonIfInvalid: "signature of the must sign user [" + alice + "] is not valid (maybe the certifcate got changed)",
			},
		},
		{
			name: "Invalid signature from non-must sign user and bob does not have rw access on the db",
			setup: func(db worldstate.DB) {
				user := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, alice, bogusCert.Raw, nil, nil, nil),
							constructUserForTest(t, bob, bobCert.Raw, nil, nil, nil),
						},
					},
				}

				require.NoError(t, db.Commit(user, 1))
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
				MustSignUserIDs: []string{bob},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "none of the user in [bob] has read-write permission on the database [bdb]",
			},
		},
		{
			name: "Invalid signature from non-must sign user but the transaction would be marked valid",
			setup: func(db worldstate.DB) {
				user := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, alice, bogusCert.Raw, nil, nil, nil),
							constructUserForTest(t, bob, bobCert.Raw, &types.Privilege{
								DBPermission: map[string]types.Privilege_Access{
									worldstate.DefaultDBName: types.Privilege_ReadWrite,
								},
							}, nil, nil),
						},
					},
				}

				require.NoError(t, db.Commit(user, 1))
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
				MustSignUserIDs: []string{bob},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "invalid: duplicate database entry in operations",
			setup: func(db worldstate.DB) {
				user := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, alice, aliceCert.Raw, nil, nil, nil),
							constructUserForTest(t, bob, bobCert.Raw, nil, nil, nil),
						},
					},
				}

				require.NoError(t, db.Commit(user, 1))
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
				MustSignUserIDs: []string{bob},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataWrites: []*types.DataWrite{
							{
								Key: "key1",
							},
						},
					},
					{
						DBName: worldstate.DefaultDBName,
						DataWrites: []*types.DataWrite{
							{
								Key: "key2",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the database [" + worldstate.DefaultDBName + "] occurs more than once in the operations. The database present in the operations should be unique",
			},
		},
		{
			name: "invalid: user does not have rw permission on the db1",
			setup: func(db worldstate.DB) {
				user := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, alice, aliceCert.Raw, nil, nil, nil),
							constructUserForTest(t, bob, bobCert.Raw, nil, nil, nil),
						},
					},
				}

				require.NoError(t, db.Commit(user, 1))
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataWrites: []*types.DataWrite{
							{
								Key: "key1",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "none of the user in [alice, bob] has read-write permission on the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "invalid: incorrect fields in the data write",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataWrites: []*types.DataWrite{
							{
								Key: "key1",
								ACL: &types.AccessControl{
									ReadWriteUsers: map[string]bool{
										"user1": true,
									},
								},
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [user1] defined in the access control for the key [key1] does not exist",
			},
		},
		{
			name: "invalid: incorrect fields in the data delete",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataWrites: []*types.DataWrite{
							{
								Key: "key1",
							},
						},
						DataDeletes: []*types.DataDelete{
							{
								Key: "key2",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [key2] does not exist in the database and hence, it cannot be deleted",
			},
		},
		{
			name: "invalid: duplicate entry",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
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

				require.NoError(t, db.Commit(data, 1))
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataWrites: []*types.DataWrite{
							{
								Key: "key1",
							},
						},
						DataDeletes: []*types.DataDelete{
							{
								Key: "key1",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [key1] is being updated as well as deleted. Only one operation per key is allowed within a transaction",
			},
		},
		{
			name: "invalid: acl check on read fails",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataReads: []*types.DataRead{
							{
								Key: "key1",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "none of the user in [alice] has a read permission on key [key1] present in the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "invalid: acl check on write fails",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataWrites: []*types.DataWrite{
							{
								Key: "key1",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "none of the user in [alice] has a write/delete permission on key [key1] present in the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "invalid: acl check on delete fails",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataDeletes: []*types.DataDelete{
							{
								Key: "key1",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "none of the user in [alice] has a write/delete permission on key [key1] present in the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "invalid: mvccValidation fails",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataReads: []*types.DataRead{
							{
								Key: "key1",
							},
							{
								Key: "key2",
							},
						},
					},
				},
			}),
			pendingOps: &pendingOperations{
				pendingWrites: map[string]bool{
					constructCompositeKey(worldstate.DefaultDBName, "key1"): true,
					constructCompositeKey("db1", "key2"):                    true,
				},
				pendingDeletes: map[string]bool{},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "invalid: no user can directly write to a system database",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
					},
					{
						DBName: worldstate.UsersDBName,
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "the database [" + worldstate.UsersDBName + "] is a system database and no user can write to a system database via data transaction. Use appropriate transaction type to modify the system database",
			},
		},
		{
			name: "valid (has extra sign)",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
				db1 := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DatabasesDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "db1",
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
				require.NoError(t, db.Commit(db1, 1))

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"alice": true,
										},
									},
								},
							},
							{
								Key: "key2",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"alice": true,
										},
									},
								},
							},
						},
					},
					{
						DBName: "db1",
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key3",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"alice": true,
										},
									},
								},
							},
							{
								Key: "key4",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"alice": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataReads: []*types.DataRead{
							{
								Key: "key1",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    1,
								},
							},
						},
						DataWrites: []*types.DataWrite{
							{
								Key:   "key1",
								Value: []byte("new-val"),
							},
						},
						DataDeletes: []*types.DataDelete{
							{
								Key: "key2",
							},
						},
					},
					{
						DBName: "db1",
						DataReads: []*types.DataRead{
							{
								Key: "key3",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    1,
								},
							},
						},
						DataWrites: []*types.DataWrite{
							{
								Key:   "key3",
								Value: []byte("new-val"),
							},
						},
						DataDeletes: []*types.DataDelete{
							{
								Key: "key4",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid (mix of write policy)",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
				db1 := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DatabasesDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "db1",
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
				require.NoError(t, db.Commit(db1, 1))

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											alice: true,
											bob:   true,
										},
										SignPolicyForWrite: types.AccessControl_ALL,
									},
								},
							},
							{
								Key: "key2",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											alice: true,
											bob:   true,
										},
										SignPolicyForWrite: types.AccessControl_ANY,
									},
								},
							},
						},
					},
					{
						DBName: "db1",
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key3",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											alice: true,
											bob:   true,
										},
										SignPolicyForWrite: types.AccessControl_ALL,
									},
								},
							},
							{
								Key: "key4",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											alice: true,
											bob:   true,
										},
										SignPolicyForWrite: types.AccessControl_ANY,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataReads: []*types.DataRead{
							{
								Key: "key1",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    1,
								},
							},
						},
						DataWrites: []*types.DataWrite{
							{
								Key:   "key1",
								Value: []byte("new-val"),
							},
						},
						DataDeletes: []*types.DataDelete{
							{
								Key: "key2",
							},
						},
					},
					{
						DBName: "db1",
						DataReads: []*types.DataRead{
							{
								Key: "key3",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    1,
								},
							},
						},
						DataWrites: []*types.DataWrite{
							{
								Key:   "key3",
								Value: []byte("new-val"),
							},
						},
						DataDeletes: []*types.DataDelete{
							{
								Key: "key4",
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "invalid due to inadequate sign to match the acl)",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)

				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: &types.Version{
										BlockNum: 1,
										TxNum:    1,
									},
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											alice: true,
											bob:   true,
										},
										SignPolicyForWrite: types.AccessControl_ALL,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
						DataReads: []*types.DataRead{
							{
								Key: "key1",
								Version: &types.Version{
									BlockNum: 1,
									TxNum:    1,
								},
							},
						},
						DataWrites: []*types.DataWrite{
							{
								Key:   "key1",
								Value: []byte("new-val"),
							},
						},
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "not all required users in [alice,bob] have signed the transaction to write/delete key [key1] present in the database [bdb]",
			},
		},
		{
			name: "valid: no read/write/delete",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
			},
			txEnv: testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
				MustSignUserIDs: []string{alice},
				DBOperations: []*types.DBOperation{
					{
						DBName: worldstate.DefaultDBName,
					},
				},
			}),
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.validate(tt.txEnv, tt.pendingOps)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateFieldsInDataWrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		dataWrites     []*types.DataWrite
		expectedResult *types.ValidationInfo
	}{
		{
			name:  "invalid: an empty entry in the write",
			setup: func(db worldstate.DB) {},
			dataWrites: []*types.DataWrite{
				nil,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the write list",
			},
		},
		{
			name:  "invalid: user defined in the read acl does not exist",
			setup: func(db worldstate.DB) {},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
					ACL: &types.AccessControl{
						ReadUsers: map[string]bool{
							"user1": true,
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [user1] defined in the access control for the key [key1] does not exist",
			},
		},
		{
			name:  "invalid: user defined in the write acl does not exist",
			setup: func(db worldstate.DB) {},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
					ACL: &types.AccessControl{
						ReadWriteUsers: map[string]bool{
							"user1": true,
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the user [user1] defined in the access control for the key [key1] does not exist",
			},
		},
		{
			name: "valid",
			setup: func(db worldstate.DB) {
				newUsers := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, "user1", nil, nil, nil, nil),
							constructUserForTest(t, "user2", nil, nil, nil, nil),
						},
					},
				}

				require.NoError(t, db.Commit(newUsers, 1))
			},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
					ACL: &types.AccessControl{
						ReadUsers: map[string]bool{
							"user1": true,
						},
						ReadWriteUsers: map[string]bool{
							"user2": true,
						},
					},
				},
				{
					Key: "key2",
					ACL: &types.AccessControl{
						ReadUsers: map[string]bool{
							"user1": true,
						},
						ReadWriteUsers: map[string]bool{
							"user2": true,
						},
					},
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.validateFieldsInDataWrites(tt.dataWrites)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateFieldsInDataDeletes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		dataDeletes    []*types.DataDelete
		pendingOps     *pendingOperations
		expectedResult *types.ValidationInfo
	}{
		{
			name:  "invalid: an empty entry in the delete",
			setup: func(db worldstate.DB) {},
			dataDeletes: []*types.DataDelete{
				nil,
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "there is an empty entry in the delete list",
			},
		},
		{
			name:  "invalid: entry to be deleted is already deleted by some previous transaction",
			setup: func(db worldstate.DB) {},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			pendingOps: &pendingOperations{
				pendingWrites: map[string]bool{},
				pendingDeletes: map[string]bool{
					constructCompositeKey(worldstate.DefaultDBName, "key1"): true,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: "the key [key1] is already deleted by some previous transaction in the block",
			},
		},
		{
			name:  "invalid: entry to be deleted does not exist",
			setup: func(db worldstate.DB) {},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [key1] does not exist in the database and hence, it cannot be deleted",
			},
		},
		{
			name: "valid",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
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

				require.NoError(t, db.Commit(data, 1))
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()
			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.validateFieldsInDataDeletes(worldstate.DefaultDBName, tt.dataDeletes, tt.pendingOps)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateUniquenessInDataWritesAndDeletes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		dataWrites     []*types.DataWrite
		dataDeletes    []*types.DataDelete
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: duplicate entry in the writes",
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
				{
					Key: "key1",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [key1] is duplicated in the write list. The keys in the write list must be unique",
			},
		},
		{
			name: "invalid: duplicate entry in the deletes",
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
				{
					Key: "key1",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [key1] is duplicated in the delete list. The keys in the delete list must be unique",
			},
		},
		{
			name: "invalid: the same entry is present in both write and delete list",
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
				ReasonIfInvalid: "the key [key1] is being updated as well as deleted. Only one operation per key is allowed within a transaction",
			},
		},
		{
			name: "valid",
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key2",
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			result := validateUniquenessInDataWritesAndDeletes(tt.dataWrites, tt.dataDeletes)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateAClOnDataReads(t *testing.T) {
	sampleVersion := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		dataReads      []*types.DataRead
		operatingUser  []string
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: user does not have the permission",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataReads: []*types.DataRead{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"operatingUser", "anotherUser"},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "none of the user in [operatingUser,anotherUser] has a read permission on key [key1] present in the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "valid: acl check passes as the user has read permission",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadUsers: map[string]bool{
											"operatingUser": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataReads: []*types.DataRead{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"operatingUser", "anotherUser"},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: acl check passes as the user has read-write permission itself",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"operatingUser": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataReads: []*types.DataRead{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"anotherUser", "operatingUser"},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: no acl",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataReads: []*types.DataRead{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"operatingUser", "anotherUser"},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:          "valid: empty reads",
			setup:         func(db worldstate.DB) {},
			dataReads:     nil,
			operatingUser: []string{"operatingUser"},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.validateACLOnDataReads(tt.operatingUser, worldstate.DefaultDBName, tt.dataReads)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateAClOnDataWrites(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		dataWrites     []*types.DataWrite
		operatingUser  []string
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: user does not have the permission - ANY write policy",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
										},
										SignPolicyForWrite: types.AccessControl_ANY,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"operatingUser", "anotherUser"},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "none of the user in [operatingUser,anotherUser] has a write/delete permission on key [key1] present in the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "invalid: user does not have the permission - ALL write policy",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
											"user2": true,
											"user3": true,
										},
										SignPolicyForWrite: types.AccessControl_ALL,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"user1", "user2"},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "not all required users in [user1,user2,user3] have signed the transaction to write/delete key [key1] present in the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "invalid: no user has permission to modify read-only key",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"user1", "user2"},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "no user can write or delete the key [key1]",
			},
		},
		{
			name: "valid: acl check passes - ANY write policy",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"operatingUser": true,
											"user1":         true,
										},
										SignPolicyForWrite: types.AccessControl_ANY,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"anotherUser", "operatingUser"},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: acl check passes - ALL write policy",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"operatingUser": true,
											"user1":         true,
										},
										SignPolicyForWrite: types.AccessControl_ALL,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"anotherUser", "operatingUser", "user1"},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: no acl",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataWrites: []*types.DataWrite{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"operatingUser"},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:          "valid: empty writes",
			setup:         func(db worldstate.DB) {},
			dataWrites:    nil,
			operatingUser: []string{"anotherUser", "operatingUser"},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.validateACLOnDataWrites(tt.operatingUser, worldstate.DefaultDBName, tt.dataWrites)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestValidateAClOnDataDeletes(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		dataDeletes    []*types.DataDelete
		operatingUser  []string
		expectedResult *types.ValidationInfo
	}{
		{
			name: "invalid: user does not have the permission - ANY write policy",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
										},
										SignPolicyForWrite: types.AccessControl_ANY,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"operatingUser", "anotherUser"},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "none of the user in [operatingUser,anotherUser] has a write/delete permission on key [key1] present in the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "invalid: user does not have the permission - ALL write policy",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
											"user2": true,
											"user3": true,
										},
										SignPolicyForWrite: types.AccessControl_ALL,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"user1", "user2"},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "not all required users in [user1,user2,user3] have signed the transaction to write/delete key [key1] present in the database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name: "invalid: no user has permission to modify read-only key",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"user1", "user2"},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_NO_PERMISSION,
				ReasonIfInvalid: "no user can write or delete the key [key1]",
			},
		},
		{
			name: "valid: acl check passes - ANY write policy",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"operatingUser": true,
											"user1":         true,
										},
										SignPolicyForWrite: types.AccessControl_ANY,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"anotherUser", "operatingUser"},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: acl check passes - ALL write policy",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"operatingUser": true,
											"user1":         true,
										},
										SignPolicyForWrite: types.AccessControl_ANY,
									},
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"anotherUser", "operatingUser", "user1"},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name: "valid: no acl",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: sampleVersion,
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataDeletes: []*types.DataDelete{
				{
					Key: "key1",
				},
			},
			operatingUser: []string{"operatingUser"},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
		{
			name:          "valid: empty delete",
			setup:         func(db worldstate.DB) {},
			dataDeletes:   nil,
			operatingUser: []string{"operatingUser"},
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.validateACLOnDataDeletes(tt.operatingUser, worldstate.DefaultDBName, tt.dataDeletes)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestMVCCOnDataTx(t *testing.T) {
	t.Parallel()

	version1 := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}

	version2 := &types.Version{
		BlockNum: 2,
		TxNum:    1,
	}

	version3 := &types.Version{
		BlockNum: 3,
		TxNum:    1,
	}

	tests := []struct {
		name           string
		setup          func(db worldstate.DB)
		dataReads      []*types.DataRead
		pendingOps     *pendingOperations
		expectedResult *types.ValidationInfo
	}{
		{
			name:  "invalid: conflict writes within the block",
			setup: func(db worldstate.DB) {},
			dataReads: []*types.DataRead{
				{
					Key:     "key1",
					Version: version1,
				},
			},
			pendingOps: &pendingOperations{
				pendingWrites: map[string]bool{
					constructCompositeKey(worldstate.DefaultDBName, "key1"): true,
				},
				pendingDeletes: map[string]bool{},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name:  "invalid: conflict deletes within the block",
			setup: func(db worldstate.DB) {},
			dataReads: []*types.DataRead{
				{
					Key:     "key1",
					Version: version1,
				},
			},
			pendingOps: &pendingOperations{
				pendingWrites: map[string]bool{},
				pendingDeletes: map[string]bool{
					constructCompositeKey(worldstate.DefaultDBName, "key1"): true,
				},
			},
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITHIN_BLOCK,
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [" + worldstate.DefaultDBName + "]",
			},
		},
		{
			name:  "invalid: committed version does not exist",
			setup: func(db worldstate.DB) {},
			dataReads: []*types.DataRead{
				{
					Key:     "key1",
					Version: version1,
				},
			},
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the key [key1] in database [" + worldstate.DefaultDBName + "] changed",
			},
		},
		{
			name: "invalid: committed version does not match the read version",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: version2,
								},
							},
							{
								Key: "key2",
								Metadata: &types.Metadata{
									Version: version3,
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataReads: []*types.DataRead{
				{
					Key:     "key1",
					Version: version2,
				},
				{
					Key:     "key2",
					Version: version1,
				},
			},
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag:            types.Flag_INVALID_MVCC_CONFLICT_WITH_COMMITTED_STATE,
				ReasonIfInvalid: "mvcc conflict has occurred as the committed state for the key [key2] in database [" + worldstate.DefaultDBName + "] changed",
			},
		},
		{
			name: "valid",
			setup: func(db worldstate.DB) {
				data := []*worldstate.DBUpdates{
					{
						DBName: worldstate.DefaultDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key1",
								Metadata: &types.Metadata{
									Version: version2,
								},
							},
							{
								Key: "key2",
								Metadata: &types.Metadata{
									Version: version3,
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(data, 1))
			},
			dataReads: []*types.DataRead{
				{
					Key:     "key1",
					Version: version2,
				},
				{
					Key:     "key2",
					Version: version3,
				},
				{
					Key:     "key3",
					Version: nil,
				},
			},
			pendingOps: newPendingOperations(),
			expectedResult: &types.ValidationInfo{
				Flag: types.Flag_VALID,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newValidatorTestEnv(t)
			defer env.cleanup()

			tt.setup(env.db)

			result, err := env.validator.dataTxValidator.mvccValidation(worldstate.DefaultDBName, tt.dataReads, tt.pendingOps)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}
