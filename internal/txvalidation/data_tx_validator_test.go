// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package txvalidation

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestValidateDataTx(t *testing.T) {
	t.Parallel()

	alice := "alice"
	bob := "bob"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{alice, bob, "bogusUser"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, alice)
	bobCert, bobSigner := testutils.LoadTestCrypto(t, cryptoDir, bob)
	bogusCert, _ := testutils.LoadTestCrypto(t, cryptoDir, "bogusUser")

	addUserWithCorrectPrivilege := func(db worldstate.DB) {
		a := &types.User{
			Id:          alice,
			Certificate: aliceCert.Raw,
			Privilege: &types.Privilege{
				DbPermission: map[string]types.Privilege_Access{
					worldstate.DefaultDBName: types.Privilege_ReadWrite,
					"db1":                    types.Privilege_ReadWrite,
				},
			},
		}
		aliceSerialized, err := proto.Marshal(a)
		require.NoError(t, err)

		b := &types.User{
			Id:          bob,
			Certificate: bobCert.Raw,
			Privilege: &types.Privilege{
				DbPermission: map[string]types.Privilege_Access{
					worldstate.DefaultDBName: types.Privilege_ReadWrite,
					"db1":                    types.Privilege_ReadWrite,
				},
			},
		}
		bobSerialized, err := proto.Marshal(b)
		require.NoError(t, err)

		userAdd := map[string]*worldstate.DBUpdates{
			worldstate.UsersDBName: {
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
		txsEnv         []*types.DataTxEnvelope
		expectedResult []*types.ValidationInfo
	}{
		{
			name: "invalid: unallowed character in the first database name",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "key7",
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
			txsEnv: []*types.DataTxEnvelope{
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: "db1/name",
							DataWrites: []*types.DataWrite{
								{
									Key: "key1",
								},
							},
						},
						{
							DbName: "db2",
							DataWrites: []*types.DataWrite{
								{
									Key: "key1",
								},
							},
						},
					},
				}),
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
						},
						{
							DbName: "db1",
							DataWrites: []*types.DataWrite{
								{
									Key: "key1",
								},
							},
						},
					},
				}),
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
						},
						{
							DbName: worldstate.ConfigDBName,
							DataWrites: []*types.DataWrite{
								{
									Key: "key1",
								},
							},
						},
					},
				}),
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
							DataWrites: []*types.DataWrite{
								{
									Key: "key1",
									Acl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
						},
					},
				}),
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
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
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
						},
						{
							DbName: worldstate.UsersDBName,
						},
					},
				}),
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
						},
					},
				}),
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
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
			},
			expectedResult: []*types.ValidationInfo{
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the database name [db1/name] is not valid",
				},
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_DATABASE_DOES_NOT_EXIST,
					ReasonIfInvalid: "the database [db1] does not exist in the cluster",
				},
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_NO_PERMISSION,
					ReasonIfInvalid: "the database [" + worldstate.ConfigDBName + "] is a system database and no user can write to a system database via data transaction. Use appropriate transaction type to modify the system database",
				},
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the user [user1] defined in the access control for the key [key1] does not exist",
				},
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the key [key2] does not exist in the database and hence, it cannot be deleted",
				},
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_NO_PERMISSION,
					ReasonIfInvalid: "the database [" + worldstate.UsersDBName + "] is a system database and no user can write to a system database via data transaction. Use appropriate transaction type to modify the system database",
				},
				&types.ValidationInfo{
					Flag: types.Flag_VALID,
				},
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_NO_PERMISSION,
					ReasonIfInvalid: "not all required users in [alice,bob] have signed the transaction to write/delete key [key7] present in the database [bdb]",
				},
			},
		},
		{
			name: "Invalid signature from must sign user",
			setup: func(db worldstate.DB) {
				user := map[string]*worldstate.DBUpdates{
					worldstate.UsersDBName: {
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, alice, bogusCert.Raw, nil, nil, nil),
							constructUserForTest(t, bob, bobCert.Raw, nil, nil, nil),
						},
					},
				}

				require.NoError(t, db.Commit(user, 1))
			},
			txsEnv: []*types.DataTxEnvelope{
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
					MustSignUserIds: []string{alice, bob},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
						},
						{
							DbName: worldstate.ConfigDBName,
							DataWrites: []*types.DataWrite{
								{
									Key: "key1",
								},
							},
						},
					},
				}),
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
					MustSignUserIds: []string{bob},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
						},
					},
				}),
			},
			expectedResult: []*types.ValidationInfo{
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_UNAUTHORISED,
					ReasonIfInvalid: "signature of the must sign user [" + alice + "] is not valid (maybe the certificate got changed)",
				},
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_NO_PERMISSION,
					ReasonIfInvalid: "none of the user in [bob] has read-write permission on the database [bdb]",
				},
			},
		},
		{
			name: "Invalid signature from non-must sign user but the transaction would be marked valid",
			setup: func(db worldstate.DB) {
				user := map[string]*worldstate.DBUpdates{
					worldstate.UsersDBName: {
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, alice, bogusCert.Raw, nil, nil, nil),
							constructUserForTest(t, bob, bobCert.Raw, &types.Privilege{
								DbPermission: map[string]types.Privilege_Access{
									worldstate.DefaultDBName: types.Privilege_ReadWrite,
								},
							}, nil, nil),
						},
					},
				}

				require.NoError(t, db.Commit(user, 1))
			},
			txsEnv: []*types.DataTxEnvelope{
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
					MustSignUserIds: []string{bob},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
						},
					},
				}),
			},
			expectedResult: []*types.ValidationInfo{
				&types.ValidationInfo{
					Flag: types.Flag_VALID,
				},
			},
		},
		{
			name: "invalid: duplicate database entry in operations",
			setup: func(db worldstate.DB) {
				user := map[string]*worldstate.DBUpdates{
					worldstate.UsersDBName: {
						Writes: []*worldstate.KVWithMetadata{
							constructUserForTest(t, alice, aliceCert.Raw, nil, nil, nil),
							constructUserForTest(t, bob, bobCert.Raw, nil, nil, nil),
						},
					},
				}

				require.NoError(t, db.Commit(user, 1))
			},
			txsEnv: []*types.DataTxEnvelope{
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
					MustSignUserIds: []string{bob},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
							DataWrites: []*types.DataWrite{
								{
									Key: "key1",
								},
							},
						},
						{
							DbName: worldstate.DefaultDBName,
							DataWrites: []*types.DataWrite{
								{
									Key: "key2",
								},
							},
						},
					},
				}),
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
							DataWrites: []*types.DataWrite{
								{
									Key: "key1",
								},
							},
						},
					},
				}),
			},
			expectedResult: []*types.ValidationInfo{
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the database [" + worldstate.DefaultDBName + "] occurs more than once in the operations. The database present in the operations should be unique",
				},
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_NO_PERMISSION,
					ReasonIfInvalid: "none of the user in [alice, bob] has read-write permission on the database [" + worldstate.DefaultDBName + "]",
				},
			},
		},
		{
			name: "invalid: duplicate entry",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)

				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
							{
								Key: "key2",
								Metadata: &types.Metadata{
									AccessControl: &types.AccessControl{
										ReadWriteUsers: map[string]bool{
											"user1": true,
										},
									},
								},
							},
							{
								Key: "key3",
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
			txsEnv: []*types.DataTxEnvelope{
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
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
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
							DataReads: []*types.DataRead{
								{
									Key: "key2",
								},
							},
						},
					},
				}),
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
							DataWrites: []*types.DataWrite{
								{
									Key: "key3",
								},
							},
						},
					},
				}),
				testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner}, &types.DataTx{
					MustSignUserIds: []string{alice},
					DbOperations: []*types.DBOperation{
						{
							DbName: worldstate.DefaultDBName,
							DataDeletes: []*types.DataDelete{
								{
									Key: "key3",
								},
							},
						},
					},
				}),
			},
			expectedResult: []*types.ValidationInfo{
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_INCORRECT_ENTRIES,
					ReasonIfInvalid: "the key [key1] is being updated as well as deleted. Only one operation per key is allowed within a transaction",
				},
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_NO_PERMISSION,
					ReasonIfInvalid: "none of the user in [alice] has a read permission on key [key2] present in the database [" + worldstate.DefaultDBName + "]",
				},
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_NO_PERMISSION,
					ReasonIfInvalid: "none of the user in [alice] has a write/delete permission on key [key3] present in the database [" + worldstate.DefaultDBName + "]",
				},
				&types.ValidationInfo{
					Flag:            types.Flag_INVALID_NO_PERMISSION,
					ReasonIfInvalid: "none of the user in [alice] has a write/delete permission on key [key3] present in the database [" + worldstate.DefaultDBName + "]",
				},
			},
		},
		{
			name: "valid (has extra sign)",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
				db1 := map[string]*worldstate.DBUpdates{
					worldstate.DatabasesDBName: {
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

				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
					"db1": {
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
			txsEnv: []*types.DataTxEnvelope{testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
				MustSignUserIds: []string{alice},
				DbOperations: []*types.DBOperation{
					{
						DbName: worldstate.DefaultDBName,
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
						DbName: "db1",
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
			},
			expectedResult: []*types.ValidationInfo{
				&types.ValidationInfo{
					Flag: types.Flag_VALID,
				},
			},
		},
		{
			name: "valid (mix of write policy)",
			setup: func(db worldstate.DB) {
				addUserWithCorrectPrivilege(db)
				db1 := map[string]*worldstate.DBUpdates{
					worldstate.DatabasesDBName: {
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

				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
					"db1": {
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
			txsEnv: []*types.DataTxEnvelope{testutils.SignedDataTxEnvelope(t, []crypto.Signer{aliceSigner, bobSigner}, &types.DataTx{
				MustSignUserIds: []string{alice},
				DbOperations: []*types.DBOperation{
					{
						DbName: worldstate.DefaultDBName,
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
						DbName: "db1",
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
			},
			expectedResult: []*types.ValidationInfo{
				&types.ValidationInfo{
					Flag: types.Flag_VALID,
				},
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

			var usersWithValidSignTx [][]string
			results := make([]*types.ValidationInfo, len(tt.txsEnv))

			for txNum, txEnv := range tt.txsEnv {
				users, valInfo, err := env.validator.dataTxValidator.validateSignatures(txEnv)
				require.NoError(t, err)
				results[txNum] = valInfo
				usersWithValidSignTx = append(usersWithValidSignTx, users)
			}

			err := env.validator.dataTxValidator.parallelValidation(tt.txsEnv, usersWithValidSignTx, results)
			require.NoError(t, err)
			for i, expected := range results {
				require.True(t, proto.Equal(expected, results[i]))
			}
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
					Acl: &types.AccessControl{
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
					Acl: &types.AccessControl{
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
				newUsers := map[string]*worldstate.DBUpdates{
					worldstate.UsersDBName: {
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
					Acl: &types.AccessControl{
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
					Acl: &types.AccessControl{
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
		txOps          *types.DBOperation
		dataReads      []*types.DataRead
		dataWrites     []*types.DataWrite
		dataDeletes    []*types.DataDelete
		pendingOps     *pendingOperations
		expectedResult *types.ValidationInfo
	}{
		{
			name:  "invalid: conflict writes within the block",
			setup: func(db worldstate.DB) {},
			txOps: &types.DBOperation{
				DataReads: []*types.DataRead{
					{
						Key:     "key1",
						Version: version1,
					},
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
			txOps: &types.DBOperation{
				DataReads: []*types.DataRead{
					{
						Key:     "key1",
						Version: version1,
					},
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
			name:  "invalid: more than one modification per key - conflict between write and delete",
			setup: func(db worldstate.DB) {},
			txOps: &types.DBOperation{
				DataWrites: []*types.DataWrite{
					{
						Key:   "key1",
						Value: []byte("value1"),
					},
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
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [" + worldstate.DefaultDBName + "]. Within a block, a key can be modified only once",
			},
		},
		{
			name:  "invalid: more than one modification per key - conflict between write and write",
			setup: func(db worldstate.DB) {},
			txOps: &types.DBOperation{
				DataWrites: []*types.DataWrite{
					{
						Key:   "key1",
						Value: []byte("value1"),
					},
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
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [" + worldstate.DefaultDBName + "]. Within a block, a key can be modified only once",
			},
		},
		{
			name:  "invalid: more than one modification per key - conflict between delete and write",
			setup: func(db worldstate.DB) {},
			txOps: &types.DBOperation{
				DataDeletes: []*types.DataDelete{
					{
						Key: "key1",
					},
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
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [" + worldstate.DefaultDBName + "]. Within a block, a key can be modified only once",
			},
		},
		{
			name:  "invalid: more than one modification per key - conflict between delete and delete",
			setup: func(db worldstate.DB) {},
			txOps: &types.DBOperation{
				DataDeletes: []*types.DataDelete{
					{
						Key: "key1",
					},
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
				ReasonIfInvalid: "mvcc conflict has occurred within the block for the key [key1] in database [" + worldstate.DefaultDBName + "]. Within a block, a key can be modified only once",
			},
		},
		{
			name:  "invalid: committed version does not exist",
			setup: func(db worldstate.DB) {},
			txOps: &types.DBOperation{
				DataReads: []*types.DataRead{
					{
						Key:     "key1",
						Version: version1,
					},
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
			txOps: &types.DBOperation{
				DataReads: []*types.DataRead{
					{
						Key:     "key1",
						Version: version2,
					},
					{
						Key:     "key2",
						Version: version1,
					},
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
				data := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
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
			txOps: &types.DBOperation{
				DataReads: []*types.DataRead{
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

			result, err := env.validator.dataTxValidator.mvccValidation(worldstate.DefaultDBName, tt.txOps, tt.pendingOps)
			require.NoError(t, err)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}
