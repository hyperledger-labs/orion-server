package identity

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/provenance"
	"github.ibm.com/blockchaindb/server/internal/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestConstructDBEntriesForUserAdminTx(t *testing.T) {
	t.Parallel()

	sampleUser := func(userID string) *types.User {
		return &types.User{
			ID:          userID,
			Certificate: []byte("certificate-" + userID),
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					worldstate.DefaultDBName: types.Privilege_ReadWrite,
				},
				Admin: false,
			},
		}
	}

	sampleUserSerialized := func(t *testing.T, userID string) []byte {
		user := sampleUser(userID)

		userSerialized, err := proto.Marshal(user)
		require.NoError(t, err)

		return userSerialized
	}

	tests := []struct {
		name              string
		transaction       *types.UserAdministrationTx
		version           *types.Version
		expectedDBUpdates *worldstate.DBUpdates
	}{
		{
			name: "only writes",
			transaction: &types.UserAdministrationTx{
				UserWrites: []*types.UserWrite{
					{
						User: sampleUser("user1"),
					},
					{
						User: sampleUser("user2"),
					},
				},
			},
			version: &types.Version{
				BlockNum: 1,
				TxNum:    5,
			},
			expectedDBUpdates: &worldstate.DBUpdates{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(UserNamespace) + "user1",
						Value: sampleUserSerialized(t, "user1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    5,
							},
						},
					},
					{
						Key:   string(UserNamespace) + "user2",
						Value: sampleUserSerialized(t, "user2"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    5,
							},
						},
					},
				},
				Deletes: nil,
			},
		},
		{
			name: "only deletes",
			transaction: &types.UserAdministrationTx{
				UserDeletes: []*types.UserDelete{
					{
						UserID: "user3",
					},
					{
						UserID: "user4",
					},
				},
			},
			version: nil,
			expectedDBUpdates: &worldstate.DBUpdates{
				DBName: worldstate.UsersDBName,
				Writes: nil,
				Deletes: []string{
					string(UserNamespace) + "user3",
					string(UserNamespace) + "user4",
				},
			},
		},
		{
			name: "both writes and deletes",
			transaction: &types.UserAdministrationTx{
				UserWrites: []*types.UserWrite{
					{
						User: sampleUser("user1"),
					},
					{
						User: sampleUser("user2"),
					},
				},
				UserDeletes: []*types.UserDelete{
					{
						UserID: "user3",
					},
					{
						UserID: "user4",
					},
				},
			},
			version: &types.Version{
				BlockNum: 2,
				TxNum:    2,
			},
			expectedDBUpdates: &worldstate.DBUpdates{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(UserNamespace) + "user1",
						Value: sampleUserSerialized(t, "user1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    2,
							},
						},
					},
					{
						Key:   string(UserNamespace) + "user2",
						Value: sampleUserSerialized(t, "user2"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    2,
							},
						},
					},
				},
				Deletes: []string{
					string(UserNamespace) + "user3",
					string(UserNamespace) + "user4",
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dbUpdates, err := ConstructDBEntriesForUserAdminTx(tt.transaction, tt.version)
			require.NoError(t, err)
			require.Equal(t, tt.expectedDBUpdates, dbUpdates)
		})
	}
}

func TestConstructDBEntriesForClusterAdmins(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 2,
		TxNum:    2,
	}

	sampleAdmin := func(adminID string, cert []byte) []byte {
		user := &types.User{
			ID:          adminID,
			Certificate: cert,
			Privilege: &types.Privilege{
				Admin: true,
			},
		}

		u, err := proto.Marshal(user)
		require.NoError(t, err)
		return u
	}

	var tests = []struct {
		name                      string
		adminsInCommittedConfigTx []*types.Admin
		adminsInNewConfigTx       []*types.Admin
		version                   *types.Version
		expectedUpdates           *worldstate.DBUpdates
	}{
		{
			name: "same set of admins, no changes",
			adminsInCommittedConfigTx: []*types.Admin{
				{
					ID:          "admin1",
					Certificate: []byte("certificate 1"),
				},
				{
					ID:          "admin2",
					Certificate: []byte("certificate 2"),
				},
			},
			adminsInNewConfigTx: []*types.Admin{
				{
					ID:          "admin1",
					Certificate: []byte("certificate 1"),
				},
				{
					ID:          "admin2",
					Certificate: []byte("certificate 2"),
				},
			},
			version:         sampleVersion,
			expectedUpdates: nil,
		},
		{
			name: "add, update, and delete admins",
			adminsInCommittedConfigTx: []*types.Admin{
				{
					ID:          "admin1",
					Certificate: []byte("certificate 1"),
				},
				{
					ID:          "admin2",
					Certificate: []byte("certificate 2"),
				},
				{
					ID:          "admin3",
					Certificate: []byte("certificate 3"),
				},
			},
			adminsInNewConfigTx: []*types.Admin{
				{
					ID:          "admin3",
					Certificate: []byte("new certificate 3"),
				},
				{
					ID:          "admin4",
					Certificate: []byte("certificate 4"),
				},
				{
					ID:          "admin5",
					Certificate: []byte("certificate 5"),
				},
			},
			version: sampleVersion,
			expectedUpdates: &worldstate.DBUpdates{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(UserNamespace) + "admin3",
						Value: sampleAdmin("admin3", []byte("new certificate 3")),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    1,
							},
						},
					},
					{
						Key:   string(UserNamespace) + "admin4",
						Value: sampleAdmin("admin4", []byte("certificate 4")),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    1,
							},
						},
					},
					{
						Key:   string(UserNamespace) + "admin5",
						Value: sampleAdmin("admin5", []byte("certificate 5")),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    1,
							},
						},
					},
				},
				Deletes: []string{string(UserNamespace) + "admin1", string(UserNamespace) + "admin2"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			updates, err := ConstructDBEntriesForClusterAdmins(tt.adminsInCommittedConfigTx, tt.adminsInNewConfigTx, &types.Version{
				BlockNum: 1,
				TxNum:    1,
			})
			require.NoError(t, err)
			if updates == nil {
				require.Equal(t, tt.expectedUpdates, updates)
				return
			}

			require.Equal(t, tt.expectedUpdates.DBName, updates.DBName)
			require.Equal(t, tt.expectedUpdates.Deletes, updates.Deletes)

			expectedWrites := make(map[string]*worldstate.KVWithMetadata)
			for _, w := range tt.expectedUpdates.Writes {
				expectedWrites[w.Key] = w
			}

			actualWrites := make(map[string]*worldstate.KVWithMetadata)
			for _, w := range updates.Writes {
				actualWrites[w.Key] = w
			}

			require.Len(t, actualWrites, len(expectedWrites))
			for key, expected := range expectedWrites {
				actual := actualWrites[key]
				require.Equal(t, expected.Value, actual.Value)
				require.True(t, proto.Equal(expected.Metadata, actual.Metadata))
			}
		})
	}
}

func TestConstructProvenanceEntriesForUserAdminTx(t *testing.T) {
	t.Parallel()

	version := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}
	user1 := &types.User{
		ID:          "user1",
		Certificate: []byte("rawcert"),
		Privilege: &types.Privilege{
			DBPermission: map[string]types.Privilege_Access{
				"db1": types.Privilege_Read,
			},
		},
	}
	user1Serialized, err := proto.Marshal(user1)
	require.NoError(t, err)

	user2 := &types.User{
		ID:          "user2",
		Certificate: []byte("rawcert"),
		Privilege: &types.Privilege{
			DBPermission: map[string]types.Privilege_Access{
				"db2": types.Privilege_ReadWrite,
			},
		},
	}
	user2Serialized, err := proto.Marshal(user2)
	require.NoError(t, err)

	user2New := &types.User{
		ID:          "user2",
		Certificate: []byte("rawcertNew"),
		Privilege: &types.Privilege{
			DBPermission: map[string]types.Privilege_Access{
				"db2": types.Privilege_ReadWrite,
			},
		},
	}
	user2NewSerialized, err := proto.Marshal(user2New)
	require.NoError(t, err)

	acl := &types.AccessControl{
		ReadUsers: map[string]bool{
			"user10": true,
		},
		ReadWriteUsers: map[string]bool{
			"user11": true,
		},
	}

	tests := []struct {
		name     string
		setup    func(db worldstate.DB)
		tx       *types.UserAdministrationTx
		expected *provenance.TxDataForProvenance
	}{
		{
			name: "writes new users",
			setup: func(db worldstate.DB) {
			},
			tx: &types.UserAdministrationTx{
				UserID:    "admin",
				TxID:      "tx1",
				UserReads: nil,
				UserWrites: []*types.UserWrite{
					{
						User: user1,
						ACL:  acl,
					},
					{
						User: user2,
						ACL:  nil,
					},
				},
				UserDeletes: nil,
			},
			expected: &provenance.TxDataForProvenance{
				IsValid: true,
				DBName:  worldstate.UsersDBName,
				UserID:  "admin",
				TxID:    "tx1",
				Reads:   nil,
				Writes: []*types.KVWithMetadata{
					{
						Key:   "user1",
						Value: user1Serialized,
						Metadata: &types.Metadata{
							Version:       version,
							AccessControl: acl,
						},
					},
					{
						Key:   "user2",
						Value: user2Serialized,
						Metadata: &types.Metadata{
							Version: version,
						},
					},
				},
				Deletes:            make(map[string]*types.Version),
				OldVersionOfWrites: make(map[string]*types.Version),
			},
		},
		{
			name: "delete existing users",
			setup: func(db worldstate.DB) {
				dbUpdates := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(UserNamespace) + "user1",
								Value: user1Serialized,
								Metadata: &types.Metadata{
									Version: version,
								},
							},
							{
								Key:   string(UserNamespace) + "user2",
								Value: user2Serialized,
								Metadata: &types.Metadata{
									Version: version,
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(dbUpdates, 1))
			},
			tx: &types.UserAdministrationTx{
				UserID:    "admin",
				TxID:      "tx1",
				UserReads: nil,
				UserDeletes: []*types.UserDelete{
					{
						UserID: "user1",
					},
					{
						UserID: "user2",
					},
				},
			},
			expected: &provenance.TxDataForProvenance{
				IsValid: true,
				DBName:  worldstate.UsersDBName,
				UserID:  "admin",
				TxID:    "tx1",
				Reads:   nil,
				Deletes: map[string]*types.Version{
					"user1": version,
					"user2": version,
				},
				OldVersionOfWrites: make(map[string]*types.Version),
			},
		},
		{
			name: "read users and write existing users",
			setup: func(db worldstate.DB) {
				dbUpdates := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   string(UserNamespace) + "user2",
								Value: user2NewSerialized,
								Metadata: &types.Metadata{
									Version: version,
								},
							},
						},
					},
				}

				require.NoError(t, db.Commit(dbUpdates, 1))
			},
			tx: &types.UserAdministrationTx{
				UserID: "admin",
				TxID:   "tx1",
				UserReads: []*types.UserRead{
					{
						UserID:  "user1",
						Version: version,
					},
				},
				UserWrites: []*types.UserWrite{
					{
						User: user2New,
					},
				},
			},
			expected: &provenance.TxDataForProvenance{
				IsValid: true,
				DBName:  worldstate.UsersDBName,
				UserID:  "admin",
				TxID:    "tx1",
				Reads: []*provenance.KeyWithVersion{
					{
						Key:     "user1",
						Version: version,
					},
				},
				Writes: []*types.KVWithMetadata{
					{
						Key:   "user2",
						Value: user2NewSerialized,
						Metadata: &types.Metadata{
							Version: version,
						},
					},
				},
				OldVersionOfWrites: map[string]*types.Version{
					"user2": version,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTestEnv(t)
			defer env.cleanup()

			tt.setup(env.db)

			provenanceData, err := ConstructProvenanceEntriesForUserAdminTx(tt.tx, version, env.db)
			require.NoError(t, err)

			require.Len(t, provenanceData.Deletes, len(tt.expected.Deletes))
			for expectedUser, expectedVersion := range tt.expected.Deletes {
				ver := provenanceData.Deletes[expectedUser]
				require.True(t, proto.Equal(expectedVersion, ver))
			}
			tt.expected.Deletes = nil
			provenanceData.Deletes = nil

			require.Len(t, provenanceData.OldVersionOfWrites, len(tt.expected.OldVersionOfWrites))
			for expectedUser, expectedVersion := range tt.expected.OldVersionOfWrites {
				ver := provenanceData.OldVersionOfWrites[expectedUser]
				require.True(t, proto.Equal(expectedVersion, ver))
			}
			tt.expected.OldVersionOfWrites = nil
			provenanceData.OldVersionOfWrites = nil
			require.Equal(t, tt.expected, provenanceData)
		})
	}
}

func TestConstructProvenanceEntriesForClusterAdmins(t *testing.T) {
	t.Parallel()

	version := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}
	admin1 := &types.User{
		ID:          "admin1",
		Certificate: []byte("rawcert-admin1"),
		Privilege: &types.Privilege{
			Admin: true,
		},
	}
	admin1Serialized, err := proto.Marshal(admin1)
	require.NoError(t, err)

	admin2 := &types.User{
		ID:          "admin2",
		Certificate: []byte("rawcert-admin2"),
		Privilege: &types.Privilege{
			Admin: true,
		},
	}
	admin2Serialized, err := proto.Marshal(admin2)
	require.NoError(t, err)

	admin2New := &types.User{
		ID:          "admin2",
		Certificate: []byte("rawcertNew-admin2"),
		Privilege: &types.Privilege{
			Admin: true,
		},
	}
	admin2NewSerialized, err := proto.Marshal(admin2New)
	require.NoError(t, err)

	tests := []struct {
		name         string
		setup        func(db worldstate.DB)
		userID       string
		txID         string
		adminUpdates *worldstate.DBUpdates
		expected     *provenance.TxDataForProvenance
	}{
		{
			name: "add new admins",
			setup: func(db worldstate.DB) {
			},
			userID: "admin",
			txID:   "tx1",
			adminUpdates: &worldstate.DBUpdates{
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(UserNamespace) + "admin1",
						Value: admin1Serialized,
						Metadata: &types.Metadata{
							Version: version,
						},
					},
				},
			},
			expected: &provenance.TxDataForProvenance{
				IsValid: true,
				DBName:  worldstate.UsersDBName,
				UserID:  "admin",
				TxID:    "tx1",
				Writes: []*types.KVWithMetadata{
					{
						Key:   "admin1",
						Value: admin1Serialized,
						Metadata: &types.Metadata{
							Version: version,
						},
					},
				},
				Deletes:            make(map[string]*types.Version),
				OldVersionOfWrites: make(map[string]*types.Version),
			},
		},
		{
			name: "update and delete admins",
			setup: func(db worldstate.DB) {
				adminUpdates := &worldstate.DBUpdates{
					DBName: worldstate.UsersDBName,
					Writes: []*worldstate.KVWithMetadata{
						{
							Key:   string(UserNamespace) + "admin1",
							Value: admin1Serialized,
							Metadata: &types.Metadata{
								Version: version,
							},
						},
						{
							Key:   string(UserNamespace) + "admin2",
							Value: admin2Serialized,
							Metadata: &types.Metadata{
								Version: version,
							},
						},
					},
				}

				require.NoError(t, db.Commit([]*worldstate.DBUpdates{adminUpdates}, 1))
			},
			userID: "admin",
			txID:   "tx1",
			adminUpdates: &worldstate.DBUpdates{
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(UserNamespace) + "admin2",
						Value: admin2NewSerialized,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    1,
							},
						},
					},
				},
				Deletes: []string{string(UserNamespace) + "admin1"},
			},
			expected: &provenance.TxDataForProvenance{
				IsValid: true,
				DBName:  worldstate.UsersDBName,
				UserID:  "admin",
				TxID:    "tx1",
				Writes: []*types.KVWithMetadata{
					{
						Key:   "admin2",
						Value: admin2NewSerialized,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    1,
							},
						},
					},
				},
				Deletes: map[string]*types.Version{
					"admin1": version,
				},
				OldVersionOfWrites: map[string]*types.Version{
					"admin2": version,
				},
			},
		},
		{
			name: "no changes in the admin",
			setup: func(db worldstate.DB) {
			},
			userID:       "admin",
			txID:         "tx1",
			adminUpdates: nil,
			expected: &provenance.TxDataForProvenance{
				IsValid:            true,
				DBName:             worldstate.UsersDBName,
				UserID:             "admin",
				TxID:               "tx1",
				Writes:             nil,
				Deletes:            make(map[string]*types.Version),
				OldVersionOfWrites: make(map[string]*types.Version),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := newTestEnv(t)
			defer env.cleanup()

			tt.setup(env.db)

			provenanceData, err := ConstructProvenanceEntriesForClusterAdmins(tt.userID, tt.txID, tt.adminUpdates, env.db)
			require.NoError(t, err)

			require.Len(t, provenanceData.Deletes, len(tt.expected.Deletes))
			for expectedUser, expectedVersion := range tt.expected.Deletes {
				ver := provenanceData.Deletes[expectedUser]
				require.True(t, proto.Equal(expectedVersion, ver))
			}
			tt.expected.Deletes = nil
			provenanceData.Deletes = nil

			require.Len(t, provenanceData.OldVersionOfWrites, len(tt.expected.OldVersionOfWrites))
			for expectedUser, expectedVersion := range tt.expected.OldVersionOfWrites {
				ver := provenanceData.OldVersionOfWrites[expectedUser]
				require.True(t, proto.Equal(expectedVersion, ver))
			}
			tt.expected.OldVersionOfWrites = nil
			provenanceData.OldVersionOfWrites = nil
			require.Equal(t, tt.expected, provenanceData)
		})
	}
}
