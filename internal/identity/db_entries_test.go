// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package identity

import (
	"testing"

	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestConstructDBEntriesForUserAdminTx(t *testing.T) {
	t.Parallel()

	sampleUser := func(userID string) *types.User {
		return &types.User{
			Id:          userID,
			Certificate: []byte("certificate-" + userID),
			Privilege: &types.Privilege{
				DbPermission: map[string]types.Privilege_Access{
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
						UserId: "user3",
					},
					{
						UserId: "user4",
					},
				},
			},
			version: nil,
			expectedDBUpdates: &worldstate.DBUpdates{
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
						UserId: "user3",
					},
					{
						UserId: "user4",
					},
				},
			},
			version: &types.Version{
				BlockNum: 2,
				TxNum:    2,
			},
			expectedDBUpdates: &worldstate.DBUpdates{
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
			Id:          adminID,
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
					Id:          "admin1",
					Certificate: []byte("certificate 1"),
				},
				{
					Id:          "admin2",
					Certificate: []byte("certificate 2"),
				},
			},
			adminsInNewConfigTx: []*types.Admin{
				{
					Id:          "admin1",
					Certificate: []byte("certificate 1"),
				},
				{
					Id:          "admin2",
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
					Id:          "admin1",
					Certificate: []byte("certificate 1"),
				},
				{
					Id:          "admin2",
					Certificate: []byte("certificate 2"),
				},
				{
					Id:          "admin3",
					Certificate: []byte("certificate 3"),
				},
			},
			adminsInNewConfigTx: []*types.Admin{
				{
					Id:          "admin3",
					Certificate: []byte("new certificate 3"),
				},
				{
					Id:          "admin4",
					Certificate: []byte("certificate 4"),
				},
				{
					Id:          "admin5",
					Certificate: []byte("certificate 5"),
				},
			},
			version: sampleVersion,
			expectedUpdates: &worldstate.DBUpdates{
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
		Id:          "user1",
		Certificate: []byte("rawcert"),
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				"db1": types.Privilege_Read,
			},
		},
	}
	user1Serialized, err := proto.Marshal(user1)
	require.NoError(t, err)

	user2 := &types.User{
		Id:          "user2",
		Certificate: []byte("rawcert"),
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
				"db2": types.Privilege_ReadWrite,
			},
		},
	}
	user2Serialized, err := proto.Marshal(user2)
	require.NoError(t, err)

	user2New := &types.User{
		Id:          "user2",
		Certificate: []byte("rawcertNew"),
		Privilege: &types.Privilege{
			DbPermission: map[string]types.Privilege_Access{
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
				UserId:    "admin",
				TxId:      "tx1",
				UserReads: nil,
				UserWrites: []*types.UserWrite{
					{
						User: user1,
						Acl:  acl,
					},
					{
						User: user2,
						Acl:  nil,
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
				dbUpdates := map[string]*worldstate.DBUpdates{
					worldstate.UsersDBName: {
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
				UserId:    "admin",
				TxId:      "tx1",
				UserReads: nil,
				UserDeletes: []*types.UserDelete{
					{
						UserId: "user1",
					},
					{
						UserId: "user2",
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
				dbUpdates := map[string]*worldstate.DBUpdates{
					worldstate.UsersDBName: {
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
				UserId: "admin",
				TxId:   "tx1",
				UserReads: []*types.UserRead{
					{
						UserId:  "user1",
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
		Id:          "admin1",
		Certificate: []byte("rawcert-admin1"),
		Privilege: &types.Privilege{
			Admin: true,
		},
	}
	admin1Serialized, err := proto.Marshal(admin1)
	require.NoError(t, err)

	admin2 := &types.User{
		Id:          "admin2",
		Certificate: []byte("rawcert-admin2"),
		Privilege: &types.Privilege{
			Admin: true,
		},
	}
	admin2Serialized, err := proto.Marshal(admin2)
	require.NoError(t, err)

	admin2New := &types.User{
		Id:          "admin2",
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

				require.NoError(t, db.Commit(
					map[string]*worldstate.DBUpdates{
						worldstate.UsersDBName: adminUpdates,
					},
					1,
				))
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

func TestConstructDBEntriesForNodes(t *testing.T) {
	t.Parallel()

	sampleVersion := &types.Version{
		BlockNum: 2,
		TxNum:    2,
	}

	sampleNode := func(nodeID string, addr string, port uint32, cert []byte) []byte {
		user := &types.NodeConfig{
			Id:          nodeID,
			Address:     addr,
			Port:        port,
			Certificate: cert,
		}

		u, err := proto.Marshal(user)
		require.NoError(t, err)
		return u
	}

	var tests = []struct {
		name                     string
		nodesInCommittedConfigTx []*types.NodeConfig
		nodesInNewConfigTx       []*types.NodeConfig
		version                  *types.Version
		expectedUpdates          *worldstate.DBUpdates
	}{
		{
			name: "same set of nodes, no changes",
			nodesInCommittedConfigTx: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "192.168.0.1",
					Port:        6000,
					Certificate: []byte("certificate 1"),
				},
				{
					Id:          "node2",
					Address:     "192.168.0.2",
					Port:        6001,
					Certificate: []byte("certificate 2"),
				},
			},
			nodesInNewConfigTx: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "192.168.0.1",
					Port:        6000,
					Certificate: []byte("certificate 1"),
				},
				{
					Id:          "node2",
					Address:     "192.168.0.2",
					Port:        6001,
					Certificate: []byte("certificate 2"),
				},
			},
			version:         sampleVersion,
			expectedUpdates: nil,
		},
		{
			name: "add, update, and delete nodes",
			nodesInCommittedConfigTx: []*types.NodeConfig{
				{
					Id:          "node1",
					Address:     "192.168.0.1",
					Port:        6000,
					Certificate: []byte("certificate 1"),
				},
				{
					Id:          "node2",
					Address:     "192.168.0.2",
					Port:        6001,
					Certificate: []byte("certificate 2"),
				},
				{
					Id:          "node3",
					Address:     "192.168.0.3",
					Port:        6002,
					Certificate: []byte("certificate 3"),
				},
			},
			nodesInNewConfigTx: []*types.NodeConfig{
				{
					Id:          "node3",
					Address:     "192.168.0.3",
					Port:        6002,
					Certificate: []byte("new certificate 3"),
				},
				{
					Id:          "node4",
					Address:     "192.168.0.4",
					Port:        6003,
					Certificate: []byte("certificate 4"),
				},
				{
					Id:          "node5",
					Address:     "192.168.0.5",
					Port:        6004,
					Certificate: []byte("certificate 5"),
				},
			},
			version: sampleVersion,
			expectedUpdates: &worldstate.DBUpdates{
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(NodeNamespace) + "node3",
						Value: sampleNode("node3", "192.168.0.3", 6002, []byte("new certificate 3")),
						Metadata: &types.Metadata{
							Version: sampleVersion,
						},
					},
					{
						Key:   string(NodeNamespace) + "node4",
						Value: sampleNode("node4", "192.168.0.4", 6003, []byte("certificate 4")),
						Metadata: &types.Metadata{
							Version: sampleVersion,
						},
					},
					{
						Key:   string(NodeNamespace) + "node5",
						Value: sampleNode("node5", "192.168.0.5", 6004, []byte("certificate 5")),
						Metadata: &types.Metadata{
							Version: sampleVersion,
						},
					},
				},
				Deletes: []string{string(NodeNamespace) + "node1", string(NodeNamespace) + "node2"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			updates, err := ConstructDBEntriesForNodes(tt.nodesInCommittedConfigTx, tt.nodesInNewConfigTx, sampleVersion)
			require.NoError(t, err)
			if updates == nil {
				require.Equal(t, tt.expectedUpdates, updates)
				return
			}

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

func TestConstructProvenanceEntriesForNodes(t *testing.T) {
	t.Parallel()

	version := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}
	node1 := &types.NodeConfig{
		Id:          "node1",
		Certificate: []byte("rawcert-node1"),
	}
	node1Serialized, err := proto.Marshal(node1)
	require.NoError(t, err)

	node2 := &types.NodeConfig{
		Id:          "node2",
		Certificate: []byte("rawcert-node2"),
	}
	node2Serialized, err := proto.Marshal(node2)
	require.NoError(t, err)

	node2New := &types.NodeConfig{
		Id:          "node2",
		Certificate: []byte("rawcertNew-node2"),
	}
	node2NewSerialized, err := proto.Marshal(node2New)
	require.NoError(t, err)

	tests := []struct {
		name        string
		setup       func(db worldstate.DB)
		userID      string
		txID        string
		nodeUpdates *worldstate.DBUpdates
		expected    *provenance.TxDataForProvenance
	}{
		{
			name: "add new node",
			setup: func(db worldstate.DB) {
			},
			userID: "admin",
			txID:   "tx1",
			nodeUpdates: &worldstate.DBUpdates{
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(NodeNamespace) + "node1",
						Value: node1Serialized,
						Metadata: &types.Metadata{
							Version: version,
						},
					},
				},
			},
			expected: &provenance.TxDataForProvenance{
				IsValid: true,
				DBName:  worldstate.ConfigDBName,
				UserID:  "admin",
				TxID:    "tx1",
				Writes: []*types.KVWithMetadata{
					{
						Key:   "node1",
						Value: node1Serialized,
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
			name: "update and delete nodes",
			setup: func(db worldstate.DB) {
				adminUpdates := &worldstate.DBUpdates{
					Writes: []*worldstate.KVWithMetadata{
						{
							Key:   string(NodeNamespace) + "node1",
							Value: node1Serialized,
							Metadata: &types.Metadata{
								Version: version,
							},
						},
						{
							Key:   string(NodeNamespace) + "node2",
							Value: node2Serialized,
							Metadata: &types.Metadata{
								Version: version,
							},
						},
					},
				}

				require.NoError(t, db.Commit(
					map[string]*worldstate.DBUpdates{
						worldstate.ConfigDBName: adminUpdates,
					},
					1,
				))
			},
			userID: "admin",
			txID:   "tx1",
			nodeUpdates: &worldstate.DBUpdates{
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:   string(NodeNamespace) + "node2",
						Value: node2NewSerialized,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    1,
							},
						},
					},
				},
				Deletes: []string{string(NodeNamespace) + "node1"},
			},
			expected: &provenance.TxDataForProvenance{
				IsValid: true,
				DBName:  worldstate.ConfigDBName,
				UserID:  "admin",
				TxID:    "tx1",
				Writes: []*types.KVWithMetadata{
					{
						Key:   "node2",
						Value: node2NewSerialized,
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    1,
							},
						},
					},
				},
				Deletes: map[string]*types.Version{
					"node1": version,
				},
				OldVersionOfWrites: map[string]*types.Version{
					"node2": version,
				},
			},
		},
		{
			name: "no changes in the nodes",
			setup: func(db worldstate.DB) {
			},
			userID:      "admin",
			txID:        "tx1",
			nodeUpdates: nil,
			expected: &provenance.TxDataForProvenance{
				IsValid:            true,
				DBName:             worldstate.ConfigDBName,
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

			provenanceData, err := ConstructProvenanceEntriesForNodes(tt.userID, tt.txID, tt.nodeUpdates, env.db)
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
