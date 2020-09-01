package identity

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

func TestConstructDBEntriesForUsers(t *testing.T) {
	t.Parallel()

	sampleUser := func(userID string) []byte {
		user := &types.User{
			ID:          userID,
			Certificate: []byte("certificate-" + userID),
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					worldstate.DefaultDBName: types.Privilege_ReadWrite,
				},
				DBAdministration:      false,
				ClusterAdministration: false,
				UserAdministration:    false,
			},
		}

		u, err := proto.Marshal(user)
		require.NoError(t, err)
		return u
	}

	tests := []struct {
		name              string
		transaction       *types.Transaction
		version           *types.Version
		expectedDBUpdates *worldstate.DBUpdates
	}{
		{
			name: "only writes",
			transaction: &types.Transaction{
				Type:   0,
				DBName: worldstate.UsersDBName,
				Writes: []*types.KVWrite{
					{
						Key:   "user1",
						Value: sampleUser("user1"),
					},
					{
						Key:   "user2",
						Value: sampleUser("user2"),
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
						Value: sampleUser("user1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    5,
							},
						},
					},
					{
						Key:   string(UserNamespace) + "user2",
						Value: sampleUser("user2"),
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
			transaction: &types.Transaction{
				Type:   0,
				DBName: worldstate.UsersDBName,
				Writes: []*types.KVWrite{
					{
						Key:      "user3",
						IsDelete: true,
					},
					{
						Key:      "user4",
						IsDelete: true,
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
			transaction: &types.Transaction{
				Type:   0,
				DBName: worldstate.UsersDBName,
				Writes: []*types.KVWrite{
					{
						Key:   "user1",
						Value: sampleUser("user1"),
					},
					{
						Key:   "user2",
						Value: sampleUser("user2"),
					},
					{
						Key:      "user3",
						IsDelete: true,
					},
					{
						Key:      "user4",
						IsDelete: true,
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
						Value: sampleUser("user1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    2,
							},
						},
					},
					{
						Key:   string(UserNamespace) + "user2",
						Value: sampleUser("user2"),
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

			dbUpdates := ConstructDBEntriesForUsers(tt.transaction, tt.version)
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
				DBAdministration:      true,
				ClusterAdministration: true,
				UserAdministration:    true,
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
			version:         nil,
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
