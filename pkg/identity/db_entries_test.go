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
						Key:   string(userNamespace) + "user1",
						Value: sampleUser("user1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 1,
								TxNum:    5,
							},
						},
					},
					{
						Key:   string(userNamespace) + "user2",
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
					string(userNamespace) + "user3",
					string(userNamespace) + "user4",
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
						Key:   string(userNamespace) + "user1",
						Value: sampleUser("user1"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								BlockNum: 2,
								TxNum:    2,
							},
						},
					},
					{
						Key:   string(userNamespace) + "user2",
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
					string(userNamespace) + "user3",
					string(userNamespace) + "user4",
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
