package identity

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type testEnv struct {
	db      *leveldb.LevelDB
	dbPath  string
	q       *Querier
	cleanup func()
}

func newTestEnv(t *testing.T) *testEnv {
	dir, err := ioutil.TempDir("/tmp", "committer")
	require.NoError(t, err)

	dbPath := filepath.Join(dir, "leveldb")
	db, err := leveldb.New(dbPath)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, rmErr)
		}
		t.Fatalf("error while creating leveldb, %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("error while closing the db instance, %v", err)
		}

		if err := os.RemoveAll(dir); err != nil {
			t.Fatalf("error while removing directory %s, %v", dir, err)
		}
	}

	return &testEnv{
		db:      db,
		dbPath:  dbPath,
		q:       NewQuerier(db),
		cleanup: cleanup,
	}
}

func TestQuerier(t *testing.T) {
	tests := []struct {
		name                   string
		user                   *types.User
		hasReadPermission      []string
		hasReadWritePermission []string
		hasNoPermission        []string
		DBAdministration       bool
		ClusterAdministration  bool
		UserAdministration     bool
	}{
		{
			name: "less privilege",
			user: &types.User{
				ID:          "userWithLessPrivilege",
				Certificate: []byte("certificate-1"),
				Privilege: &types.Privilege{
					DBPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_Read,
						"db2": types.Privilege_Read,
						"db3": types.Privilege_ReadWrite,
						"db4": types.Privilege_ReadWrite,
					},
					DBAdministration:      false,
					ClusterAdministration: false,
					UserAdministration:    false,
				},
			},
			hasReadPermission:      []string{"db1", "db2", "db3", "db4"},
			hasReadWritePermission: []string{"db3", "db4"},
			hasNoPermission:        []string{"db5", "db6"},
			DBAdministration:       false,
			ClusterAdministration:  false,
			UserAdministration:     false,
		},
		{
			name: "more privilege",
			user: &types.User{
				ID:          "userWithMorePrivilege",
				Certificate: []byte("certificate-2"),
				Privilege: &types.Privilege{
					DBPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_ReadWrite,
						"db2": types.Privilege_ReadWrite,
						"db3": types.Privilege_ReadWrite,
						"db4": types.Privilege_ReadWrite,
						"db5": types.Privilege_ReadWrite,
					},
					DBAdministration:      true,
					ClusterAdministration: true,
					UserAdministration:    true,
				},
			},
			hasReadPermission:      []string{"db1", "db2", "db3", "db4", "db5"},
			hasReadWritePermission: []string{"db1", "db2", "db3", "db4", "db5"},
			hasNoPermission:        []string{"db6"},
			DBAdministration:       true,
			ClusterAdministration:  true,
			UserAdministration:     true,
		},
		{
			name: "no privilege",
			user: &types.User{
				ID:          "no Privilege",
				Certificate: []byte("certificate-3"),
				Privilege:   nil,
			},
			hasReadPermission:      nil,
			hasReadWritePermission: nil,
			hasNoPermission:        []string{"db1", "db2", "db3", "db4", "db5", "db6"},
			DBAdministration:       false,
			ClusterAdministration:  false,
			UserAdministration:     false,
		},
		{
			name:                   "no user",
			user:                   nil,
			hasReadPermission:      nil,
			hasReadWritePermission: nil,
			hasNoPermission:        []string{"db1", "db2", "db3", "db4", "db5", "db6"},
			DBAdministration:       false,
			ClusterAdministration:  false,
			UserAdministration:     false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			defer env.cleanup()

			var metadata *types.Metadata
			var userID string

			if tt.user != nil {
				user, err := proto.Marshal(tt.user)
				require.NoError(t, err)

				metadata = &types.Metadata{
					Version: &types.Version{
						BlockNum: 1,
						TxNum:    1,
					},
				}

				dbUpdates := []*worldstate.DBUpdates{
					{
						DBName: worldstate.UsersDBName,
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:      string(userNamespace) + tt.user.ID,
								Value:    user,
								Metadata: metadata,
							},
						},
					},
				}

				require.NoError(t, env.db.Commit(dbUpdates))

				userID = tt.user.ID
			}

			persistedUser, persistedMetadata, err := env.q.GetUser(userID)
			require.NoError(t, err)
			require.True(t, proto.Equal(metadata, persistedMetadata))
			require.True(t, proto.Equal(tt.user, persistedUser))

			for _, dbName := range tt.hasReadPermission {
				canRead, err := env.q.HasReadAccess(userID, dbName)
				require.NoError(t, err)
				require.True(t, canRead)
			}

			for _, dbName := range tt.hasReadWritePermission {
				canReadWrite, err := env.q.HasReadWriteAccess(userID, dbName)
				require.NoError(t, err)
				require.True(t, canReadWrite)
			}

			for _, dbName := range tt.hasNoPermission {
				canRead, err := env.q.HasReadAccess(userID, dbName)
				require.NoError(t, err)
				require.False(t, canRead)

				canRead, err = env.q.HasReadWriteAccess(userID, dbName)
				require.NoError(t, err)
				require.False(t, canRead)
			}

			perm, err := env.q.HasDBAdministrationPrivilege(userID)
			require.NoError(t, err)
			require.Equal(t, tt.DBAdministration, perm)

			perm, err = env.q.HasClusterAdministrationPrivilege(userID)
			require.NoError(t, err)
			require.Equal(t, tt.ClusterAdministration, perm)

			perm, err = env.q.HasUserAdministrationPrivilege(userID)
			require.NoError(t, err)
			require.Equal(t, tt.UserAdministration, perm)
		})
	}
}
