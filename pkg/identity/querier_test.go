package identity

import (
	"crypto/x509"
	"encoding/pem"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/logger"
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
	c := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("/tmp", "committer")
	require.NoError(t, err)

	dbPath := filepath.Join(dir, "leveldb")
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    logger,
		},
	)
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
	sampleMetadata := &types.Metadata{
		Version: &types.Version{
			BlockNum: 1,
			TxNum:    1,
		},
		AccessControl: &types.AccessControl{
			ReadUsers: map[string]bool{
				"user1": true,
			},
			ReadWriteUsers: map[string]bool{
				"user2": true,
			},
		},
	}

	b, err := ioutil.ReadFile(path.Join("..", "cryptoservice", "testdata", "alice.pem"))
	require.NoError(t, err)
	bl, _ := pem.Decode(b)
	require.NotNil(t, bl)
	certRaw := bl.Bytes
	certParsed, err := x509.ParseCertificate(certRaw)
	require.NoError(t, err)

	setup := func(db worldstate.DB, u *types.User) {
		user, err := proto.Marshal(u)
		require.NoError(t, err)

		dbUpdates := []*worldstate.DBUpdates{
			{
				DBName: worldstate.UsersDBName,
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      string(UserNamespace) + u.ID,
						Value:    user,
						Metadata: sampleMetadata,
					},
				},
			},
		}

		require.NoError(t, db.Commit(dbUpdates, 1))
	}

	tests := []struct {
		name                                   string
		user                                   *types.User
		userID                                 string
		expectedReadPermissionOnDBs            []string
		expectedReadWritePermissionOnDBs       []string
		expectedNoPermissionOnDBs              []string
		expectedDBAdministrativePrivilege      bool
		expectedClusterAdministrativePrivilege bool
		expectedUserAdministrativePrivilege    bool
	}{
		{
			name: "less privilege",
			user: &types.User{
				ID:          "userWithLessPrivilege",
				Certificate: certRaw,
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
			userID:                                 "userWithLessPrivilege",
			expectedReadPermissionOnDBs:            []string{"db1", "db2", "db3", "db4"},
			expectedReadWritePermissionOnDBs:       []string{"db3", "db4"},
			expectedNoPermissionOnDBs:              []string{"db5", "db6"},
			expectedDBAdministrativePrivilege:      false,
			expectedClusterAdministrativePrivilege: false,
			expectedUserAdministrativePrivilege:    false,
		},
		{
			name: "more privilege",
			user: &types.User{
				ID:          "userWithMorePrivilege",
				Certificate: certRaw,
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
			userID:                                 "userWithMorePrivilege",
			expectedReadPermissionOnDBs:            []string{"db1", "db2", "db3", "db4", "db5"},
			expectedReadWritePermissionOnDBs:       []string{"db1", "db2", "db3", "db4", "db5"},
			expectedNoPermissionOnDBs:              []string{"db6"},
			expectedDBAdministrativePrivilege:      true,
			expectedClusterAdministrativePrivilege: true,
			expectedUserAdministrativePrivilege:    true,
		},
		{
			name: "no privilege",
			user: &types.User{
				ID:          "no Privilege",
				Certificate: certRaw,
				Privilege:   nil,
			},
			userID:                                 "no Privilege",
			expectedReadPermissionOnDBs:            nil,
			expectedReadWritePermissionOnDBs:       nil,
			expectedNoPermissionOnDBs:              []string{"db1", "db2", "db3", "db4", "db5", "db6"},
			expectedDBAdministrativePrivilege:      false,
			expectedClusterAdministrativePrivilege: false,
			expectedUserAdministrativePrivilege:    false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			defer env.cleanup()

			setup(env.db, tt.user)

			t.Run("DoesUserExist", func(t *testing.T) {
				exist, err := env.q.DoesUserExist(tt.userID)
				require.NoError(t, err)
				require.True(t, exist)
			})

			t.Run("GetUser()", func(t *testing.T) {
				persistedUser, persistedMetadata, err := env.q.GetUser(tt.userID)
				require.NoError(t, err)
				require.True(t, proto.Equal(sampleMetadata, persistedMetadata))
				require.True(t, proto.Equal(tt.user, persistedUser))
			})

			t.Run("Read and Write Access Check on DBs", func(t *testing.T) {
				for _, dbName := range tt.expectedReadPermissionOnDBs {
					canRead, err := env.q.HasReadAccessOnDataDB(tt.userID, dbName)
					require.NoError(t, err)
					require.True(t, canRead)
				}

				for _, dbName := range tt.expectedReadWritePermissionOnDBs {
					canReadWrite, err := env.q.HasReadWriteAccess(tt.userID, dbName)
					require.NoError(t, err)
					require.True(t, canReadWrite)
				}

				for _, dbName := range tt.expectedNoPermissionOnDBs {
					canRead, err := env.q.HasReadAccessOnDataDB(tt.userID, dbName)
					require.NoError(t, err)

					require.False(t, canRead)

					canRead, err = env.q.HasReadWriteAccess(tt.userID, dbName)
					require.NoError(t, err)

					require.False(t, canRead)
				}
			})

			t.Run("Check Admin Privileges", func(t *testing.T) {
				perm, err := env.q.HasDBAdministrationPrivilege(tt.userID)
				require.NoError(t, err)
				require.Equal(t, tt.expectedDBAdministrativePrivilege, perm)

				perm, err = env.q.HasClusterAdministrationPrivilege(tt.userID)
				require.NoError(t, err)
				require.Equal(t, tt.expectedClusterAdministrativePrivilege, perm)

				perm, err = env.q.HasUserAdministrationPrivilege(tt.userID)
				require.NoError(t, err)
				require.Equal(t, tt.expectedUserAdministrativePrivilege, perm)
			})

			t.Run("GetAccessControl()", func(t *testing.T) {
				acl, err := env.q.GetAccessControl(tt.userID)
				require.NoError(t, err)
				require.True(t, proto.Equal(sampleMetadata.AccessControl, acl))
			})

			t.Run("GetVersion()", func(t *testing.T) {
				ver, err := env.q.GetVersion(tt.userID)
				require.NoError(t, err)
				require.True(t, proto.Equal(sampleMetadata.Version, ver))
			})

			t.Run("Read and Write Access on the User", func(t *testing.T) {
				canRead, err := env.q.HasReadAccessOnTargetUser("user1", tt.userID)
				require.NoError(t, err)
				require.True(t, canRead)

				canRead, err = env.q.HasReadWriteAccessOnTargetUser("user1", tt.userID)
				require.NoError(t, err)
				require.False(t, canRead)

				canRead, err = env.q.HasReadAccessOnTargetUser("user2", tt.userID)
				require.NoError(t, err)
				require.True(t, canRead)

				canRead, err = env.q.HasReadWriteAccessOnTargetUser("user2", tt.userID)
				require.NoError(t, err)
				require.True(t, canRead)
			})

			t.Run("GetCertificate()", func(t *testing.T) {
				cert, err := env.q.GetCertificate(tt.userID)
				require.NoError(t, err)
				require.True(t, cert.Equal(certParsed))
			})
		})
	}

	t.Run("bad certificate", func(t *testing.T) {
		env := newTestEnv(t)
		defer env.cleanup()

		user := &types.User{
			ID:          "userWithBadCertificate",
			Certificate: []byte("A bad certificate"),
			Privilege: &types.Privilege{
				DBPermission: map[string]types.Privilege_Access{
					"db1": types.Privilege_Read,
				},
				DBAdministration:      false,
				ClusterAdministration: false,
				UserAdministration:    false,
			},
		}
		setup(env.db, user)

		cert, err := env.q.GetCertificate(user.ID)
		require.Contains(t, err.Error(), "asn1: structure error: tags don't match")
		require.Nil(t, cert)
	})
}

func TestQuerierNonExistingUser(t *testing.T) {
	t.Parallel()

	env := newTestEnv(t)
	defer env.cleanup()

	t.Run("DoesUserExist would return false", func(t *testing.T) {
		exist, err := env.q.DoesUserExist("nouser")
		require.NoError(t, err)
		require.False(t, exist)
	})

	t.Run("GetUser would return UserNotFoundErr", func(t *testing.T) {
		user, metadata, err := env.q.GetUser("nouser")
		require.EqualError(t, err, "the user [nouser] does not exist")
		require.Nil(t, user)
		require.Nil(t, metadata)
	})

	t.Run("GetAccessControl returns UserNotFoundErr", func(t *testing.T) {
		acl, err := env.q.GetAccessControl("nouser")
		require.EqualError(t, err, "the user [nouser] does not exist")
		require.Nil(t, acl)
	})

	t.Run("GetCertificate returns UserNotFoundErr", func(t *testing.T) {
		cert, err := env.q.GetCertificate("nouser")
		require.EqualError(t, err, "the user [nouser] does not exist")
		require.Nil(t, cert)
	})

	t.Run("GetVersion returns UserNotFoundErr", func(t *testing.T) {
		ver, err := env.q.GetVersion("nouser")
		require.EqualError(t, err, "the user [nouser] does not exist")
		require.Nil(t, ver)
	})

	t.Run("HasReadAccess returns UserNotFoundErr", func(t *testing.T) {
		perm, err := env.q.HasReadAccessOnDataDB("nouser", "db1")
		require.EqualError(t, err, "the user [nouser] does not exist")
		require.False(t, perm)
	})

	t.Run("HasReadWriteAccess returns UserNotFoundErr", func(t *testing.T) {
		perm, err := env.q.HasReadWriteAccess("nouser", "db1")
		require.EqualError(t, err, "the user [nouser] does not exist")
		require.False(t, perm)
	})

	t.Run("HasDBAdministrationPrivilege returns UserNotFoundErr", func(t *testing.T) {
		perm, err := env.q.HasDBAdministrationPrivilege("nouser")
		require.EqualError(t, err, "the user [nouser] does not exist")
		require.False(t, perm)
	})

	t.Run("HasClusterAdministrationPrivilege returns UserNotFoundErr", func(t *testing.T) {
		perm, err := env.q.HasClusterAdministrationPrivilege("nouser")
		require.EqualError(t, err, "the user [nouser] does not exist")
		require.False(t, perm)
	})

	t.Run("HasUserAdministrationPrivilege returns UserNotFoundErr", func(t *testing.T) {
		perm, err := env.q.HasUserAdministrationPrivilege("nouser")
		require.EqualError(t, err, "the user [nouser] does not exist")
		require.False(t, perm)
	})

	t.Run("HasReadAccessOnTargetUser returns UserNotFoundErr", func(t *testing.T) {
		perm, err := env.q.HasReadAccessOnTargetUser("user1", "nouser")
		require.EqualError(t, err, "the user [nouser] does not exist")
		require.False(t, perm)
	})

	t.Run("HasReadWriteAccessOnTargetUser returns UserNotFoundErr", func(t *testing.T) {
		perm, err := env.q.HasReadWriteAccessOnTargetUser("user1", "nouser")
		require.EqualError(t, err, "the user [nouser] does not exist")
		require.False(t, perm)
	})
}
