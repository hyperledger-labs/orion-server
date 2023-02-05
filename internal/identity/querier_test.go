// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package identity

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
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

	dir := t.TempDir()

	dbPath := filepath.Join(dir, "leveldb")
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    logger,
		},
	)
	if err != nil {
		t.Fatalf("error while creating leveldb, %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("error while closing the db instance, %v", err)
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

	caCert, caKey, err := testutils.GenerateRootCA("root", "127.0.0.1")
	require.NoError(t, err)
	keyPair, err := tls.X509KeyPair(caCert, caKey)
	require.NoError(t, err)
	require.NotNil(t, keyPair)
	cert, _, err := testutils.IssueCertificate("alice", "127.0.0.1", keyPair)
	require.NoError(t, err)
	bl, _ := pem.Decode(cert)
	require.NotNil(t, bl)
	certRaw := bl.Bytes
	certParsed, err := x509.ParseCertificate(certRaw)
	require.NoError(t, err)

	setup := func(db worldstate.DB, u *types.User) {
		user, err := proto.Marshal(u)
		require.NoError(t, err)

		dbUpdates := map[string]*worldstate.DBUpdates{
			worldstate.UsersDBName: {
				Writes: []*worldstate.KVWithMetadata{
					{
						Key:      string(UserNamespace) + u.Id,
						Value:    user,
						Metadata: sampleMetadata,
					},
				},
			},
		}

		require.NoError(t, db.Commit(dbUpdates, 1))
	}

	tests := []struct {
		name                             string
		user                             *types.User
		userID                           string
		expectedReadPermissionOnDBs      []string
		expectedReadWritePermissionOnDBs []string
		expectedNoPermissionOnDBs        []string
		expectedAdministrativePrivilege  bool
	}{
		{
			name: "less privilege",
			user: &types.User{
				Id:          "userWithLessPrivilege",
				Certificate: certRaw,
				Privilege: &types.Privilege{
					DbPermission: map[string]types.Privilege_Access{
						"db1": types.Privilege_Read,
						"db2": types.Privilege_Read,
						"db3": types.Privilege_ReadWrite,
						"db4": types.Privilege_ReadWrite,
					},
					Admin: false,
				},
			},
			userID:                           "userWithLessPrivilege",
			expectedReadPermissionOnDBs:      []string{"db1", "db2", "db3", "db4"},
			expectedReadWritePermissionOnDBs: []string{"db3", "db4"},
			expectedNoPermissionOnDBs:        []string{"db5", "db6"},
			expectedAdministrativePrivilege:  false,
		},
		{
			name: "more privilege",
			user: &types.User{
				Id:          "userWithMorePrivilege",
				Certificate: certRaw,
				Privilege: &types.Privilege{
					Admin: true,
				},
			},
			userID:                           "userWithMorePrivilege",
			expectedReadPermissionOnDBs:      []string{"db1", "db2", "db3", "db4", "db5"},
			expectedReadWritePermissionOnDBs: []string{"db1", "db2", "db3", "db4", "db5"},
			expectedNoPermissionOnDBs:        nil,
			expectedAdministrativePrivilege:  true,
		},
		{
			name: "no privilege",
			user: &types.User{
				Id:          "no Privilege",
				Certificate: certRaw,
				Privilege:   nil,
			},
			userID:                           "no Privilege",
			expectedReadPermissionOnDBs:      nil,
			expectedReadWritePermissionOnDBs: nil,
			expectedNoPermissionOnDBs:        []string{"db1", "db2", "db3", "db4", "db5", "db6"},
			expectedAdministrativePrivilege:  false,
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
				perm, err := env.q.HasAdministrationPrivilege(tt.userID)
				require.NoError(t, err)
				require.Equal(t, tt.expectedAdministrativePrivilege, perm)
			})

			t.Run("GetAccessControl()", func(t *testing.T) {
				acl, err := env.q.GetAccessControl(tt.userID)
				require.NoError(t, err)
				require.True(t, proto.Equal(sampleMetadata.AccessControl, acl))
			})

			t.Run("GetUserVersion()", func(t *testing.T) {
				ver, err := env.q.GetUserVersion(tt.userID)
				require.NoError(t, err)
				require.True(t, proto.Equal(sampleMetadata.Version, ver))
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
			Id:          "userWithBadCertificate",
			Certificate: []byte("A bad certificate"),
			Privilege: &types.Privilege{
				DbPermission: map[string]types.Privilege_Access{
					"db1": types.Privilege_Read,
				},
				Admin: false,
			},
		}
		setup(env.db, user)

		cert, err := env.q.GetCertificate(user.Id)
		require.Contains(t, err.Error(), "x509: malformed certificate")
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
		ver, err := env.q.GetUserVersion("nouser")
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
		perm, err := env.q.HasAdministrationPrivilege("nouser")
		require.EqualError(t, err, "the user [nouser] does not exist")
		require.False(t, perm)
	})

}
