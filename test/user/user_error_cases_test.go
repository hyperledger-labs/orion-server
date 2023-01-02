package user

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/hyperledger-labs/orion-server/test/setup"
	"github.com/stretchr/testify/require"
)

func TestUserTxErrorCases(t *testing.T) {
	dir := t.TempDir()

	nPort, pPort := getPorts(1)
	setupConfig := &setup.Config{
		NumberOfServers:     1,
		TestDirAbsolutePath: dir,
		BDBBinaryPath:       "../../bin/bdb",
		CmdTimeout:          10 * time.Second,
		BaseNodePort:        nPort + 5,
		BasePeerPort:        pPort + 5,
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

	t.Run("creating an user with acl on non-existence database would fail", func(t *testing.T) {
		aliceCert, _ := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "alice")
		alice := &types.User{
			Id:          "alice",
			Certificate: aliceCert.Raw,
			Privilege: &types.Privilege{
				DbPermission: map[string]types.Privilege_Access{
					"db1": types.Privilege_Read,
				},
			},
		}

		userTx := &types.UserAdministrationTx{
			UserId:     "admin",
			TxId:       uuid.New().String(),
			UserWrites: []*types.UserWrite{{User: alice}},
		}

		_, err := s.SubmitTransaction(t, constants.PostUserTx, &types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, s.AdminSigner(), userTx),
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_DATABASE_DOES_NOT_EXIST, Reason: the database [db1] present in the db permission list does not exist in the cluster")
	})

	t.Run("creating an user with incorrect certificate would fail", func(t *testing.T) {
		alice := &types.User{
			Id:          "alice",
			Certificate: []byte("junk"),
		}

		userTx := &types.UserAdministrationTx{
			UserId: "admin",
			TxId:   uuid.New().String(),
			UserWrites: []*types.UserWrite{
				{
					User: alice,
				},
			},
		}

		_, err := s.SubmitTransaction(t, constants.PostUserTx, &types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, s.AdminSigner(), userTx),
		})
		require.Contains(t, err.Error(), "TxValidation: Flag: INVALID_INCORRECT_ENTRIES, Reason: the user [alice] in the write list has an invalid certificate")
	})

	t.Run("creating an user with an empty ID would fail", func(t *testing.T) {
		alice := &types.User{}

		userTx := &types.UserAdministrationTx{
			UserId: "admin",
			TxId:   uuid.New().String(),
			UserWrites: []*types.UserWrite{
				{
					User: alice,
				},
			},
		}

		_, err := s.SubmitTransaction(t, constants.PostUserTx, &types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, s.AdminSigner(), userTx),
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_INCORRECT_ENTRIES, Reason: there is an user in the write list with an empty ID. A valid userID must be an non-empty string")
	})

	t.Run("creating an user by non-admin would fail", func(t *testing.T) {
		aliceCert, aliceSigner := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "alice")
		alice := &types.User{
			Id:          "alice",
			Certificate: aliceCert.Raw,
		}

		userTx := &types.UserAdministrationTx{
			UserId:     "admin",
			TxId:       uuid.New().String(),
			UserWrites: []*types.UserWrite{{User: alice}},
		}

		_, err := s.SubmitTransaction(t, constants.PostUserTx, &types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, s.AdminSigner(), userTx),
		})
		require.NoError(t, err)

		bobCert, _ := testutils.LoadTestCrypto(t, c.GetUserCertDir(), "bob")
		bob := &types.User{
			Id:          "alice",
			Certificate: bobCert.Raw,
		}

		userTx = &types.UserAdministrationTx{
			UserId:     "alice",
			TxId:       uuid.New().String(),
			UserWrites: []*types.UserWrite{{User: bob}},
		}

		_, err = s.SubmitTransaction(t, constants.PostUserTx, &types.UserAdministrationTxEnvelope{
			Payload:   userTx,
			Signature: testutils.SignatureFromTx(t, aliceSigner, userTx),
		})
		require.EqualError(t, err, "TxValidation: Flag: INVALID_NO_PERMISSION, Reason: the user [alice] has no privilege to perform user administrative operations")
	})
}
