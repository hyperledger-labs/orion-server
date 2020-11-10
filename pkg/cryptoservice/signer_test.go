package cryptoservice_test

import (
	"crypto/x509"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/crypto"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice/mocks"
	"github.ibm.com/blockchaindb/server/pkg/server/backend/handlers"
)

func TestSignQuery(t *testing.T) {
	rawCert := loadRawCertificate(t, path.Join("testdata", "alice.pem"))
	cert, err := x509.ParseCertificate(rawCert)
	require.NoError(t, err)
	userDB := &mocks.UserDBQuerier{}
	sigVerifier := cryptoservice.NewVerifier(userDB)
	userDB.GetCertificateReturns(cert, nil)

	signer, err := crypto.NewSigner(&crypto.SignerOptions{KeyFilePath: path.Join("testdata", "alice.key")})

	t.Run("Sign correctly", func(t *testing.T) {
		queries := []interface{}{
			&types.GetConfigQuery{UserID: "id"},
			&types.GetDataQuery{UserID: "id", DBName: "db", Key: "foo"},
			&types.GetDBStatusQuery{UserID: "id", DBName: "db"},
			&types.GetUserQuery{UserID: "id", TargetUserID: "target"},
		}

		for _, q := range queries {
			sig, err := cryptoservice.SignQuery(signer, q)
			require.NoError(t, err)
			require.NotNil(t, sig)
			err, _ = handlers.VerifyQuerySignature(sigVerifier, "alice", sig, q)
			require.NoError(t, err)
		}
	})

	t.Run("Unknown type", func(t *testing.T) {
		notQ := &types.GetConfigQueryEnvelope{
			Payload:   &types.GetConfigQuery{UserID: "id"},
			Signature: []byte("oops"),
		}

		sig, err := cryptoservice.SignQuery(signer, notQ)
		require.EqualError(t, err, "unknown query type: *types.GetConfigQueryEnvelope")
		require.Nil(t, sig)
	})
}
