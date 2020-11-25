package cryptoservice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/cryptoservice"
	"github.ibm.com/blockchaindb/server/internal/cryptoservice/mocks"
	"github.ibm.com/blockchaindb/server/internal/server/backend/handlers"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestSignQuery(t *testing.T) {
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	cert, signer := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	userDB := &mocks.UserDBQuerier{}
	sigVerifier := cryptoservice.NewVerifier(userDB)
	userDB.GetCertificateReturns(cert, nil)

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
			var status int
			err, status = handlers.VerifyRequestSignature(sigVerifier, "alice", sig, q)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, status)
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

func TestSignQueryResponse(t *testing.T) {
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	cert, signer := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	t.Run("Sign correctly", func(t *testing.T) {
		queriesRsps := []interface{}{
			&types.GetUserResponse{},
			&types.GetDataResponse{},
			&types.GetDBStatusResponse{},
			&types.GetConfigResponse{},
			&types.GetLedgerPathResponse{},
			&types.GetBlockResponse{},
		}

		for _, q := range queriesRsps {
			sig, err := cryptoservice.SignQueryResponse(signer, q)
			require.NoError(t, err)
			require.NotNil(t, sig)
			testutils.VerifyPayloadSignature(t, cert.Raw, q, sig)
		}
	})

	t.Run("Unknown type", func(t *testing.T) {
		notQR := &types.GetConfigResponseEnvelope{}

		sig, err := cryptoservice.SignQueryResponse(signer, notQR)
		require.EqualError(t, err, "unknown query response type: *types.GetConfigResponseEnvelope")
		require.Nil(t, sig)
	})
}
