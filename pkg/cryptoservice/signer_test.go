// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package cryptoservice_test

import (
	"net/http"
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"

	"github.com/IBM-Blockchain/bcdb-server/internal/httphandler"
	"github.com/IBM-Blockchain/bcdb-server/pkg/cryptoservice"
	"github.com/IBM-Blockchain/bcdb-server/pkg/cryptoservice/mocks"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server/testutils"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestSignQuery(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "unit-test",
	})
	require.NoError(t, err)
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	cert, signer := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	userDB := &mocks.UserDBQuerier{}
	sigVerifier := cryptoservice.NewVerifier(userDB, lg)
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
			err, status = httphandler.VerifyRequestSignature(sigVerifier, "alice", sig, q)
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
