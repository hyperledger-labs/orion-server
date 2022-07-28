// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"errors"
	"net/http"
	"testing"

	"github.com/hyperledger-labs/orion-server/internal/bcdb/mocks"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestVerifyRequestSignature(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "unit-test",
	})
	require.NoError(t, err)
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	t.Run("good sig", func(t *testing.T) {
		db := &mocks.DB{}
		verifier := cryptoservice.NewVerifier(db, lg)
		db.On("GetCertificate", "alice").Return(aliceCert, nil)
		payload := &types.UserAdministrationTx{UserId: "alice", TxId: "xxx"}
		err, code := VerifyRequestSignature(verifier, "alice", testutils.SignatureFromTx(t, aliceSigner, payload), payload)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, code)
	})

	t.Run("no such user", func(t *testing.T) {
		db := &mocks.DB{}
		verifier := cryptoservice.NewVerifier(db, lg)
		db.On("GetCertificate", "alice").Return(nil, errors.New("no such user"))
		payload := &types.UserAdministrationTx{UserId: "alice", TxId: "xxx"}
		err, code := VerifyRequestSignature(verifier, "alice", testutils.SignatureFromTx(t, aliceSigner, payload), payload)
		require.EqualError(t, err, "signature verification failed")
		require.Equal(t, http.StatusUnauthorized, code)
	})

	t.Run("bad sig", func(t *testing.T) {
		db := &mocks.DB{}
		verifier := cryptoservice.NewVerifier(db, lg)
		db.On("GetCertificate", "alice").Return(aliceCert, nil)
		payload := &types.UserAdministrationTx{UserId: "alice", TxId: "xxx"}
		err, code := VerifyRequestSignature(verifier, "alice", []byte("bad-sig"), payload)
		require.EqualError(t, err, "signature verification failed")
		require.Equal(t, http.StatusUnauthorized, code)
	})

	t.Run("internal error", func(t *testing.T) {
		db := &mocks.DB{}
		verifier := cryptoservice.NewVerifier(db, lg)
		payload := make(chan struct{})
		err, code := VerifyRequestSignature(verifier, "alice", []byte("something"), payload)
		require.EqualError(t, err, "payload is not a protoreflect message")
		require.Equal(t, http.StatusInternalServerError, code)
	})
}

var correctTxRespEnv *types.TxReceiptResponseEnvelope

func init() {
	correctTxRespEnv = &types.TxReceiptResponseEnvelope{
		Response: &types.TxReceiptResponse{
			Header: &types.ResponseHeader{
				NodeId: "node1",
			},
			Receipt: &types.TxReceipt{
				Header: &types.BlockHeader{
					BaseHeader: &types.BlockHeaderBase{
						Number: 1,
					},
				},
				TxIndex: 1,
			},
		},
	}
}
