package httphandler

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.ibm.com/blockchaindb/server/pkg/logger"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/bcdb/mocks"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestSendHTTPResponse(t *testing.T) {
	t.Parallel()

	t.Run("ok status", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		dbStatus := &types.ResponseEnvelope{
			Payload: MarshalOrPanic(&types.Payload{
				Header: &types.ResponseHeader{
					NodeID: "testID",
				},
				Response: MarshalOrPanic(&types.GetDBStatusResponse{
				}),
			}),
		}
		SendHTTPResponse(w, http.StatusOK, dbStatus)

		require.Equal(t, http.StatusOK, w.Code)
		actualDBStatus := &types.ResponseEnvelope{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), actualDBStatus))
		require.True(t, proto.Equal(dbStatus, actualDBStatus))
	})

	t.Run("forbidden status", func(t *testing.T) {
		t.Parallel()

		w := httptest.NewRecorder()
		err := &types.HttpResponseErr{
			ErrMsg: "user does not have a read permission",
		}
		SendHTTPResponse(w, http.StatusForbidden, err)

		require.Equal(t, http.StatusForbidden, w.Code)
		actualErr := &types.HttpResponseErr{}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), actualErr))
		require.Equal(t, err, actualErr)
	})
}

func TestVerifyRequestSignature(t *testing.T) {
	lg, err := logger.New(&logger.Config{
		Level:         "info",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
		Name:          "unit-test",
	})
	require.NoError(t, err)
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	t.Run("good sig", func(t *testing.T) {
		db := &mocks.DB{}
		verifier := cryptoservice.NewVerifier(db, lg)
		db.On("GetCertificate", "alice").Return(aliceCert, nil)
		payload := &types.UserAdministrationTx{UserID: "alice", TxID: "xxx"}
		err, code := VerifyRequestSignature(verifier, "alice", testutils.SignatureFromTx(t, aliceSigner, payload), payload)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, code)
	})

	t.Run("no such user", func(t *testing.T) {
		db := &mocks.DB{}
		verifier := cryptoservice.NewVerifier(db, lg)
		db.On("GetCertificate", "alice").Return(nil, errors.New("no such user"))
		payload := &types.UserAdministrationTx{UserID: "alice", TxID: "xxx"}
		err, code := VerifyRequestSignature(verifier, "alice", testutils.SignatureFromTx(t, aliceSigner, payload), payload)
		require.EqualError(t, err, "signature verification failed")
		require.Equal(t, http.StatusUnauthorized, code)
	})

	t.Run("bad sig", func(t *testing.T) {
		db := &mocks.DB{}
		verifier := cryptoservice.NewVerifier(db, lg)
		db.On("GetCertificate", "alice").Return(aliceCert, nil)
		payload := &types.UserAdministrationTx{UserID: "alice", TxID: "xxx"}
		err, code := VerifyRequestSignature(verifier, "alice", []byte("bad-sig"), payload)
		require.EqualError(t, err, "signature verification failed")
		require.Equal(t, http.StatusUnauthorized, code)
	})

	t.Run("internal error", func(t *testing.T) {
		db := &mocks.DB{}
		verifier := cryptoservice.NewVerifier(db, lg)
		payload := make(chan struct{})
		err, code := VerifyRequestSignature(verifier, "alice", []byte("something"), payload)
		require.EqualError(t, err, "failure during json.Marshal: json: unsupported type: chan struct {}")
		require.Equal(t, http.StatusInternalServerError, code)
	})
}

var correctTxRespEnv *types.ResponseEnvelope

func init() {
	correctTxRespEnv = &types.ResponseEnvelope{
		Payload: MarshalOrPanic(&types.Payload{
			Header: &types.ResponseHeader{
				NodeID: "node1",
			},
			Response: MarshalOrPanic(&types.TxResponse{
				Receipt: &types.TxReceipt{
					Header: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
					},
					TxIndex: 1,
				},
			}),
		}),
	}
}
