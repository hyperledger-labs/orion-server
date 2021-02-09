package httphandler

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	interrors "github.ibm.com/blockchaindb/server/internal/errors"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/bcdb"
	"github.ibm.com/blockchaindb/server/internal/bcdb/mocks"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

func TestDBRequestHandler_DBStatus(t *testing.T) {
	submittingUserName := "alice"
	dbName := "testDBName"

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "bob")

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.ResponseEnvelope) bcdb.DB
		expectedResponse   *types.ResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid dbStatus request",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBStatus(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBStatusQuery{UserID: submittingUserName, DBName: dbName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDBStatus", dbName).Return(response, nil)
				return db
			},
			expectedResponse: &types.ResponseEnvelope{
				Payload: MarshalOrPanic(&types.Payload{
					Header: &types.ResponseHeader{
						NodeID: "testNodeID",
					},
					Response: MarshalOrPanic(&types.GetDBStatusResponse{
						Exist: true,
					}),
				}),
				Signature: []byte{0, 0, 0},
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "invalid dbStatus request missing user header",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBStatus(dbName), nil)
				if err != nil {
					return nil, err
				}
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBStatusQuery{UserID: submittingUserName, DBName: dbName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "UserID is not set in the http request header",
		},
		{
			name: "invalid dbStatus request missing user's signature",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBStatus(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)

				return req, nil
			},
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "Signature is not set in the http request header",
		},
		{
			name: "invalid dbStatus request, submitting user doesn't exists",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBStatus(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBStatusQuery{UserID: submittingUserName, DBName: dbName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "invalid dbStatus request, failed to verify signature",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBStatus(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, bobSigner, &types.GetDBStatusQuery{UserID: submittingUserName, DBName: dbName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "invalid dbStatus request, failed to get db status",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBStatus(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBStatusQuery{UserID: submittingUserName, DBName: dbName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDBStatus", dbName).Return(nil, errors.New("failed to retrieve db status"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /db/testDBName' because failed to retrieve db status",
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			req, err := tt.requestFactory()
			require.NoError(t, err)
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)
			handler := NewDBRequestHandler(db, logger)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)
			if tt.expectedStatusCode != http.StatusOK {
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				res := &types.ResponseEnvelope{}
				err := json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)

				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}

func TestDBRequestHandler_DBTransaction(t *testing.T) {
	userID := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	dbTx := &types.DBAdministrationTx{
		TxID:      "1",
		UserID:    userID,
		CreateDBs: []string{"newDB"},
		DeleteDBs: []string{"dbToDelete"},
	}

	aliceSig := testutils.SignatureFromTx(t, aliceSigner, dbTx)

	testCases := []struct {
		name                    string
		txEnvFactory            func() *types.DBAdministrationTxEnvelope
		txRespFactory           func() *types.ResponseEnvelope
		createMockAndInstrument func(t *testing.T, dbTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB
		timeoutStr              string
		expectedCode            int
		expectedErr             string
	}{
		{
			name: "submit valid dbAdmin transaction",
			txEnvFactory: func() *types.DBAdministrationTxEnvelope {
				return &types.DBAdministrationTxEnvelope{
					Payload:   dbTx,
					Signature: aliceSig,
				}
			},
			txRespFactory: func() *types.ResponseEnvelope {
				return correctTxRespEnv
			},
			createMockAndInstrument: func(t *testing.T, dbTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", userID).Return(aliceCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
					tx, ok := args[0].(*types.DBAdministrationTxEnvelope)
					require.True(t, ok)
					require.Equal(t, dbTxEnv, tx)
					require.Equal(t, timeout, args[1].(time.Duration))
				}).Return(txRespEnv, nil)
				return db
			},
			timeoutStr:   "1s",
			expectedCode: http.StatusOK,
		},
		{
			name: "transaction timeout",
			txEnvFactory: func() *types.DBAdministrationTxEnvelope {
				return &types.DBAdministrationTxEnvelope{
					Payload:   dbTx,
					Signature: aliceSig,
				}
			},
			txRespFactory: func() *types.ResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dbTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", userID).Return(aliceCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).
					Run(func(args mock.Arguments) {
						tx := args[0].(*types.DBAdministrationTxEnvelope)
						require.Equal(t, dbTxEnv, tx)
						require.Equal(t, timeout, args[1].(time.Duration))
					}).
					Return(txRespEnv, &interrors.TimeoutErr{ErrMsg: "Timeout error"})
				return db
			},
			timeoutStr:   "1s",
			expectedCode: http.StatusAccepted,
			expectedErr:  "Transaction processing timeout",
		},
		{
			name: "transaction timeout invalid",
			txEnvFactory: func() *types.DBAdministrationTxEnvelope {
				return &types.DBAdministrationTxEnvelope{
					Payload:   dbTx,
					Signature: aliceSig,
				}
			},
			txRespFactory: func() *types.ResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dbTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			timeoutStr:   "asdf",
			expectedCode: http.StatusBadRequest,
			expectedErr:  "time: invalid duration \"asdf\"",
		},
		{
			name: "transaction timeout negative",
			txEnvFactory: func() *types.DBAdministrationTxEnvelope {
				return &types.DBAdministrationTxEnvelope{
					Payload:   dbTx,
					Signature: aliceSig,
				}
			},
			txRespFactory: func() *types.ResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dbTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			timeoutStr:   "-2s",
			expectedCode: http.StatusBadRequest,
			expectedErr:  "timeout can't be negative \"-2s\"",
		},
		{
			name: "submit dbAdmin tx with missing payload",
			txEnvFactory: func() *types.DBAdministrationTxEnvelope {
				return &types.DBAdministrationTxEnvelope{Payload: nil, Signature: aliceSig}
			},
			txRespFactory: func() *types.ResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dbTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing transaction envelope payload (*types.DBAdministrationTx)",
		},
		{
			name: "submit dbAdmin tx with missing userID",
			txEnvFactory: func() *types.DBAdministrationTxEnvelope {
				tx := &types.DBAdministrationTx{}
				*tx = *dbTx
				tx.UserID = ""
				return &types.DBAdministrationTxEnvelope{Payload: tx, Signature: aliceSig}
			},
			txRespFactory: func() *types.ResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dbTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing UserID in transaction envelope payload (*types.DBAdministrationTx)",
		},
		{
			name: "submit dbAdmin tx with missing signature",
			txEnvFactory: func() *types.DBAdministrationTxEnvelope {
				return &types.DBAdministrationTxEnvelope{Payload: dbTx, Signature: nil}
			},
			txRespFactory: func() *types.ResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dbTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing Signature in transaction envelope payload (*types.DBAdministrationTx)",
		},
		{
			name: "bad signature",
			txEnvFactory: func() *types.DBAdministrationTxEnvelope {
				return &types.DBAdministrationTxEnvelope{
					Payload:   dbTx,
					Signature: []byte("bad-sig"),
				}
			},
			txRespFactory: func() *types.ResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dbTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", userID).Return(aliceCert, nil)

				return db
			},
			expectedCode: http.StatusUnauthorized,
			expectedErr:  "signature verification failed",
		},
		{
			name: "no such user",
			txEnvFactory: func() *types.DBAdministrationTxEnvelope {
				tx := &types.DBAdministrationTx{}
				*tx = *dbTx
				tx.UserID = "not-alice"
				return &types.DBAdministrationTxEnvelope{
					Payload:   tx,
					Signature: aliceSig,
				}
			},
			txRespFactory: func() *types.ResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dbTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", "not-alice").Return(nil, errors.New("no such user"))

				return db
			},
			expectedCode: http.StatusUnauthorized,
			expectedErr:  "signature verification failed",
		},
		{
			name: "fail to submit transaction",
			txEnvFactory: func() *types.DBAdministrationTxEnvelope {
				return &types.DBAdministrationTxEnvelope{
					Payload:   dbTx,
					Signature: aliceSig,
				}
			},
			txRespFactory: func() *types.ResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dbTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", userID).Return(aliceCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).Return(nil, errors.New("oops, submission failed"))

				return db
			},
			expectedCode: http.StatusInternalServerError,
			expectedErr:  "oops, submission failed",
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			txEnv := tt.txEnvFactory()
			txBytes, err := json.Marshal(txEnv)
			txResp := tt.txRespFactory()
			require.NoError(t, err)
			require.NotNil(t, txBytes)

			txReader := bytes.NewReader(txBytes)
			require.NotNil(t, txReader)

			req, err := http.NewRequest(http.MethodPost, constants.PostDBTx, txReader)
			require.NoError(t, err)
			require.NotNil(t, req)

			rr := httptest.NewRecorder()

			var timeout time.Duration
			timeout = 0
			if len(tt.timeoutStr) != 0 {
				req.Header.Set(constants.TimeoutHeader, tt.timeoutStr)
				timeout, err = time.ParseDuration(tt.timeoutStr)
				if err != nil {
					timeout = 0
				}
				if timeout < 0 {
					timeout = 0
				}
			}

			db := tt.createMockAndInstrument(t, txEnv, txResp, timeout)
			handler := NewDBRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedCode, rr.Code)
			if tt.expectedCode == http.StatusOK {
				resp := &types.ResponseEnvelope{}
				err := json.NewDecoder(rr.Body).Decode(resp)
				require.NoError(t, err)
				require.Equal(t, txResp, resp)

			} else {
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}
		})
	}
}
