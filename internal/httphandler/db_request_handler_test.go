package httphandler

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

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
		dbMockFactory      func(response *types.GetDBStatusResponseEnvelope) bcdb.DB
		expectedResponse   *types.GetDBStatusResponseEnvelope
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
			dbMockFactory: func(response *types.GetDBStatusResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDBStatus", dbName).Return(response, nil)
				return db
			},
			expectedResponse: &types.GetDBStatusResponseEnvelope{
				Payload: &types.GetDBStatusResponse{
					Header: &types.ResponseHeader{
						NodeID: "testNodeID",
					},
					Exist: true,
				},
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
			dbMockFactory: func(response *types.GetDBStatusResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.GetDBStatusResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.GetDBStatusResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.GetDBStatusResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.GetDBStatusResponseEnvelope) bcdb.DB {
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
				respErr := &ResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				res := &types.GetDBStatusResponseEnvelope{}
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
		createMockAndInstrument func(t *testing.T, dataTxEnv interface{}) bcdb.DB
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
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", userID).Return(aliceCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
					tx, ok := args[0].(*types.DBAdministrationTxEnvelope)
					require.True(t, ok)
					require.Equal(t, dataTxEnv, tx)
				}).Return(nil, nil)
				return db
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "submit dbAdmin tx with missing payload",
			txEnvFactory: func() *types.DBAdministrationTxEnvelope {
				return &types.DBAdministrationTxEnvelope{Payload: nil, Signature: aliceSig}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
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
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
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
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
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
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
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
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
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
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
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
			require.NoError(t, err)
			require.NotNil(t, txBytes)

			txReader := bytes.NewReader(txBytes)
			require.NotNil(t, txReader)

			req, err := http.NewRequest(http.MethodPost, constants.PostDBTx, txReader)
			require.NoError(t, err)
			require.NotNil(t, req)

			rr := httptest.NewRecorder()

			db := tt.createMockAndInstrument(t, txEnv)
			handler := NewDBRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedCode, rr.Code)
			if tt.expectedCode != http.StatusOK {
				respErr := &ResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}
		})
	}
}
