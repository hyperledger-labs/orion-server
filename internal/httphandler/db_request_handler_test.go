// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/internal/bcdb"
	"github.com/hyperledger-labs/orion-server/internal/bcdb/mocks"
	interrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/marshal"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

func TestDBRequestHandler_DBStatus(t *testing.T) {
	submittingUserName := "alice"
	dbName := "testDBName"

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestCrypto(t, cryptoDir, "bob")

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
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBStatusQuery{UserId: submittingUserName, DbName: dbName})
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
				Response: &types.GetDBStatusResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
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
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBStatusQuery{UserId: submittingUserName, DbName: dbName})
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
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBStatusQuery{UserId: submittingUserName, DbName: dbName})
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
				sig := testutils.SignatureFromQuery(t, bobSigner, &types.GetDBStatusQuery{UserId: submittingUserName, DbName: dbName})
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
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBStatusQuery{UserId: submittingUserName, DbName: dbName})
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
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				requestBody, err := ioutil.ReadAll(rr.Body)
				require.NoError(t, err)
				res := &types.GetDBStatusResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}

func TestDBRequestHandler_DBIndex(t *testing.T) {
	submittingUserName := "alice"
	dbName := "testDBName"

	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestCrypto(t, cryptoDir, "bob")

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetDBIndexResponseEnvelope) bcdb.DB
		expectedResponse   *types.GetDBIndexResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid dbIndex request",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBIndex(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBIndexQuery{UserId: submittingUserName, DbName: dbName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDBIndexResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDBIndex", dbName, submittingUserName).Return(response, nil)
				return db
			},
			expectedResponse: &types.GetDBIndexResponseEnvelope{
				Response:  &types.GetDBIndexResponse{Header: &types.ResponseHeader{NodeId: "testNodeID"}, Index: `{"field1": "value1", "field2": "value2"}`},
				Signature: []byte{0, 0, 0},
			},
			expectedStatusCode: http.StatusOK,
			expectedErr:        "",
		},
		{
			name: "submitting user is not eligible to query the db index",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBIndex(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBIndexQuery{UserId: submittingUserName, DbName: dbName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDBIndexResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDBIndex", dbName, submittingUserName).Return(nil, &interrors.PermissionErr{ErrMsg: "the submitting user [alice] does not have the read permission on db [testDBName]"})
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusForbidden,
			expectedErr:        "error while processing 'GET /db/index/testDBName' because the submitting user [alice] does not have the read permission on db [testDBName]",
		},
		{
			name: "invalid dbIndex request missing user header",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBIndex(dbName), nil)
				if err != nil {
					return nil, err
				}
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBIndexQuery{UserId: submittingUserName, DbName: dbName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.GetDBIndexResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "UserID is not set in the http request header",
		},
		{
			name: "invalid dbIndex request missing user's signature",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBIndex(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)

				return req, nil
			},
			dbMockFactory: func(response *types.GetDBIndexResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "Signature is not set in the http request header",
		},
		{
			name: "invalid dbIndex request, submitting user doesn't exists",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBIndex(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBIndexQuery{UserId: submittingUserName, DbName: dbName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.GetDBIndexResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "invalid dbIndex request, failed to verify signature",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBIndex(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, bobSigner, &types.GetDBIndexQuery{UserId: submittingUserName, DbName: dbName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.GetDBIndexResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "invalid dbIndex request, failed to get db index",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBIndex(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDBIndexQuery{UserId: submittingUserName, DbName: dbName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.GetDBIndexResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDBIndex", dbName, submittingUserName).Return(nil, errors.New("failed to retrieve db index"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /db/index/testDBName' because failed to retrieve db index",
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
				requestBody, err := ioutil.ReadAll(rr.Body)
				require.NoError(t, err)
				res := &types.GetDBIndexResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}

func TestDBRequestHandler_DBTransaction(t *testing.T) {
	userID := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	dbTx := &types.DBAdministrationTx{
		TxId:      "1",
		UserId:    userID,
		CreateDbs: []string{"newDB"},
		DeleteDbs: []string{"dbToDelete"},
	}

	aliceSig := testutils.SignatureFromTx(t, aliceSigner, dbTx)

	testCases := []struct {
		name                    string
		txEnvFactory            func() *types.DBAdministrationTxEnvelope
		txRespFactory           func() *types.TxReceiptResponseEnvelope
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
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
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
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
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
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
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
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
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
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
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
				tx.UserId = ""
				return &types.DBAdministrationTxEnvelope{Payload: tx, Signature: aliceSig}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
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
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
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
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
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
				tx.UserId = "not-alice"
				return &types.DBAdministrationTxEnvelope{
					Payload:   tx,
					Signature: aliceSig,
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
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
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
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
		{
			name: "not a leader",
			txEnvFactory: func() *types.DBAdministrationTxEnvelope {
				return &types.DBAdministrationTxEnvelope{
					Payload:   dbTx,
					Signature: aliceSig,
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return correctTxRespEnv
			},
			createMockAndInstrument: func(t *testing.T, dbTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", userID).Return(aliceCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).Return(nil, &interrors.NotLeaderError{
					LeaderID:       3,
					LeaderHostPort: "server3.example.com:6091",
				})
				return db
			},
			timeoutStr:   "1s",
			expectedCode: http.StatusTemporaryRedirect,
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			txEnv := tt.txEnvFactory()
			txBytes, err := marshal.DefaultMarshaler().Marshal(txEnv)
			require.NoError(t, err)
			require.NotNil(t, txBytes)

			txReader := bytes.NewReader(txBytes)
			require.NotNil(t, txReader)

			txResp := tt.txRespFactory()

			reqUrl := &url.URL{
				Scheme: "http",
				Host:   "server1.example.com:6091",
				Path:   constants.PostDBTx,
			}
			req, err := http.NewRequest(http.MethodPost, reqUrl.String(), txReader)
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
				requestBody, err := ioutil.ReadAll(rr.Body)
				require.NoError(t, err)
				resp := &types.TxReceiptResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, resp))
				require.Equal(t, txResp, resp)
			} else if tt.expectedCode == http.StatusTemporaryRedirect {
				locationUrl := rr.Header().Get("Location")
				require.Equal(t, "http://server3.example.com:6091/db/tx", locationUrl)
			} else {
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}
		})
	}
}
