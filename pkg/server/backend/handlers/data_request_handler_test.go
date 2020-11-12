package handlers

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
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
	"github.ibm.com/blockchaindb/server/pkg/server/backend/mocks"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
)

func TestDataRequestHandler_DataQuery(t *testing.T) {
	dbName := "test_database"

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "bob")

	sigFoo := signatureFromQuery(t, aliceSigner, &types.GetDataQuery{
		UserID: submittingUserName,
		DBName: dbName,
		Key:    "foo",
	})

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetDataResponseEnvelope) backend.DB
		expectedResponse   *types.GetDataResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get data request",
			expectedResponse: &types.GetDataResponseEnvelope{
				Signature: []byte{0, 0, 0},
				Payload: &types.GetDataResponse{
					Header: &types.ResponseHeader{
						NodeID: "testNodeID",
					},
					Value: []byte("bar"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							TxNum:    1,
							BlockNum: 1,
						},
					},
				},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetData(dbName, "foo"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetData", dbName, submittingUserName, "foo").Return(response, nil)
				db.On("IsDBExists", dbName).Return(true)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "submitting user is not eligible to update the key",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetData(dbName, "foo"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(true)
				db.On("GetData", dbName, submittingUserName, "foo").
					Return(nil, &backend.PermissionErr{ErrMsg: "access forbidden"})
				return db
			},
			expectedStatusCode: http.StatusForbidden,
			expectedErr:        "error while processing 'GET /data/test_database/foo' because access forbidden",
		},
		{
			name: "failed to get data",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetData(dbName, "foo"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(true)
				db.On("GetData", dbName, submittingUserName, "foo").
					Return(nil, errors.New("failed to get data"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /data/test_database/foo' because failed to get data",
		},
		{
			name:             "user doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetData(dbName, "foo"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name:             "fail to verify signature",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetData(dbName, "foo"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sigFooBob := signatureFromQuery(t, bobSigner, &types.GetDataQuery{
					UserID: submittingUserName,
					DBName: dbName,
					Key:    "foo",
				})

				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFooBob))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name:             "missing userID http header",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetData(dbName, "foo"), nil)
				if err != nil {
					return nil, err
				}
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataResponseEnvelope) backend.DB {
				return &mocks.DB{}
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "UserID is not set in the http request header",
		},
		{
			name:             "missing signature http header",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetData(dbName, "foo"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)

				return req, nil
			},
			dbMockFactory: func(response *types.GetDataResponseEnvelope) backend.DB {
				return &mocks.DB{}
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "Signature is not set in the http request header",
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
			rr := httptest.NewRecorder()
			handler := NewDataRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)
			if tt.expectedStatusCode != http.StatusOK {
				respErr := &ResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				res := &types.GetDataResponseEnvelope{}
				err = json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)
				require.Equal(t, tt.expectedResponse, res)
				//TODO verify signature on response
			}
		})
	}
}

func TestDataRequestHandler_DataTransaction(t *testing.T) {
	userID := "testUserID"
	testCases := []struct {
		name                    string
		dataTx                  interface{}
		createMockAndInstrument func(t *testing.T, dataTx interface{}) backend.DB
		expectedCode            int
	}{
		{
			name: "submit valid data transaction",
			dataTx: &types.DataTxEnvelope{
				Payload: &types.DataTx{
					UserID: userID,
					TxID:   "1",
					DBName: "testDB",
					DataDeletes: []*types.DataDelete{
						{
							Key: "foo",
						},
					},
					DataReads: []*types.DataRead{
						{
							Key: "bar",
							Version: &types.Version{
								TxNum:    1,
								BlockNum: 1,
							},
						},
					},
					DataWrites: []*types.DataWrite{
						{
							Key:   "xxx",
							Value: []byte("yyy"),
							ACL:   &types.AccessControl{},
						},
					},
				},
				Signature: []byte{0, 0, 0},
			},
			createMockAndInstrument: func(t *testing.T, dataTx interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", userID).Return(true, nil)
				db.On("SubmitTransaction", mock.Anything).
					Run(func(args mock.Arguments) {
						tx := args[0].(*types.DataTxEnvelope)
						require.Equal(t, dataTx, tx)
					}).
					Return(nil)
				return db
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "submit valid data transaction",
			dataTx: &types.DataTxEnvelope{
				Payload: &types.DataTx{
					UserID: userID,
					TxID:   "1",
					DBName: "testDB",
				},
				Signature: []byte{0, 0, 0},
			},
			createMockAndInstrument: func(t *testing.T, dataTx interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", userID).Return(true, nil)
				db.On("SubmitTransaction", mock.Anything).Return(errors.New("failed to store data transaction"))
				return db
			},
			expectedCode: http.StatusInternalServerError,
		},
		{
			name:   "failed to retrieve submitting user id",
			dataTx: nil,
			createMockAndInstrument: func(t *testing.T, dataTx interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", mock.Anything).Return(false, errors.New("fail to read from database"))
				return db
			},
			expectedCode: http.StatusBadRequest,
		},
		{
			name:   "submitting user doesn't exist",
			dataTx: nil,
			createMockAndInstrument: func(t *testing.T, dataTx interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", mock.Anything).Return(false, nil)
				return db
			},
			expectedCode: http.StatusForbidden,
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			txBytes, err := json.Marshal(tt.dataTx)
			require.NoError(t, err)
			require.NotNil(t, txBytes)

			txReader := bytes.NewReader(txBytes)
			require.NotNil(t, txReader)

			req, err := http.NewRequest(http.MethodPost, constants.PostDataTx, txReader)
			require.NoError(t, err)
			require.NotNil(t, req)

			rr := httptest.NewRecorder()

			db := tt.createMockAndInstrument(t, tt.dataTx)
			handler := NewDataRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedCode, rr.Code)
		})
	}
}
