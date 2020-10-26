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
)

func TestDBRequestHandler_DBStatus(t *testing.T) {
	submittingUserName := "admin"
	dbName := "testDBName"

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetDBStatusResponseEnvelope) backend.DB
		expectedResponse   *types.GetDBStatusResponseEnvelope
		expectedStatusCode int
	}{
		{
			name: "valid dbStatus request",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBStatus(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))

				return req, nil
			},
			dbMockFactory: func(response *types.GetDBStatusResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(true, nil)
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
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))

				return req, nil
			},
			dbMockFactory: func(response *types.GetDBStatusResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(true, nil)
				db.On("GetDBStatus", dbName).Return(response, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
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
			dbMockFactory: func(response *types.GetDBStatusResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(true, nil)
				db.On("GetDBStatus", dbName).Return(response, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "invalid dbStatus request, submitting user doesn't exists",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBStatus(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))

				return req, nil
			},
			dbMockFactory: func(response *types.GetDBStatusResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(false, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name: "invalid dbStatus request, cannot retrieve submitting user record",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBStatus(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))

				return req, nil
			},
			dbMockFactory: func(response *types.GetDBStatusResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(false, errors.New("failed to get user's record"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
		},
		{
			name: "invalid dbStatus request, failed to get db status",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDBStatus(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))

				return req, nil
			},
			dbMockFactory: func(response *types.GetDBStatusResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(true, nil)
				db.On("GetDBStatus", dbName).Return(nil, errors.New("failed to retrieve db status"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			req, err := tt.requestFactory()
			require.NoError(t, err)
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)
			handler := NewDBRequestHandler(db)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)

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
	userID := "testUserID"
	testCases := []struct {
		name                    string
		dataTx                  interface{}
		createMockAndInstrument func(t *testing.T, dataTx interface{}) backend.DB
		expectedCode            int
	}{
		{
			name: "submit valid db transaction",
			dataTx: &types.DBAdministrationTxEnvelope{
				Payload: &types.DBAdministrationTx{
					TxID:      "1",
					UserID:    userID,
					CreateDBs: []string{"newDB"},
					DeleteDBs: []string{"dbToDelete"},
				},
				Signature: []byte{0, 0, 0},
			},
			createMockAndInstrument: func(t *testing.T, dataTx interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", userID).Return(true, nil)
				db.On("SubmitTransaction", mock.Anything).Run(func(args mock.Arguments) {
					tx, ok := args[0].(*types.DBAdministrationTxEnvelope)
					require.True(t, ok)
					require.Equal(t, dataTx, tx)
				}).Return(nil)
				return db
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "submit configuration with missing userID",
			dataTx: &types.DBAdministrationTxEnvelope{
				Payload: &types.DBAdministrationTx{
					TxID:      "1",
					CreateDBs: []string{"newDB"},
					DeleteDBs: []string{"dbToDelete"},
				},
				Signature: []byte{0, 0, 0},
			},
			createMockAndInstrument: func(t *testing.T, dataTx interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", userID).Return(false, nil)
				return db
			},
			expectedCode: http.StatusForbidden,
		},
		{
			name: "fail to retrieve information about submitting user",
			dataTx: &types.DBAdministrationTxEnvelope{
				Payload: &types.DBAdministrationTx{
					TxID:      "1",
					CreateDBs: []string{"newDB"},
					DeleteDBs: []string{"dbToDelete"},
				},
				Signature: []byte{0, 0, 0},
			},
			createMockAndInstrument: func(t *testing.T, dataTx interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", userID).Return(false, errors.New("fail to retrieve user's record"))
				return db
			},
			expectedCode: http.StatusBadRequest,
		},
		{
			name:   "fail to submit transaction",
			dataTx: &types.DBAdministrationTxEnvelope{},
			createMockAndInstrument: func(t *testing.T, dataTx interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", mock.Anything).Return(true, nil)
				db.On("SubmitTransaction", mock.Anything).Return(errors.New("failed to submit transactions"))
				return db
			},
			expectedCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			txBytes, err := json.Marshal(tt.dataTx)
			require.NoError(t, err)
			require.NotNil(t, txBytes)

			txReader := bytes.NewReader(txBytes)
			require.NotNil(t, txReader)

			req, err := http.NewRequest(http.MethodPost, constants.PostDBTx, txReader)
			require.NoError(t, err)
			require.NotNil(t, req)

			rr := httptest.NewRecorder()

			db := tt.createMockAndInstrument(t, tt.dataTx)
			handler := NewDBRequestHandler(db)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedCode, rr.Code)
		})
	}
}
