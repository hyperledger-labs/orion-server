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

func TestDataRequestHandler_DataQuery(t *testing.T) {
	submittingUserName := "admin"
	dbName := "test_database"

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetDataResponseEnvelope) backend.DB
		expectedResponse   *types.GetDataResponseEnvelope
		expectedStatusCode int
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
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(true, nil)
				db.On("GetData", dbName, submittingUserName, "foo").
					Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "submitting user is not eligible to update the key",
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
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(true, nil)
				db.On("GetData", dbName, submittingUserName, "foo").
					Return(nil, &backend.PermissionErr{ErrMsg: "access forbidden"})
				return db
			},
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name: "failed to store data transaction update",
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
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(true, nil)
				db.On("GetData", dbName, submittingUserName, "foo").
					Return(nil, errors.New("failed to store transaction"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
		},
		{
			name:             "querier user doesn't exist",
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
				db.On("DoesUserExist", submittingUserName).
					Return(false, nil)
				return db
			},
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name:             "fail to retrieve querier record",
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
				db.On("DoesUserExist", submittingUserName).
					Return(false, errors.New("fail to read from database"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
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
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req, err := tt.requestFactory()
			require.NoError(t, err)
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)
			rr := httptest.NewRecorder()
			handler := NewDataRequestHandler(db)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)

			if tt.expectedResponse != nil {
				res := &types.GetDataResponseEnvelope{}
				err = json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)
				require.Equal(t, tt.expectedResponse, res)
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
							ACL: &types.AccessControl{
							},
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
			handler := NewDataRequestHandler(db)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedCode, rr.Code)
		})
	}
}
