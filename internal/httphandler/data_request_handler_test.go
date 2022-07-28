// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"bytes"
	"context"
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

func TestDataRequestHandler_DataQuery(t *testing.T) {
	dbName := "test_database"

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestCrypto(t, cryptoDir, "bob")

	sigFoo := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDataQuery{
		UserId: submittingUserName,
		DbName: dbName,
		Key:    "foo",
	})

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetDataResponseEnvelope) bcdb.DB
		expectedResponse   *types.GetDataResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get data request",
			expectedResponse: &types.GetDataResponseEnvelope{
				Response: &types.GetDataResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					Value: []byte("bar"),
					Metadata: &types.Metadata{
						Version: &types.Version{
							TxNum:    1,
							BlockNum: 1,
						},
					},
				},
				Signature: []byte{0, 0, 0},
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
			dbMockFactory: func(response *types.GetDataResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.GetDataResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(true)
				db.On("GetData", dbName, submittingUserName, "foo").Return(nil, &interrors.PermissionErr{ErrMsg: "access forbidden"})
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
			dbMockFactory: func(response *types.GetDataResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.GetDataResponseEnvelope) bcdb.DB {
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
				sigFooBob := testutils.SignatureFromQuery(t, bobSigner, &types.GetDataQuery{
					UserId: submittingUserName,
					DbName: dbName,
					Key:    "foo",
				})

				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFooBob))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.GetDataResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.GetDataResponseEnvelope) bcdb.DB {
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
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				requestBody, err := ioutil.ReadAll(rr.Body)
				require.NoError(t, err)
				res := &types.GetDataResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
				//TODO verify signature on response
			}
		})
	}
}

func TestDataRequestHandler_DataRangeQuery(t *testing.T) {
	dbName := "test_database"

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestCrypto(t, cryptoDir, "bob")

	sigFoo := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDataRangeQuery{
		UserId:   submittingUserName,
		DbName:   dbName,
		StartKey: "key1",
		EndKey:   "key10",
		Limit:    10,
	})

	sigFooNoLimits := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDataRangeQuery{
		UserId:   submittingUserName,
		DbName:   dbName,
		StartKey: "key1",
		EndKey:   "key10",
		Limit:    0,
	})

	sigFooWithEmptyStartKey := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDataRangeQuery{
		UserId:   submittingUserName,
		DbName:   dbName,
		StartKey: "",
		EndKey:   "key10",
		Limit:    10,
	})

	sigFooWithEmptyEndKey := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDataRangeQuery{
		UserId:   submittingUserName,
		DbName:   dbName,
		StartKey: "key1",
		EndKey:   "",
		Limit:    10,
	})

	sigFooWithEmptyStartEndKeys := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDataRangeQuery{
		UserId:   submittingUserName,
		DbName:   dbName,
		StartKey: "",
		EndKey:   "",
		Limit:    0,
	})

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetDataRangeResponseEnvelope) bcdb.DB
		expectedResponse   *types.GetDataRangeResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get data range with a limit",
			expectedResponse: &types.GetDataRangeResponseEnvelope{
				Response: &types.GetDataRangeResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key2",
							Value: []byte("value2"),
						},
					},
				},
				Signature: []byte{0, 0, 0},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDataRange(dbName, "key1", "key10", 10), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataRangeResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDataRange", dbName, submittingUserName, "key1", "key10", uint64(10)).Return(response, nil)
				db.On("IsDBExists", dbName).Return(true)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "valid get data range with a limit and empty start key",
			expectedResponse: &types.GetDataRangeResponseEnvelope{
				Response: &types.GetDataRangeResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key2",
							Value: []byte("value2"),
						},
					},
				},
				Signature: []byte{0, 0, 0},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDataRange(dbName, "", "key10", 10), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFooWithEmptyStartKey))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataRangeResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDataRange", dbName, submittingUserName, "", "key10", uint64(10)).Return(response, nil)
				db.On("IsDBExists", dbName).Return(true)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "valid get data range with a limit and empty end key",
			expectedResponse: &types.GetDataRangeResponseEnvelope{
				Response: &types.GetDataRangeResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key2",
							Value: []byte("value2"),
						},
					},
				},
				Signature: []byte{0, 0, 0},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDataRange(dbName, "key1", "", 10), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFooWithEmptyEndKey))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataRangeResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDataRange", dbName, submittingUserName, "key1", "", uint64(10)).Return(response, nil)
				db.On("IsDBExists", dbName).Return(true)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "valid get data range without a limit",
			expectedResponse: &types.GetDataRangeResponseEnvelope{
				Response: &types.GetDataRangeResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key2",
							Value: []byte("value2"),
						},
					},
				},
				Signature: []byte{0, 0, 0},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDataRange(dbName, "key1", "key10", 0), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFooNoLimits))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataRangeResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDataRange", dbName, submittingUserName, "key1", "key10", uint64(0)).Return(response, nil)
				db.On("IsDBExists", dbName).Return(true)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "valid get data range without a limit and empty start/end keys",
			expectedResponse: &types.GetDataRangeResponseEnvelope{
				Response: &types.GetDataRangeResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key2",
							Value: []byte("value2"),
						},
					},
				},
				Signature: []byte{0, 0, 0},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDataRange(dbName, "", "", 0), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFooWithEmptyStartEndKeys))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataRangeResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDataRange", dbName, submittingUserName, "", "", uint64(0)).Return(response, nil)
				db.On("IsDBExists", dbName).Return(true)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "submitting user is not eligible to read from the database",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDataRange(dbName, "key1", "key10", 10), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataRangeResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(true)
				db.On("GetDataRange", dbName, submittingUserName, "key1", "key10", uint64(10)).Return(nil, &interrors.PermissionErr{ErrMsg: "access forbidden"})
				return db
			},
			expectedStatusCode: http.StatusForbidden,
			expectedErr:        "error while processing 'GET /data/test_database?startkey=\"key1\"&endkey=\"key10\"&limit=10' because access forbidden",
		},
		{
			name: "failed to get data",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDataRange(dbName, "key1", "key10", 10), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataRangeResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(true)
				db.On("GetDataRange", dbName, submittingUserName, "key1", "key10", uint64(10)).
					Return(nil, errors.New("failed to get data"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /data/test_database?startkey=\"key1\"&endkey=\"key10\"&limit=10' because failed to get data",
		},
		{
			name:             "user doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDataRange(dbName, "key1", "key10", uint64(10)), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataRangeResponseEnvelope) bcdb.DB {
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
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDataRange(dbName, "key1", "key10", uint64(10)), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sigFooBob := testutils.SignatureFromQuery(t, bobSigner, &types.GetDataQuery{
					UserId: submittingUserName,
					DbName: dbName,
					Key:    "foo",
				})

				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFooBob))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataRangeResponseEnvelope) bcdb.DB {
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
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDataRange(dbName, "key1", "key10", uint64(10)), nil)
				if err != nil {
					return nil, err
				}
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataRangeResponseEnvelope) bcdb.DB {
				return &mocks.DB{}
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "UserID is not set in the http request header",
		},
		{
			name:             "missing signature http header",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetDataRange(dbName, "key1", "key10", uint64(10)), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)

				return req, nil
			},
			dbMockFactory: func(response *types.GetDataRangeResponseEnvelope) bcdb.DB {
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
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				requestBody, err := ioutil.ReadAll(rr.Body)
				require.NoError(t, err)
				res := &types.GetDataRangeResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
				//TODO verify signature on response
			}
		})
	}
}

func TestDataRequestHandler_DataJSONQuery(t *testing.T) {
	dbName := "test_database"

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")
	bobCert, _ := testutils.LoadTestCrypto(t, cryptoDir, "bob")

	q := `{"attr1":{"$eq":true}}`
	queryBytes, err := json.Marshal(q)
	require.NoError(t, err)
	require.NotNil(t, queryBytes)

	queryReader := bytes.NewReader(queryBytes)
	require.NotNil(t, queryReader)

	sigFoo := testutils.SignatureFromQuery(t, aliceSigner, &types.DataJSONQuery{
		UserId: submittingUserName,
		DbName: dbName,
		Query:  q,
	})

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.DataQueryResponseEnvelope) bcdb.DB
		expectedResponse   *types.DataQueryResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid json query",
			expectedResponse: &types.DataQueryResponseEnvelope{
				Response: &types.DataQueryResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key1",
							Value: []byte(`{"attr1":true}`),
						},
					},
				},
				Signature: []byte{0, 0, 0},
			},
			requestFactory: func() (*http.Request, error) {
				queryReader := bytes.NewReader(queryBytes)
				require.NotNil(t, queryReader)
				req, err := http.NewRequest(http.MethodPost, constants.URLForJSONQuery(dbName), queryReader)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.DataQueryResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(true)
				db.On("DataQuery", mock.Anything, dbName, submittingUserName, []byte(q)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "database does not exist",
			requestFactory: func() (*http.Request, error) {
				queryReader := bytes.NewReader(queryBytes)
				require.NotNil(t, queryReader)
				req, err := http.NewRequest(http.MethodPost, constants.URLForJSONQuery(dbName), queryReader)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.DataQueryResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(false)
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "'test_database' does not exist",
		},
		{
			name: "submitting user is not eligible to query the database",
			requestFactory: func() (*http.Request, error) {
				queryReader := bytes.NewReader(queryBytes)
				require.NotNil(t, queryReader)
				req, err := http.NewRequest(http.MethodPost, constants.URLForJSONQuery(dbName), queryReader)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.DataQueryResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(true)
				db.On("DataQuery", mock.Anything, dbName, submittingUserName, []byte(q)).
					Return(nil, &interrors.PermissionErr{ErrMsg: "access forbidden"})
				return db
			},
			expectedStatusCode: http.StatusForbidden,
			expectedErr:        "error while processing 'POST /data/test_database/jsonquery' because access forbidden",
		},
		{
			name: "failed to execute the query",
			requestFactory: func() (*http.Request, error) {
				queryReader := bytes.NewReader(queryBytes)
				require.NotNil(t, queryReader)
				req, err := http.NewRequest(http.MethodPost, constants.URLForJSONQuery(dbName), queryReader)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.DataQueryResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(true)
				db.On("DataQuery", mock.Anything, dbName, submittingUserName, []byte(q)).
					Return(nil, errors.New("failed to execute the query"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'POST /data/test_database/jsonquery' because failed to execute the query",
		},
		{
			name: "user does not exist",
			requestFactory: func() (*http.Request, error) {
				queryReader := bytes.NewReader(queryBytes)
				require.NotNil(t, queryReader)
				req, err := http.NewRequest(http.MethodPost, constants.URLForJSONQuery(dbName), queryReader)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.DataQueryResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "failed to verify signature",
			requestFactory: func() (*http.Request, error) {
				queryReader := bytes.NewReader(queryBytes)
				require.NotNil(t, queryReader)
				req, err := http.NewRequest(http.MethodPost, constants.URLForJSONQuery(dbName), queryReader)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.DataQueryResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(bobCert, nil)
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "missing userID http header",
			requestFactory: func() (*http.Request, error) {
				queryReader := bytes.NewReader(queryBytes)
				require.NotNil(t, queryReader)
				req, err := http.NewRequest(http.MethodPost, constants.URLForJSONQuery(dbName), queryReader)
				if err != nil {
					return nil, err
				}
				return req, nil
			},
			dbMockFactory: func(response *types.DataQueryResponseEnvelope) bcdb.DB {
				return &mocks.DB{}
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "UserID is not set in the http request header",
		},
		{
			name: "missing signature http header",
			requestFactory: func() (*http.Request, error) {
				queryReader := bytes.NewReader(queryBytes)
				require.NotNil(t, queryReader)
				req, err := http.NewRequest(http.MethodPost, constants.URLForJSONQuery(dbName), queryReader)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				return req, nil
			},
			dbMockFactory: func(response *types.DataQueryResponseEnvelope) bcdb.DB {
				return &mocks.DB{}
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "Signature is not set in the http request header",
		},
		{
			name: "empty query",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodPost, constants.URLForJSONQuery(dbName), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.DataQueryResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(true)
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "query is empty",
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
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				requestBody, err := ioutil.ReadAll(rr.Body)
				require.NoError(t, err)
				res := &types.DataQueryResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
				//TODO verify signature on response
			}
		})
	}
}

func TestDataRequestHandler_DataTransaction(t *testing.T) {
	alice := "alice"
	bob := "bob"
	charlie := "charlie"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice", "bob", "charlie"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")
	bobCert, bobSigner := testutils.LoadTestCrypto(t, cryptoDir, "bob")
	_, charlieSigner := testutils.LoadTestCrypto(t, cryptoDir, "charlie")

	dataTx := &types.DataTx{
		MustSignUserIds: []string{alice, bob},
		TxId:            "1",
		DbOperations: []*types.DBOperation{
			{
				DbName: "testDB",
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
						Acl:   &types.AccessControl{},
					},
				},
			},
		},
	}
	aliceSig := testutils.SignatureFromTx(t, aliceSigner, dataTx)
	bobSig := testutils.SignatureFromTx(t, bobSigner, dataTx)
	charlieSig := testutils.SignatureFromTx(t, charlieSigner, dataTx)
	testCases := []struct {
		name                    string
		txEnvFactory            func() *types.DataTxEnvelope
		txRespFactory           func() *types.TxReceiptResponseEnvelope
		createMockAndInstrument func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB
		timeoutStr              string
		expectedCode            int
		expectedErr             string
	}{
		{
			name: "submit valid data transaction",
			txEnvFactory: func() *types.DataTxEnvelope {
				return &types.DataTxEnvelope{
					Payload: dataTx,
					Signatures: map[string][]byte{
						alice:   aliceSig,
						bob:     bobSig,
						charlie: charlieSig,
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return correctTxRespEnv
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", alice).Return(aliceCert, nil)
				db.On("GetCertificate", bob).Return(bobCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).
					Run(func(args mock.Arguments) {
						tx := args[0].(*types.DataTxEnvelope)
						require.Equal(t, dataTxEnv, tx)
						require.Equal(t, timeout, args[1].(time.Duration))
					}).
					Return(txRespEnv, nil)
				return db
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "transaction timeout",
			txEnvFactory: func() *types.DataTxEnvelope {
				return &types.DataTxEnvelope{
					Payload: dataTx,
					Signatures: map[string][]byte{
						alice: aliceSig,
						bob:   bobSig,
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", alice).Return(aliceCert, nil)
				db.On("GetCertificate", bob).Return(bobCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).
					Run(func(args mock.Arguments) {
						tx := args[0].(*types.DataTxEnvelope)
						require.Equal(t, dataTxEnv, tx)
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
			txEnvFactory: func() *types.DataTxEnvelope {
				return &types.DataTxEnvelope{
					Payload: dataTx,
					Signatures: map[string][]byte{
						alice: aliceSig,
						bob:   bobSig,
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			timeoutStr:   "asdf",
			expectedCode: http.StatusBadRequest,
			expectedErr:  "time: invalid duration \"asdf\"",
		},
		{
			name: "transaction timeout negative",
			txEnvFactory: func() *types.DataTxEnvelope {
				return &types.DataTxEnvelope{
					Payload: dataTx,
					Signatures: map[string][]byte{
						alice: aliceSig,
						bob:   bobSig,
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			timeoutStr:   "-2s",
			expectedCode: http.StatusBadRequest,
			expectedErr:  "timeout can't be negative \"-2s\"",
		},
		{
			name: "submit data tx with missing payload",
			txEnvFactory: func() *types.DataTxEnvelope {
				return &types.DataTxEnvelope{
					Payload: nil,
					Signatures: map[string][]byte{
						alice: aliceSig,
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing transaction envelope payload (*types.DataTx)",
		},
		{
			name: "submit data tx with missing userID",
			txEnvFactory: func() *types.DataTxEnvelope {
				tx := &types.DataTx{}
				*tx = *dataTx
				tx.MustSignUserIds = nil
				return &types.DataTxEnvelope{
					Payload: tx,
					Signatures: map[string][]byte{
						alice: aliceSig,
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing UserID in transaction envelope payload (*types.DataTx)",
		},
		{
			name: "submit data tx with missing signature",
			txEnvFactory: func() *types.DataTxEnvelope {
				tx := &types.DataTx{}
				*tx = *dataTx
				tx.MustSignUserIds = []string{"charlie", "bob", "alice"}
				return &types.DataTxEnvelope{
					Payload: tx,
					Signatures: map[string][]byte{
						alice: aliceSig,
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "users [bob,charlie] in the must sign list have not signed the transaction",
		},
		{
			name: "submit data tx with an empty userID",
			txEnvFactory: func() *types.DataTxEnvelope {
				tx := &types.DataTx{}
				*tx = *dataTx
				tx.MustSignUserIds = []string{""}
				return &types.DataTxEnvelope{
					Payload: tx,
					Signatures: map[string][]byte{
						"": []byte("sign"),
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "an empty UserID in MustSignUserIDs list present in the transaction envelope",
		},
		{
			name: "bad signature",
			txEnvFactory: func() *types.DataTxEnvelope {
				return &types.DataTxEnvelope{
					Payload: dataTx,
					Signatures: map[string][]byte{
						alice: []byte("bad-sig"),
						bob:   bobSig,
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", alice).Return(aliceCert, nil)
				db.On("GetCertificate", bob).Return(bobCert, nil)

				return db
			},
			expectedCode: http.StatusUnauthorized,
			expectedErr:  "signature verification failed",
		},
		{
			name: "valid tx but contains an invalid signature from non-must sign user",
			txEnvFactory: func() *types.DataTxEnvelope {
				return &types.DataTxEnvelope{
					Payload: dataTx,
					Signatures: map[string][]byte{
						alice:   aliceSig,
						bob:     bobSig,
						charlie: []byte("bad sign"),
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return correctTxRespEnv
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", alice).Return(aliceCert, nil)
				db.On("GetCertificate", bob).Return(bobCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).
					Run(func(args mock.Arguments) {
						tx := args[0].(*types.DataTxEnvelope)
						require.Equal(t, dataTxEnv, tx)
						require.Equal(t, timeout, args[1].(time.Duration))
					}).
					Return(txRespEnv, nil)
				return db
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "no such user",
			txEnvFactory: func() *types.DataTxEnvelope {
				tx := &types.DataTx{}
				*tx = *dataTx
				tx.MustSignUserIds[0] = "not-alice"
				return &types.DataTxEnvelope{
					Payload: tx,
					Signatures: map[string][]byte{
						"not-alice": aliceSig,
						bob:         bobSig,
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", "not-alice").Return(nil, errors.New("no such user"))

				return db
			},
			expectedCode: http.StatusUnauthorized,
			expectedErr:  "signature verification failed",
		},
		{
			name: "fail to submit transaction",
			txEnvFactory: func() *types.DataTxEnvelope {
				tx := &types.DataTx{}
				*tx = *dataTx
				tx.MustSignUserIds[0] = alice
				return &types.DataTxEnvelope{
					Payload: dataTx,
					Signatures: map[string][]byte{
						alice: aliceSig,
						bob:   bobSig,
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", alice).Return(aliceCert, nil)
				db.On("GetCertificate", bob).Return(bobCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).Return(nil, errors.New("oops, submission failed"))

				return db
			},
			expectedCode: http.StatusInternalServerError,
			expectedErr:  "oops, submission failed",
		},
		{
			name: "not a leader",
			txEnvFactory: func() *types.DataTxEnvelope {
				return &types.DataTxEnvelope{
					Payload: dataTx,
					Signatures: map[string][]byte{
						alice:   aliceSig,
						bob:     bobSig,
						charlie: charlieSig,
					},
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return correctTxRespEnv
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", alice).Return(aliceCert, nil)
				db.On("GetCertificate", bob).Return(bobCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).Return(nil, &interrors.NotLeaderError{
					LeaderID:       3,
					LeaderHostPort: "server3.example.com:6091",
				})
				return db
			},
			expectedCode: http.StatusTemporaryRedirect,
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			txEnv := tt.txEnvFactory()
			txResp := tt.txRespFactory()
			txBytes, err := marshal.DefaultMarshaler().Marshal(txEnv)
			require.NoError(t, err)
			require.NotNil(t, txBytes)

			txReader := bytes.NewReader(txBytes)
			require.NotNil(t, txReader)

			reqUrl := &url.URL{
				Scheme: "http",
				Host:   "server1.example.com:6091",
				Path:   constants.PostDataTx,
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
			handler := NewDataRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			if tt.expectedCode == http.StatusOK {
				requestBody, err := ioutil.ReadAll(rr.Body)
				require.NoError(t, err)
				resp := &types.TxReceiptResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, resp))
				require.Equal(t, txResp, resp)
			} else if tt.expectedCode == http.StatusTemporaryRedirect {
				locationUrl := rr.Header().Get("Location")
				require.Equal(t, "http://server3.example.com:6091/data/tx", locationUrl)
			} else {
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}
		})
	}
}

func TestDataRequestHandler_DataJSONQueryWithContext(t *testing.T) {
	dbName := "test_database"

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	q := `{"attr1":{"$eq":true}}`
	queryBytes, err := json.Marshal(q)
	require.NoError(t, err)
	require.NotNil(t, queryBytes)

	queryReader := bytes.NewReader(queryBytes)
	require.NotNil(t, queryReader)

	sigFoo := testutils.SignatureFromQuery(t, aliceSigner, &types.DataJSONQuery{
		UserId: submittingUserName,
		DbName: dbName,
		Query:  q,
	})

	testCases := []struct {
		name                string
		requestFactory      func() (*http.Request, error)
		dbMockFactory       func(response *types.DataQueryResponseEnvelope) bcdb.DB
		expectedResponse    *types.DataQueryResponseEnvelope
		useCancelledContext bool
		expectedStatusCode  int
		expectedErr         string
	}{
		{
			name: "valid json query",
			requestFactory: func() (*http.Request, error) {
				queryReader := bytes.NewReader(queryBytes)
				require.NotNil(t, queryReader)
				req, err := http.NewRequest(http.MethodPost, constants.URLForJSONQuery(dbName), queryReader)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			useCancelledContext: false,
			dbMockFactory: func(response *types.DataQueryResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(true)
				db.On("DataQuery", mock.Anything, dbName, submittingUserName, []byte(q)).Return(response, nil)
				return db
			},
			expectedResponse: &types.DataQueryResponseEnvelope{
				Response: &types.DataQueryResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key1",
							Value: []byte(`{"attr1":true}`),
						},
					},
				},
				Signature: []byte{0, 0, 0},
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "valid json query but the context is closed",
			requestFactory: func() (*http.Request, error) {
				queryReader := bytes.NewReader(queryBytes)
				require.NotNil(t, queryReader)
				req, err := http.NewRequest(http.MethodPost, constants.URLForJSONQuery(dbName), queryReader)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFoo))
				return req, nil
			},
			dbMockFactory: func(response *types.DataQueryResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(true)
				db.On("DataQuery", mock.Anything, dbName, submittingUserName, []byte(q)).Return(response, nil)
				return db
			},
			useCancelledContext: true,
			expectedResponse:    nil,
			expectedStatusCode:  http.StatusRequestTimeout,
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

			var deadline time.Time
			if tt.useCancelledContext {
				deadline = time.Now()
			} else {
				deadline = time.Now().Add(10 * time.Second)
			}

			ctx, cancel := context.WithDeadline(req.Context(), deadline)
			defer cancel()
			if tt.useCancelledContext {
				cancel()
			}
			req = req.WithContext(ctx)

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
				res := &types.DataQueryResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
				//TODO verify signature on response
			}
		})
	}
}
