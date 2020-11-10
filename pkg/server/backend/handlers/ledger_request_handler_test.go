package handlers

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
	"github.ibm.com/blockchaindb/server/pkg/server/backend/mocks"
)

func TestBlockQuery(t *testing.T) {
	submittingUserName := "admin"

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetBlockResponseEnvelope) backend.DB
		expectedResponse   *types.GetBlockResponseEnvelope
		expectedStatusCode int
	}{
		{
			name: "valid get header request",
			expectedResponse: &types.GetBlockResponseEnvelope{
				Signature: []byte{0, 0, 0},
				Payload: &types.GetBlockResponse{
					Header: &types.ResponseHeader{
						NodeID: "testNodeID",
					},
					BlockHeader: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
					},
				},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerBlock(1), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetBlockResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(true, nil)
				db.On("GetBlockHeader", submittingUserName, uint64(1)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:             "user doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerBlock(1), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetBlockResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(false, nil)
				db.On("GetBlockHeader", submittingUserName, uint64(1)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name: "no block exist",
			expectedResponse: &types.GetBlockResponseEnvelope{
				Signature: []byte{0, 0, 0},
				Payload: &types.GetBlockResponse{
					Header: &types.ResponseHeader{
						NodeID: "testNodeID",
					},
				},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerBlock(1), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetBlockResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(true, nil)
				db.On("GetBlockHeader", submittingUserName, uint64(1)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req, err := tt.requestFactory()
			require.NoError(t, err)
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)
			rr := httptest.NewRecorder()
			handler := NewLedgerRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)

			if tt.expectedResponse != nil {
				res := &types.GetBlockResponseEnvelope{}
				err = json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)
				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}

func TestPathQuery(t *testing.T) {
	submittingUserName := "admin"

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetLedgerPathResponseEnvelope) backend.DB
		expectedResponse   *types.GetLedgerPathResponseEnvelope
		expectedStatusCode int
	}{
		{
			name: "valid get path request",
			expectedResponse: &types.GetLedgerPathResponseEnvelope{
				Signature: []byte{0, 0, 0},
				Payload: &types.GetLedgerPathResponse{
					Header: &types.ResponseHeader{
						NodeID: "testNodeID",
					},
					BlockHeaders: []*types.BlockHeader{
						{
							BaseHeader: &types.BlockHeaderBase{
								Number: 1,
							},
						},
						{
							BaseHeader: &types.BlockHeaderBase{
								Number: 2,
							},
						},
					},
				},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerPath(1, 2), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(true, nil)
				db.On("GetLedgerPath", submittingUserName, uint64(1), uint64(2)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:             "user doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerPath(1, 2), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(false, nil)
				db.On("GetLedgerPath", submittingUserName, uint64(1), uint64(2)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name:             "no path exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerPath(1, 2), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(true, nil)
				db.On("GetLedgerPath", submittingUserName, uint64(1), uint64(2)).Return(response, errors.Errorf("can't find path in blocks skip list between 2 1"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req, err := tt.requestFactory()
			require.NoError(t, err)
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)
			rr := httptest.NewRecorder()
			handler := NewLedgerRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)

			if tt.expectedResponse != nil {
				res := &types.GetLedgerPathResponseEnvelope{}
				rr.Body.Bytes()
				err = json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)
				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}
