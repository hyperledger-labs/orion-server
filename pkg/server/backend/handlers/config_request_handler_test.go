package handlers

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
	"github.ibm.com/blockchaindb/server/pkg/server/backend/mocks"
)

func createLogger(logLevel string) (*logger.SugarLogger, error) {
	c := &logger.Config{
		Level:         logLevel,
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(c)
	return logger, err
}

func TestConfigRequestHandler_GetConfig(t *testing.T) {
	submittingUserName := "admin"

	testCases := []struct {
		name               string
		requestFactory     func() *http.Request
		dbMockFactory      func(response *types.GetConfigResponseEnvelope) backend.DB
		expectedResponse   *types.GetConfigResponseEnvelope
		expectedStatusCode int
	}{
		{
			name: "successfully retrieve configuration",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetConfig, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(true, nil)
				db.On("GetConfig").Return(response, nil)
				return db
			},
			expectedResponse: &types.GetConfigResponseEnvelope{
				Payload: &types.GetConfigResponse{
					Header: &types.ResponseHeader{
						NodeID: "testNodeId",
					},
					Metadata: &types.Metadata{
						Version: &types.Version{
							TxNum:    1,
							BlockNum: 1,
						},
					},
					Config: &types.ClusterConfig{
						RootCACertificate: []byte{0, 0, 0},
						Nodes: []*types.NodeConfig{
							{
								ID:          "testNodeId",
								Address:     "http://localhost",
								Port:        8080,
								Certificate: []byte{0, 0, 0},
							},
						},
					},
				},
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "missing user header",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetConfig, nil)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) backend.DB {
				return &mocks.DB{}
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "fails to retrieve configuration",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetConfig, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) backend.DB {
				return &mocks.DB{}
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "fail to retrieve record of submitting user",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetConfig, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(false, errors.New("fail to retrieve user's record"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
		},
		{
			name: "submitting user doesn't exists",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetConfig, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(false, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "failing to submit config transaction",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetConfig, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(true, nil)
				db.On("GetConfig").Return(nil, errors.New("failed to get configuration"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("GetConfig %s", tt.name), func(t *testing.T) {
			t.Parallel()
			req := tt.requestFactory()
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)

			rr := httptest.NewRecorder()
			handler := NewConfigRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)

			if tt.expectedResponse != nil {
				res := &types.GetConfigResponseEnvelope{}
				err := json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)

				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}

func TestConfigRequestHandler_SubmitConfig(t *testing.T) {
	testCases := []struct {
		name                    string
		configTx                *types.ConfigTxEnvelope
		createMockAndInstrument func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB
		expectedCode            int
	}{
		{
			name: "submit valid configuration update",
			configTx: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					UserID: "admin",
					TxID:   "1",
					NewConfig: &types.ClusterConfig{
						Admins: []*types.Admin{
							{
								ID: "admin1",
							},
						},
						Nodes: []*types.NodeConfig{
							{
								ID:          "testNode",
								Certificate: []byte{0, 0, 0},
								Address:     "http://localhost",
								Port:        8080,
							},
						},
					},
					ReadOldConfigVersion: &types.Version{
						TxNum:    1,
						BlockNum: 1,
					},
				},
				Signature: []byte{0, 0, 0},
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", mock.Anything).Return(true, nil)
				db.On("SubmitTransaction", mock.Anything).Run(func(args mock.Arguments) {
					config := args[0].(*types.ConfigTxEnvelope)
					require.Equal(t, configTx, config)
				}).Return(nil)

				return db
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "submit configuration with missing userID",
			configTx: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					TxID: "1",
					NewConfig: &types.ClusterConfig{
						Admins: []*types.Admin{
							{
								ID: "admin1",
							},
						},
						Nodes: []*types.NodeConfig{
							{
								ID:          "testNode",
								Certificate: []byte{0, 0, 0},
								Address:     "http://localhost",
								Port:        8080,
							},
						},
					},
					ReadOldConfigVersion: &types.Version{
						TxNum:    1,
						BlockNum: 1,
					},
				},
				Signature: []byte{0, 0, 0},
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", mock.Anything).Return(false, nil)
				return db
			},
			expectedCode: http.StatusForbidden,
		},
		{
			name: "fail to retrieve information about submitting user",
			configTx: &types.ConfigTxEnvelope{
				Payload: &types.ConfigTx{
					TxID: "1",
					NewConfig: &types.ClusterConfig{
						Admins: []*types.Admin{
							{
								ID: "admin1",
							},
						},
						Nodes: []*types.NodeConfig{
							{
								ID:          "testNode",
								Certificate: []byte{0, 0, 0},
								Address:     "http://localhost",
								Port:        8080,
							},
						},
					},
					ReadOldConfigVersion: &types.Version{
						TxNum:    1,
						BlockNum: 1,
					},
				},
				Signature: []byte{0, 0, 0},
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", mock.Anything).Return(false, errors.New("fail to retrieve user's record"))
				return db
			},
			expectedCode: http.StatusBadRequest,
		},
		{
			name:     "fail to submit transaction",
			configTx: &types.ConfigTxEnvelope{},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", mock.Anything).Return(true, nil)
				db.On("SubmitTransaction", mock.Anything).Return(errors.New("failed to submit transactions"))

				return db
			},
			expectedCode: http.StatusInternalServerError,
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			txBytes, err := json.Marshal(tt.configTx)
			require.NoError(t, err)

			txReader := bytes.NewReader(txBytes)
			require.NotNil(t, txReader)

			req, err := http.NewRequest(http.MethodPost, constants.PostConfigTx, txReader)
			require.NoError(t, err)
			require.NotNil(t, req)

			rr := httptest.NewRecorder()
			require.NotNil(t, rr)

			handler := NewConfigRequestHandler(tt.createMockAndInstrument(t, tt.configTx), logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedCode, rr.Code)

		})
	}
}
