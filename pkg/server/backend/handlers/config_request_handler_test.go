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
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
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
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "bob")

	testCases := []struct {
		name               string
		requestFactory     func() *http.Request
		dbMockFactory      func(response *types.GetConfigResponseEnvelope) backend.DB
		expectedResponse   *types.GetConfigResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "successfully retrieve configuration",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetConfig, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetConfigQuery{UserID: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
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
			expectedErr:        "",
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
			expectedErr:        "UserID is not set in the http request header",
		},
		{
			name: "missing signature header",
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
			expectedErr:        "Signature is not set in the http request header",
		},
		{
			name: "fail to verify signature of submitting user",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetConfig, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, bobSigner, &types.GetConfigQuery{UserID: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "submitting user doesn't exists",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetConfig, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetConfigQuery{UserID: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "failing to get config from DB",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetConfig, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetConfigQuery{UserID: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetConfig").Return(nil, errors.New("failed to get configuration"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /config/tx' because failed to get configuration",
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("GetConfig %s", tt.name), func(t *testing.T) {
			req := tt.requestFactory()
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)

			rr := httptest.NewRecorder()
			handler := NewConfigRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)
			if tt.expectedStatusCode != http.StatusOK {
				respErr := &ResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				res := &types.GetConfigResponseEnvelope{}
				err := json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)
				require.Equal(t, tt.expectedResponse, res)
				// TODO verify signature on responses
			}
		})
	}
}

func TestConfigRequestHandler_SubmitConfig(t *testing.T) {
	submittingUserName := "admin"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"admin"})
	adminCert, adminSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "admin")

	configTx := &types.ConfigTx{
		UserID: submittingUserName,
		TxID:   "1",
		NewConfig: &types.ClusterConfig{
			Admins: []*types.Admin{
				{
					ID:          "admin1",
					Certificate: []byte("bogus"),
				},
			},
			Nodes: []*types.NodeConfig{
				{
					ID:          "testNode",
					Certificate: []byte("fake"),
					Address:     "http://localhost",
					Port:        8080,
				},
			},
		},
		ReadOldConfigVersion: &types.Version{
			BlockNum: 1,
			TxNum:    1,
		},
	}
	sigAdmin := testutils.SignatureFromTx(t, adminSigner, configTx)

	type testCase struct {
		name                    string
		txEnvFactory            func() *types.ConfigTxEnvelope
		createMockAndInstrument func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB
		expectedCode            int
		expectedErr             string
	}

	testCases := []testCase{
		{
			name: "submit valid configuration update",
			txEnvFactory: func() *types.ConfigTxEnvelope {
				return &types.ConfigTxEnvelope{
					Payload:   configTx,
					Signature: sigAdmin,
				}
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(adminCert, nil)
				db.On("SubmitTransaction", mock.Anything).Run(func(args mock.Arguments) {
					config := args[0].(*types.ConfigTxEnvelope)
					require.Equal(t, configTx, config)
				}).Return(nil)

				return db
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "submit configuration with missing payload",
			txEnvFactory: func() *types.ConfigTxEnvelope {
				return &types.ConfigTxEnvelope{Payload: nil, Signature: sigAdmin}
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing transaction envelope payload (*types.ConfigTx)",
		},
		{
			name: "submit configuration with missing userID",
			txEnvFactory: func() *types.ConfigTxEnvelope {
				tx := &types.ConfigTx{}
				*tx = *configTx
				tx.UserID = ""
				return &types.ConfigTxEnvelope{Payload: tx, Signature: sigAdmin}
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing UserID in transaction envelope payload (*types.ConfigTx)",
		},
		{
			name: "submit configuration with missing signature",
			txEnvFactory: func() *types.ConfigTxEnvelope {
				return &types.ConfigTxEnvelope{Payload: configTx, Signature: nil}
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing Signature in transaction envelope payload (*types.ConfigTx)",
		},
		{
			name: "bad signature",
			txEnvFactory: func() *types.ConfigTxEnvelope {
				return &types.ConfigTxEnvelope{
					Payload:   configTx,
					Signature: []byte("bad-sig"),
				}
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(adminCert, nil)

				return db
			},
			expectedCode: http.StatusUnauthorized,
			expectedErr:  "signature verification failed",
		},
		{
			name: "no such user",
			txEnvFactory: func() *types.ConfigTxEnvelope {
				tx := &types.ConfigTx{}
				*tx = *configTx
				tx.UserID = "not-admin"
				return &types.ConfigTxEnvelope{
					Payload:   tx,
					Signature: sigAdmin,
				}
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", "not-admin").Return(nil, errors.New("no such user"))

				return db
			},
			expectedCode: http.StatusUnauthorized,
			expectedErr:  "signature verification failed",
		},
		{
			name: "fail to submit transaction",
			txEnvFactory: func() *types.ConfigTxEnvelope {
				return &types.ConfigTxEnvelope{
					Payload:   configTx,
					Signature: sigAdmin,
				}
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(adminCert, nil)
				db.On("SubmitTransaction", mock.Anything).Return(errors.New("oops, submission failed"))

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

			txReader := bytes.NewReader(txBytes)
			require.NotNil(t, txReader)

			req, err := http.NewRequest(http.MethodPost, constants.PostConfigTx, txReader)
			require.NoError(t, err)
			require.NotNil(t, req)

			rr := httptest.NewRecorder()
			require.NotNil(t, rr)

			handler := NewConfigRequestHandler(tt.createMockAndInstrument(t, txEnv), logger)
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
