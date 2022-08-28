// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/hyperledger-labs/orion-server/internal/bcdb"
	"github.com/hyperledger-labs/orion-server/internal/bcdb/mocks"
	interrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/marshal"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
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
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestCrypto(t, cryptoDir, "bob")

	testCases := []struct {
		name               string
		requestFactory     func() *http.Request
		dbMockFactory      func(response *types.GetConfigResponseEnvelope) bcdb.DB
		expectedResponse   *types.GetConfigResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "successfully retrieve configuration",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetConfig, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetConfigQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetConfig", "alice").Return(response, nil)
				return db
			},
			expectedResponse: &types.GetConfigResponseEnvelope{
				Response: &types.GetConfigResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeId",
					},
					Metadata: &types.Metadata{
						Version: &types.Version{
							TxNum:    1,
							BlockNum: 1,
						},
					},
					Config: &types.ClusterConfig{
						CertAuthConfig: &types.CAConfig{
							Roots: [][]byte{{0, 0, 0}},
						},
						Nodes: []*types.NodeConfig{
							{
								Id:          "testNodeId",
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
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) bcdb.DB {
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
				sig := testutils.SignatureFromQuery(t, bobSigner, &types.GetConfigQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) bcdb.DB {
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
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetConfigQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) bcdb.DB {
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
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetConfigQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetConfigResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetConfig", "alice").Return(nil, errors.New("failed to get configuration"))
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
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				requestBody, err := ioutil.ReadAll(rr.Body)
				require.NoError(t, err)
				res := &types.GetConfigResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
				// TODO verify signature on responses
			}
		})
	}
}

func TestConfigRequestHandler_SubmitConfig(t *testing.T) {
	submittingUserName := "admin"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"admin"})
	adminCert, adminSigner := testutils.LoadTestCrypto(t, cryptoDir, "admin")

	configTx := &types.ConfigTx{
		UserId: submittingUserName,
		TxId:   "1",
		NewConfig: &types.ClusterConfig{
			Admins: []*types.Admin{
				{
					Id:          "admin1",
					Certificate: []byte("bogus"),
				},
			},
			Nodes: []*types.NodeConfig{
				{
					Id:          "testNode",
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
		txRespFactory           func() *types.TxReceiptResponseEnvelope
		createMockAndInstrument func(t *testing.T, configTx *types.ConfigTxEnvelope, txRespEnv interface{}, timeout time.Duration) bcdb.DB
		timeoutStr              string
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
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return correctTxRespEnv
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(adminCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
					config := args[0].(*types.ConfigTxEnvelope)
					require.Equal(t, configTx, config)
					require.Equal(t, timeout, args[1].(time.Duration))
				}).Return(txRespEnv, nil)

				return db
			},
			timeoutStr:   "1s",
			expectedCode: http.StatusOK,
		},
		{
			name: "transaction timeout",
			txEnvFactory: func() *types.ConfigTxEnvelope {
				return &types.ConfigTxEnvelope{
					Payload:   configTx,
					Signature: sigAdmin,
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(adminCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).
					Run(func(args mock.Arguments) {
						tx := args[0].(*types.ConfigTxEnvelope)
						require.Equal(t, configTx, tx)
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
			txEnvFactory: func() *types.ConfigTxEnvelope {
				return &types.ConfigTxEnvelope{
					Payload:   configTx,
					Signature: sigAdmin,
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			timeoutStr:   "asdf",
			expectedCode: http.StatusBadRequest,
			expectedErr:  "time: invalid duration \"asdf\"",
		},
		{
			name: "transaction timeout negative",
			txEnvFactory: func() *types.ConfigTxEnvelope {
				return &types.ConfigTxEnvelope{
					Payload:   configTx,
					Signature: sigAdmin,
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			timeoutStr:   "-2s",
			expectedCode: http.StatusBadRequest,
			expectedErr:  "timeout can't be negative \"-2s\"",
		},
		{
			name: "submit configuration with missing payload",
			txEnvFactory: func() *types.ConfigTxEnvelope {
				return &types.ConfigTxEnvelope{Payload: nil, Signature: sigAdmin}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
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
				tx.UserId = ""
				return &types.ConfigTxEnvelope{Payload: tx, Signature: sigAdmin}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
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
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
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
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
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
				tx.UserId = "not-admin"
				return &types.ConfigTxEnvelope{
					Payload:   tx,
					Signature: sigAdmin,
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
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
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return nil
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(adminCert, nil)
				db.On("SubmitTransaction", mock.Anything, mock.Anything).Return(nil, errors.New("oops, submission failed"))

				return db
			},
			expectedCode: http.StatusInternalServerError,
			expectedErr:  "oops, submission failed",
		},
		{
			name: "not a leader",
			txEnvFactory: func() *types.ConfigTxEnvelope {
				return &types.ConfigTxEnvelope{
					Payload:   configTx,
					Signature: sigAdmin,
				}
			},
			txRespFactory: func() *types.TxReceiptResponseEnvelope {
				return correctTxRespEnv
			},
			createMockAndInstrument: func(t *testing.T, configTx *types.ConfigTxEnvelope, txRespEnv interface{}, timeout time.Duration) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(adminCert, nil)
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
			txResp := tt.txRespFactory()
			txBytes, err := marshal.DefaultMarshaler().Marshal(txEnv)
			require.NoError(t, err)

			txReader := bytes.NewReader(txBytes)
			require.NotNil(t, txReader)

			reqUrl := &url.URL{
				Scheme: "http",
				Host:   "server1.example.com:6091",
				Path:   constants.PostConfigTx,
			}
			req, err := http.NewRequest(http.MethodPost, reqUrl.String(), txReader)
			require.NoError(t, err)
			require.NotNil(t, req)

			rr := httptest.NewRecorder()
			require.NotNil(t, rr)

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

			handler := NewConfigRequestHandler(tt.createMockAndInstrument(t, txEnv, txResp, timeout), logger)
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
				require.Equal(t, "http://server3.example.com:6091/config/tx", locationUrl)
			} else {
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}
		})
	}
}

func TestConfigRequestHandler_GetNodesConfig(t *testing.T) {
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestCrypto(t, cryptoDir, "bob")

	testCases := []struct {
		name               string
		requestFactory     func() *http.Request
		dbMockFactory      func(response *types.GetNodeConfigResponseEnvelope) bcdb.DB
		expectedResponse   *types.GetNodeConfigResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "successfully retrieve single node configuration",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.URLForNodeConfigPath("node1"), nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetNodeConfigQuery{
					UserId: submittingUserName,
					NodeId: "node1",
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetNodeConfigResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetNodeConfig", "node1").Return(response, nil)
				return db
			},
			expectedResponse: &types.GetNodeConfigResponseEnvelope{
				Response: &types.GetNodeConfigResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeId",
					},
					NodeConfig: &types.NodeConfig{
						Id:          "node1",
						Address:     "http://localhost",
						Port:        8080,
						Certificate: []byte{0, 0, 0},
					},
				},
			},
			expectedStatusCode: http.StatusOK,
			expectedErr:        "",
		},
		{
			name: "missing user header",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.URLForNodeConfigPath("node1"), nil)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req
			},
			dbMockFactory: func(response *types.GetNodeConfigResponseEnvelope) bcdb.DB {
				return &mocks.DB{}
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "UserID is not set in the http request header",
		},
		{
			name: "missing signature header",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.URLForNodeConfigPath("node1"), nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				return req
			},
			dbMockFactory: func(response *types.GetNodeConfigResponseEnvelope) bcdb.DB {
				return &mocks.DB{}
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "Signature is not set in the http request header",
		},
		{
			name: "fail to verify signature of submitting user",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.URLForNodeConfigPath("node1"), nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, bobSigner, &types.GetNodeConfigQuery{
					UserId: submittingUserName,
					NodeId: "node1",
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetNodeConfigResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetNodeConfig", "node1").Return(nil, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "submitting user doesn't exists",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.URLForNodeConfigPath("node1"), nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetNodeConfigQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetNodeConfigResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				db.On("GetNodeConfig", "node1").Return(nil, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "failing to get config from DB",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.URLForNodeConfigPath("node1"), nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetNodeConfigQuery{
					UserId: submittingUserName,
					NodeId: "node1",
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetNodeConfigResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetNodeConfig", "node1").Return(nil, errors.New("failed to get configuration"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /config/node/node1' because failed to get configuration",
		},
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(fmt.Sprintf("GetNodesConfig %s", tt.name), func(t *testing.T) {
			req := tt.requestFactory()
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)

			rr := httptest.NewRecorder()
			handler := NewConfigRequestHandler(db, logger)
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
				res := &types.GetNodeConfigResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
				// TODO verify signature on responses
			}
		})
	}
}

func TestConfigRequestHandler_GetLastConfigBlock(t *testing.T) {
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestCrypto(t, cryptoDir, "bob")

	testCases := []struct {
		name               string
		requestFactory     func() *http.Request
		dbMockFactory      func(response *types.GetConfigBlockResponseEnvelope) bcdb.DB
		expectedResponse   *types.GetConfigBlockResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "successfully retrieve last config block",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetLastConfigBlock, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetConfigBlockQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetConfigBlockResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetConfigBlock", submittingUserName, uint64(0)).Return(response, nil)
				return db
			},
			expectedResponse: &types.GetConfigBlockResponseEnvelope{
				Response: &types.GetConfigBlockResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeId",
					},
					Block: utils.MarshalOrPanic(
						&types.Block{
							Header: &types.BlockHeader{
								BaseHeader: &types.BlockHeaderBase{
									Number: 10,
								},
							},
							Payload: &types.Block_ConfigTxEnvelope{
								ConfigTxEnvelope: &types.ConfigTxEnvelope{
									Payload: &types.ConfigTx{
										UserId:    "admin",
										NewConfig: &types.ClusterConfig{},
									},
								},
							},
						},
					),
				},
			},
			expectedStatusCode: http.StatusOK,
			expectedErr:        "",
		},
		{
			name: "missing user header",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetLastConfigBlock, nil)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req
			},
			dbMockFactory: func(response *types.GetConfigBlockResponseEnvelope) bcdb.DB {
				return &mocks.DB{}
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "UserID is not set in the http request header",
		},
		{
			name: "missing signature header",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetLastConfigBlock, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				return req
			},
			dbMockFactory: func(response *types.GetConfigBlockResponseEnvelope) bcdb.DB {
				return &mocks.DB{}
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "Signature is not set in the http request header",
		},
		{
			name: "fail to verify signature of submitting user",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetLastConfigBlock, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, bobSigner, &types.GetConfigQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetConfigBlockResponseEnvelope) bcdb.DB {
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
				req := httptest.NewRequest(http.MethodGet, constants.GetLastConfigBlock, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetConfigQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetConfigBlockResponseEnvelope) bcdb.DB {
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
				req := httptest.NewRequest(http.MethodGet, constants.GetLastConfigBlock, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetConfigQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetConfigBlockResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetConfigBlock", submittingUserName, uint64(0)).Return(nil, errors.New("failed to get configuration"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /config/block/last' because failed to get configuration",
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
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				requestBody, err := ioutil.ReadAll(rr.Body)
				require.NoError(t, err)
				res := &types.GetConfigBlockResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
				// TODO verify signature on responses
			}
		})
	}
}

func TestConfigRequestHandler_GetClusterStatus(t *testing.T) {
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestCrypto(t, cryptoDir, "bob")

	testCases := []struct {
		name               string
		requestFactory     func() *http.Request
		dbMockFactory      func(response *types.GetClusterStatusResponseEnvelope) bcdb.DB
		expectedResponse   *types.GetClusterStatusResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "successfully retrieve cluster status",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetClusterStatus, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetClusterStatusQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetClusterStatusResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetClusterStatus", false).Return(response, nil)
				return db
			},
			expectedResponse: &types.GetClusterStatusResponseEnvelope{
				Response: &types.GetClusterStatusResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeId1",
					},
					Nodes: []*types.NodeConfig{
						{
							Id:          "testNodeId1",
							Address:     "10.10.10.11",
							Port:        23001,
							Certificate: []byte("bogus-cert"),
						},
						{
							Id:          "testNodeId2",
							Address:     "10.10.10.12",
							Port:        23001,
							Certificate: []byte("bogus-cert"),
						},
						{
							Id:          "testNodeId3",
							Address:     "10.10.10.13",
							Port:        23001,
							Certificate: []byte("bogus-cert"),
						},
					},
					Version: &types.Version{BlockNum: 1, TxNum: 0},
					Leader:  "testNodeId1",
					Active:  []string{"testNodeId1", "testNodeId2", "testNodeId3"},
				},
			},
			expectedStatusCode: http.StatusOK,
			expectedErr:        "",
		},
		{
			name: "successfully retrieve cluster status, query noCert=true",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetClusterStatus+"?nocert=true", nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetClusterStatusQuery{
					UserId:         submittingUserName,
					NoCertificates: true,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetClusterStatusResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetClusterStatus", true).Return(response, nil)
				return db
			},
			expectedResponse: &types.GetClusterStatusResponseEnvelope{
				Response: &types.GetClusterStatusResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeId1",
					},
					Nodes: []*types.NodeConfig{
						{
							Id:      "testNodeId1",
							Address: "10.10.10.11",
							Port:    23001,
						},
						{
							Id:      "testNodeId2",
							Address: "10.10.10.12",
							Port:    23001,
						},
						{
							Id:      "testNodeId3",
							Address: "10.10.10.13",
							Port:    23001,
						},
					},
					Version: &types.Version{BlockNum: 1, TxNum: 0},
					Leader:  "testNodeId1",
					Active:  []string{"testNodeId1", "testNodeId2", "testNodeId3"},
				},
			},
			expectedStatusCode: http.StatusOK,
			expectedErr:        "",
		},
		{
			name: "successfully retrieve cluster status, query noCert=false",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetClusterStatus+"?nocert=false", nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetClusterStatusQuery{
					UserId:         submittingUserName,
					NoCertificates: false,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetClusterStatusResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetClusterStatus", false).Return(response, nil)
				return db
			},
			expectedResponse: &types.GetClusterStatusResponseEnvelope{
				Response: &types.GetClusterStatusResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeId1",
					},
					Nodes: []*types.NodeConfig{
						{
							Id:          "testNodeId1",
							Address:     "10.10.10.11",
							Port:        23001,
							Certificate: []byte("bogus-cert"),
						},
						{
							Id:          "testNodeId2",
							Address:     "10.10.10.12",
							Port:        23001,
							Certificate: []byte("bogus-cert"),
						},
						{
							Id:          "testNodeId3",
							Address:     "10.10.10.13",
							Port:        23001,
							Certificate: []byte("bogus-cert"),
						},
					},
					Version: &types.Version{BlockNum: 1, TxNum: 0},
					Leader:  "testNodeId1",
					Active:  []string{"testNodeId1", "testNodeId2", "testNodeId3"},
				},
			},
			expectedStatusCode: http.StatusOK,
			expectedErr:        "",
		},
		{
			name: "bad query is ignored",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetClusterStatus+"?noKidding=true", nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetClusterStatusQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetClusterStatusResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetClusterStatus", false).Return(response, nil)
				return db
			},
			expectedResponse: &types.GetClusterStatusResponseEnvelope{
				Response: &types.GetClusterStatusResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeId1",
					},
					Nodes: []*types.NodeConfig{
						{
							Id:          "testNodeId1",
							Address:     "10.10.10.11",
							Port:        23001,
							Certificate: []byte("bogus-cert"),
						},
						{
							Id:          "testNodeId2",
							Address:     "10.10.10.12",
							Port:        23001,
							Certificate: []byte("bogus-cert"),
						},
						{
							Id:          "testNodeId3",
							Address:     "10.10.10.13",
							Port:        23001,
							Certificate: []byte("bogus-cert"),
						},
					},
					Version: &types.Version{BlockNum: 1, TxNum: 0},
					Leader:  "testNodeId1",
					Active:  []string{"testNodeId1", "testNodeId2", "testNodeId3"},
				},
			},
			expectedStatusCode: http.StatusOK,
			expectedErr:        "",
		},
		{
			name: "missing user header",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetClusterStatus, nil)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req
			},
			dbMockFactory: func(response *types.GetClusterStatusResponseEnvelope) bcdb.DB {
				return &mocks.DB{}
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "UserID is not set in the http request header",
		},
		{
			name: "missing signature header",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetClusterStatus, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				return req
			},
			dbMockFactory: func(response *types.GetClusterStatusResponseEnvelope) bcdb.DB {
				return &mocks.DB{}
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "Signature is not set in the http request header",
		},
		{
			name: "fail to verify signature of submitting user",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetClusterStatus, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, bobSigner, &types.GetConfigQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetClusterStatusResponseEnvelope) bcdb.DB {
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
				req := httptest.NewRequest(http.MethodGet, constants.GetClusterStatus, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetConfigQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetClusterStatusResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "failing to get cluster status from DB",
			requestFactory: func() *http.Request {
				req := httptest.NewRequest(http.MethodGet, constants.GetClusterStatus, nil)
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetConfigQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req
			},
			dbMockFactory: func(response *types.GetClusterStatusResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetClusterStatus", false).Return(nil, errors.New("failed to get cluster status"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /config/cluster' because failed to get cluster status",
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
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				requestBody, err := ioutil.ReadAll(rr.Body)
				require.NoError(t, err)
				res := &types.GetClusterStatusResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
				// TODO verify signature on responses
			}
		})
	}
}
