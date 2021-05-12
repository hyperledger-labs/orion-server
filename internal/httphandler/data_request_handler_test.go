// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/IBM-Blockchain/bcdb-server/internal/bcdb"
	"github.com/IBM-Blockchain/bcdb-server/internal/bcdb/mocks"
	interrors "github.com/IBM-Blockchain/bcdb-server/internal/errors"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/server/testutils"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

func TestDataRequestHandler_DataQuery(t *testing.T) {
	dbName := "test_database"

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "bob")

	sigFoo := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDataQuery{
		UserID: submittingUserName,
		DBName: dbName,
		Key:    "foo",
	})

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.ResponseEnvelope) bcdb.DB
		expectedResponse   *types.ResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get data request",
			expectedResponse: &types.ResponseEnvelope{
				Signature: []byte{0, 0, 0},
				Payload: MarshalOrPanic(&types.Payload{
					Header: &types.ResponseHeader{
						NodeID: "testNodeID",
					},

					Response: MarshalOrPanic(&types.GetDataResponse{
						Value: []byte("bar"),
						Metadata: &types.Metadata{
							Version: &types.Version{
								TxNum:    1,
								BlockNum: 1,
							},
						},
					}),
				}),
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
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("IsDBExists", dbName).Return(true)
				db.On("GetData", dbName, submittingUserName, "foo").
					Return(nil, &interrors.PermissionErr{ErrMsg: "access forbidden"})
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
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
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
					UserID: submittingUserName,
					DBName: dbName,
					Key:    "foo",
				})

				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sigFooBob))
				return req, nil
			},
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
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
			dbMockFactory: func(response *types.ResponseEnvelope) bcdb.DB {
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
				res := &types.ResponseEnvelope{}
				err = json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)
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
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice", "bob", "charlie"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")
	bobCert, bobSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "bob")
	_, charlieSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "charlie")

	dataTx := &types.DataTx{
		MustSignUserIDs: []string{alice, bob},
		TxID:            "1",
		DBOperations: []*types.DBOperation{
			{
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
		},
	}
	aliceSig := testutils.SignatureFromTx(t, aliceSigner, dataTx)
	bobSig := testutils.SignatureFromTx(t, bobSigner, dataTx)
	charlieSig := testutils.SignatureFromTx(t, charlieSigner, dataTx)
	testCases := []struct {
		name                    string
		txEnvFactory            func() *types.DataTxEnvelope
		txRespFactory           func() *types.ResponseEnvelope
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
			txRespFactory: func() *types.ResponseEnvelope {
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
			txRespFactory: func() *types.ResponseEnvelope {
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
			txRespFactory: func() *types.ResponseEnvelope {
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
			txRespFactory: func() *types.ResponseEnvelope {
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
			txRespFactory: func() *types.ResponseEnvelope {
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
				tx.MustSignUserIDs = nil
				return &types.DataTxEnvelope{
					Payload: tx,
					Signatures: map[string][]byte{
						alice: aliceSig,
					},
				}
			},
			txRespFactory: func() *types.ResponseEnvelope {
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
				tx.MustSignUserIDs = []string{"charlie", "bob", "alice"}
				return &types.DataTxEnvelope{
					Payload: tx,
					Signatures: map[string][]byte{
						alice: aliceSig,
					},
				}
			},
			txRespFactory: func() *types.ResponseEnvelope {
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
				tx.MustSignUserIDs = []string{""}
				return &types.DataTxEnvelope{
					Payload: tx,
					Signatures: map[string][]byte{
						"": []byte("sign"),
					},
				}
			},
			txRespFactory: func() *types.ResponseEnvelope {
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
			txRespFactory: func() *types.ResponseEnvelope {
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
			txRespFactory: func() *types.ResponseEnvelope {
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
				tx.MustSignUserIDs[0] = "not-alice"
				return &types.DataTxEnvelope{
					Payload: tx,
					Signatures: map[string][]byte{
						"not-alice": aliceSig,
						bob:         bobSig,
					},
				}
			},
			txRespFactory: func() *types.ResponseEnvelope {
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
				tx.MustSignUserIDs[0] = alice
				return &types.DataTxEnvelope{
					Payload: dataTx,
					Signatures: map[string][]byte{
						alice: aliceSig,
						bob:   bobSig,
					},
				}
			},
			txRespFactory: func() *types.ResponseEnvelope {
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
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			txEnv := tt.txEnvFactory()
			txResp := tt.txRespFactory()
			txBytes, err := json.Marshal(txEnv)
			require.NoError(t, err)
			require.NotNil(t, txBytes)

			txReader := bytes.NewReader(txBytes)
			require.NotNil(t, txReader)

			req, err := http.NewRequest(http.MethodPost, constants.PostDataTx, txReader)
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

			// require.Equal(t, tt.expectedCode, rr.Code)
			if tt.expectedCode == http.StatusOK {
				resp := &types.ResponseEnvelope{}
				err := json.NewDecoder(rr.Body).Decode(resp)
				require.NoError(t, err)
				require.Equal(t, txResp, resp)
			} else {
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}
		})
	}
}
