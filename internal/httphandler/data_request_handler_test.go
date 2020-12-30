package httphandler

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
	"github.ibm.com/blockchaindb/server/internal/bcdb"
	"github.ibm.com/blockchaindb/server/internal/bcdb/mocks"
	interrors "github.ibm.com/blockchaindb/server/internal/errors"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
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
		dbMockFactory      func(response *types.GetDataResponseEnvelope) bcdb.DB
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
					UserID: submittingUserName,
					DBName: dbName,
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
	userID := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	dataTx := &types.DataTx{
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
	}
	aliceSig := testutils.SignatureFromTx(t, aliceSigner, dataTx)

	testCases := []struct {
		name                    string
		txEnvFactory            func() *types.DataTxEnvelope
		createMockAndInstrument func(t *testing.T, dataTxEnv interface{}) bcdb.DB
		expectedCode            int
		expectedErr             string
	}{
		{
			name: "submit valid data transaction",
			txEnvFactory: func() *types.DataTxEnvelope {
				return &types.DataTxEnvelope{
					Payload:   dataTx,
					Signature: aliceSig,
				}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", userID).Return(aliceCert, nil)
				db.On("SubmitTransaction", mock.Anything).
					Run(func(args mock.Arguments) {
						tx := args[0].(*types.DataTxEnvelope)
						require.Equal(t, dataTxEnv, tx)
					}).
					Return(nil)
				return db
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "submit data tx with missing payload",
			txEnvFactory: func() *types.DataTxEnvelope {
				return &types.DataTxEnvelope{Payload: nil, Signature: aliceSig}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
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
				tx.UserID = ""
				return &types.DataTxEnvelope{Payload: tx, Signature: aliceSig}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing UserID in transaction envelope payload (*types.DataTx)",
		},
		{
			name: "submit data tx with missing signature",
			txEnvFactory: func() *types.DataTxEnvelope {
				return &types.DataTxEnvelope{Payload: dataTx, Signature: nil}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing Signature in transaction envelope payload (*types.DataTx)",
		},
		{
			name: "bad signature",
			txEnvFactory: func() *types.DataTxEnvelope {
				return &types.DataTxEnvelope{
					Payload:   dataTx,
					Signature: []byte("bad-sig"),
				}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", userID).Return(aliceCert, nil)

				return db
			},
			expectedCode: http.StatusUnauthorized,
			expectedErr:  "signature verification failed",
		},
		{
			name: "no such user",
			txEnvFactory: func() *types.DataTxEnvelope {
				tx := &types.DataTx{}
				*tx = *dataTx
				tx.UserID = "not-alice"
				return &types.DataTxEnvelope{
					Payload:   tx,
					Signature: aliceSig,
				}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
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
				return &types.DataTxEnvelope{
					Payload:   dataTx,
					Signature: aliceSig,
				}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", userID).Return(aliceCert, nil)
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
			require.NotNil(t, txBytes)

			txReader := bytes.NewReader(txBytes)
			require.NotNil(t, txReader)

			req, err := http.NewRequest(http.MethodPost, constants.PostDataTx, txReader)
			require.NoError(t, err)
			require.NotNil(t, req)

			rr := httptest.NewRecorder()

			db := tt.createMockAndInstrument(t, txEnv)
			handler := NewDataRequestHandler(db, logger)
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
