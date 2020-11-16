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

func TestUsersRequestHandler_GetUser(t *testing.T) {
	submittingUserName := "alice"
	targetUserID := "targetUserID"

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice", "bob"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")
	_, bobSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "bob")

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetUserResponseEnvelope) backend.DB
		expectedResponse   *types.GetUserResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get user data request",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetUser(targetUserID), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetUserQuery{UserID: submittingUserName, TargetUserID: targetUserID})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.GetUserResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetUser", submittingUserName, targetUserID).Return(response, nil)
				return db
			},
			expectedResponse: &types.GetUserResponseEnvelope{
				Payload: &types.GetUserResponse{
					Header: &types.ResponseHeader{
						NodeID: "testNodeID",
					},
					Metadata: &types.Metadata{
						Version: &types.Version{
							BlockNum: 1,
							TxNum:    1,
						},
					},
				},
				Signature: []byte{0, 0, 0},
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "invalid get user request missing user header",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetUser(targetUserID), nil)
				if err != nil {
					return nil, err
				}
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetUserQuery{UserID: submittingUserName, TargetUserID: targetUserID})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.GetUserResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "UserID is not set in the http request header",
		},
		{
			name: "invalid get user request missing user's signature",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetUser(targetUserID), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)

				return req, nil
			},
			dbMockFactory: func(response *types.GetUserResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "Signature is not set in the http request header",
		},
		{
			name: "invalid get user request, submitting user doesn't exists",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetUser(targetUserID), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetUserQuery{UserID: submittingUserName, TargetUserID: targetUserID})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.GetUserResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "invalid get user request, failed to verify submitting user signature",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetUser(targetUserID), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, bobSigner, &types.GetUserQuery{UserID: submittingUserName, TargetUserID: targetUserID})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.GetUserResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "invalid get user request, failed to get user's data",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetUser(targetUserID), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetUserQuery{UserID: submittingUserName, TargetUserID: targetUserID})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

				return req, nil
			},
			dbMockFactory: func(response *types.GetUserResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetUser", submittingUserName, targetUserID).Return(nil, errors.New("failed to retrieve user record"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /user/targetUserID' because failed to retrieve user record",
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
			handler := NewUsersRequestHandler(db, logger)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)
			if tt.expectedStatusCode != http.StatusOK {
				respErr := &ResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				res := &types.GetUserResponseEnvelope{}
				err := json.NewDecoder(rr.Body).Decode(res)
				require.NoError(t, err)

				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}

func TestUsersRequestHandler_AddUser(t *testing.T) {
	userID := "testUserID"
	userToDelete := "userToDelete"
	userGet := "userGet"
	userWrite := "userWrite"

	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	userTx := &types.UserAdministrationTx{
		TxID:        "1",
		UserID:      userID,
		UserDeletes: []*types.UserDelete{{UserID: userToDelete}},
		UserReads:   []*types.UserRead{{UserID: userGet}},
		UserWrites: []*types.UserWrite{
			{
				User: &types.User{
					ID:          userWrite,
					Certificate: []byte{0, 0, 0},
					Privilege: &types.Privilege{
						DBPermission: map[string]types.Privilege_Access{
							"testDB": types.Privilege_ReadWrite,
						},
						UserAdministration:    true,
						DBAdministration:      true,
						ClusterAdministration: true,
					},
				},
			},
		},
	}
	aliceSig := testutils.SignatureFromTx(t, aliceSigner, userTx)

	testCases := []struct {
		name                    string
		txEnvFactory            func() *types.UserAdministrationTxEnvelope
		createMockAndInstrument func(t *testing.T, dataTxEnv interface{}) backend.DB
		expectedCode            int
		expectedErr             string
	}{
		{
			name: "submit valid userAdmin transaction",
			txEnvFactory: func() *types.UserAdministrationTxEnvelope {
				return &types.UserAdministrationTxEnvelope{
					Payload:   userTx,
					Signature: aliceSig,
				}
			},
			createMockAndInstrument: func(t *testing.T, txEnv interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", userID).Return(aliceCert, nil)
				db.On("SubmitTransaction", mock.Anything).Run(func(args mock.Arguments) {
					tx, ok := args[0].(*types.UserAdministrationTxEnvelope)
					require.True(t, ok)
					require.Equal(t, txEnv, tx)
				}).Return(nil)
				return db
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "submit userAdmin tx with missing payload",
			txEnvFactory: func() *types.UserAdministrationTxEnvelope {
				return &types.UserAdministrationTxEnvelope{Payload: nil, Signature: aliceSig}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) backend.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing transaction envelope payload (*types.UserAdministrationTx)",
		},
		{
			name: "submit userAdmin tx with missing userID",
			txEnvFactory: func() *types.UserAdministrationTxEnvelope {
				tx := &types.UserAdministrationTx{}
				*tx = *userTx
				tx.UserID = ""
				return &types.UserAdministrationTxEnvelope{Payload: tx, Signature: aliceSig}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) backend.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing UserID in transaction envelope payload (*types.UserAdministrationTx)",
		},
		{
			name: "submit userAdmin tx with missing signature",
			txEnvFactory: func() *types.UserAdministrationTxEnvelope {
				return &types.UserAdministrationTxEnvelope{Payload: userTx, Signature: nil}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) backend.DB {
				db := &mocks.DB{}
				return db
			},
			expectedCode: http.StatusBadRequest,
			expectedErr:  "missing Signature in transaction envelope payload (*types.UserAdministrationTx)",
		},
		{
			name: "bad signature",
			txEnvFactory: func() *types.UserAdministrationTxEnvelope {
				return &types.UserAdministrationTxEnvelope{
					Payload:   userTx,
					Signature: []byte("bad-sig"),
				}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", userID).Return(aliceCert, nil)

				return db
			},
			expectedCode: http.StatusUnauthorized,
			expectedErr:  "signature verification failed",
		},
		{
			name: "no such user",
			txEnvFactory: func() *types.UserAdministrationTxEnvelope {
				tx := &types.UserAdministrationTx{}
				*tx = *userTx
				tx.UserID = "not-alice"
				return &types.UserAdministrationTxEnvelope{
					Payload:   tx,
					Signature: aliceSig,
				}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", "not-alice").Return(nil, errors.New("no such user"))

				return db
			},
			expectedCode: http.StatusUnauthorized,
			expectedErr:  "signature verification failed",
		},
		{
			name: "fail to submit transaction",
			txEnvFactory: func() *types.UserAdministrationTxEnvelope {
				return &types.UserAdministrationTxEnvelope{
					Payload:   userTx,
					Signature: aliceSig,
				}
			},
			createMockAndInstrument: func(t *testing.T, dataTxEnv interface{}) backend.DB {
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

			req, err := http.NewRequest(http.MethodPost, constants.PostUserTx, txReader)
			require.NoError(t, err)
			require.NotNil(t, req)

			rr := httptest.NewRecorder()

			db := tt.createMockAndInstrument(t, txEnv)
			handler := NewUsersRequestHandler(db, logger)
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
