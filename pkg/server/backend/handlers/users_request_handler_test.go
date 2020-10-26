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

func TestUsersRequestHandler_GetUser(t *testing.T) {
	submittingUserName := "admin"
	testUserID := "testUserID"

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetUserResponseEnvelope) backend.DB
		expectedResponse   *types.GetUserResponseEnvelope
		expectedStatusCode int
	}{
		{
			name: "valid get user data request",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetUser(testUserID), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))

				return req, nil
			},
			dbMockFactory: func(response *types.GetUserResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(true, nil)
				db.On("GetUser", submittingUserName, testUserID).Return(response, nil)
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
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetUser(testUserID), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))

				return req, nil
			},
			dbMockFactory: func(response *types.GetUserResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(true, nil)
				db.On("GetUser", submittingUserName, testUserID).Return(response, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "invalid get user request missing user's signature",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetUser(testUserID), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)

				return req, nil
			},
			dbMockFactory: func(response *types.GetUserResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(true, nil)
				db.On("GetUser", submittingUserName, testUserID).Return(response, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusBadRequest,
		},
		{
			name: "invalid get user request, submitting user doesn't exists",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetUser(testUserID), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))

				return req, nil
			},
			dbMockFactory: func(response *types.GetUserResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(false, nil)
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusForbidden,
		},
		{
			name: "invalid get user request, cannot retrieve submitting user record",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetUser(testUserID), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))

				return req, nil
			},
			dbMockFactory: func(response *types.GetUserResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(false, errors.New("failed to get user's record"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
		},
		{
			name: "invalid get user request, failed to get user's data",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetUser(testUserID), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))

				return req, nil
			},
			dbMockFactory: func(response *types.GetUserResponseEnvelope) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).Return(true, nil)
				db.On("GetUser", submittingUserName, testUserID).Return(nil, errors.New("failed to retrieve user record"))
				return db
			},
			expectedResponse:   nil,
			expectedStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req, err := tt.requestFactory()
			require.NoError(t, err)
			require.NotNil(t, req)

			db := tt.dbMockFactory(tt.expectedResponse)
			handler := NewUsersRequestHandler(db)
			rr := httptest.NewRecorder()

			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)

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

	testCases := []struct {
		name                    string
		userTx                  interface{}
		createMockAndInstrument func(t *testing.T, dataTx interface{}) backend.DB
		expectedCode            int
	}{
		{
			name: "submit valid db transaction",
			userTx: &types.UserAdministrationTxEnvelope{
				Payload: &types.UserAdministrationTx{
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
				},
				Signature: []byte{0, 0, 0},
			},
			createMockAndInstrument: func(t *testing.T, dataTx interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", userID).Return(true, nil)
				db.On("SubmitTransaction", mock.Anything).Run(func(args mock.Arguments) {
					tx, ok := args[0].(*types.UserAdministrationTxEnvelope)
					require.True(t, ok)
					require.Equal(t, dataTx, tx)
				}).Return(nil)
				return db
			},
			expectedCode: http.StatusOK,
		},
		{
			name: "submit configuration with missing userID",
			userTx: &types.UserAdministrationTxEnvelope{
				Payload: &types.UserAdministrationTx{
					TxID:        "1",
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
				},
				Signature: []byte{0, 0, 0},
			},
			createMockAndInstrument: func(t *testing.T, dataTx interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", mock.Anything).Return(false, nil)
				return db
			},
			expectedCode: http.StatusForbidden,
		},
		{
			name: "fail to retrieve information about submitting user",
			userTx: &types.UserAdministrationTxEnvelope{
				Payload: &types.UserAdministrationTx{
					TxID:        "1",
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
				},
				Signature: []byte{0, 0, 0},
			},
			createMockAndInstrument: func(t *testing.T, dataTx interface{}) backend.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", mock.Anything).Return(false, errors.New("fail to retrieve user's record"))
				return db
			},
			expectedCode: http.StatusBadRequest,
		},
		{
			name:   "fail to submit transaction",
			userTx: &types.UserAdministrationTxEnvelope{},
			createMockAndInstrument: func(t *testing.T, configTx interface{}) backend.DB {
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
			txBytes, err := json.Marshal(tt.userTx)
			require.NoError(t, err)
			require.NotNil(t, txBytes)

			txReader := bytes.NewReader(txBytes)
			require.NotNil(t, txReader)

			req, err := http.NewRequest(http.MethodPost, constants.PostUserTx, txReader)
			require.NoError(t, err)
			require.NotNil(t, req)

			rr := httptest.NewRecorder()

			db := tt.createMockAndInstrument(t, tt.userTx)
			handler := NewUsersRequestHandler(db)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedCode, rr.Code)
		})
	}
}
