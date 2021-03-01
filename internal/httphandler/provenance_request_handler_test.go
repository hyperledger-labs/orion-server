// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.ibm.com/blockchaindb/server/internal/bcdb"
	"github.ibm.com/blockchaindb/server/internal/bcdb/mocks"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/server/testutils"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

type testCase struct {
	name               string
	request            *http.Request
	dbMockFactory      func(response interface{}) bcdb.DB
	expectedStatusCode int
	expectedResponse   interface{}
	expectedErr        string
}

func TestGetHistoricalData(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	dbName := "db1"
	key := "key1"
	version := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}
	genericResponse := &types.ResponseEnvelope{
		Payload: MarshalOrPanic(&types.Payload{
			Header: &types.ResponseHeader{
				NodeID: "testNodeID",
			},
			Response: MarshalOrPanic(&types.GetHistoricalDataResponse{
				Values: []*types.ValueWithMetadata{
					{
						Value: []byte("value1"),
					},
				},
			}),
		}),
	}

	testCases := []testCase{
		{
			name: "valid: GetValues",
			request: constructRequestForTestCase(
				t,
				constants.URLForGetHistoricalData(dbName, key),
				&types.GetHistoricalDataQuery{
					UserID: submittingUserName,
					DBName: dbName,
					Key:    key,
				},
				aliceSigner,
				submittingUserName,
			),
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetValues", dbName, key).Return(genericResponse, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   genericResponse,
		},
		{
			name: "valid: GetDeletedValues",
			request: constructRequestForTestCase(
				t,
				constants.URLForGetHistoricalDeletedData(dbName, key),
				&types.GetHistoricalDataQuery{
					UserID:      submittingUserName,
					DBName:      dbName,
					Key:         key,
					OnlyDeletes: true,
				},
				aliceSigner,
				submittingUserName,
			),
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDeletedValues", dbName, key).Return(genericResponse, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   genericResponse,
		},
		{
			name: "valid: GetValueAt",
			request: constructRequestForTestCase(
				t,
				constants.URLForGetHistoricalDataAt(dbName, key, version),
				&types.GetHistoricalDataQuery{
					UserID:  submittingUserName,
					DBName:  dbName,
					Key:     key,
					Version: version,
				},
				aliceSigner,
				submittingUserName,
			),
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetValueAt", dbName, key, version).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   genericResponse,
		},
		{
			name: "valid: GetMostRecentValueAtOrBelow",
			request: constructRequestForTestCase(
				t,
				constants.URLForGetHistoricalDataAtOrBelow(dbName, key, version),
				&types.GetHistoricalDataQuery{
					UserID:     submittingUserName,
					DBName:     dbName,
					Key:        key,
					Version:    version,
					MostRecent: true,
				},
				aliceSigner,
				submittingUserName,
			),
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetMostRecentValueAtOrBelow", dbName, key, version).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   genericResponse,
		},
		{
			name: "valid: GetPreviousValues",
			request: constructRequestForTestCase(
				t,
				constants.URLForGetPreviousHistoricalData(dbName, key, version),
				&types.GetHistoricalDataQuery{
					UserID:    submittingUserName,
					DBName:    dbName,
					Key:       key,
					Version:   version,
					Direction: "previous",
				},
				aliceSigner,
				submittingUserName,
			),
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetPreviousValues", dbName, key, version).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   genericResponse,
		},
		{
			name: "valid: GetNextValues",
			request: constructRequestForTestCase(
				t,
				constants.URLForGetNextHistoricalData(dbName, key, version),
				&types.GetHistoricalDataQuery{
					UserID:    submittingUserName,
					DBName:    dbName,
					Key:       key,
					Version:   version,
					Direction: "next",
				},
				aliceSigner,
				submittingUserName,
			),
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetNextValues", dbName, key, version).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   genericResponse,
		},
		{
			name: "internal server error",
			request: constructRequestForTestCase(
				t,
				constants.URLForGetHistoricalData(dbName, key),
				&types.GetHistoricalDataQuery{
					UserID: submittingUserName,
					DBName: dbName,
					Key:    key,
				},
				aliceSigner,
				submittingUserName,
			),
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetValues", dbName, key).Return(nil, errors.New("error in provenance db"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET " + constants.URLForGetHistoricalData(dbName, key) + "' because error in provenance db",
		},
		constructTestCaseForSigVerificationFailure(t, constants.URLForGetHistoricalData(dbName, key), submittingUserName),
	}

	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			assertTestCase(t, tt, &types.ResponseEnvelope{})
		})
	}
}

func TestGetDataReaders(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	dbName := "db1"
	key := "key1"
	genericResponse := &types.ResponseEnvelope{
		Payload: MarshalOrPanic(&types.Payload{
			Header: &types.ResponseHeader{
				NodeID: "testNodeID",
			},
			Response: MarshalOrPanic(&types.GetDataReadersResponse{
				ReadBy: map[string]uint32{
					"user1": 5,
					"user2": 6,
				},
			}),
		}),
	}
	url := constants.URLForGetDataReaders(dbName, key)
	req := constructRequestForTestCase(
		t,
		url,
		&types.GetDataReadersQuery{
			UserID: submittingUserName,
			DBName: dbName,
			Key:    key,
		},
		aliceSigner,
		submittingUserName,
	)

	testCases := []testCase{
		{
			name:    "valid",
			request: req,
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetReaders", dbName, key).Return(genericResponse, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   genericResponse,
		},
		{
			name:    "internal server error",
			request: req,
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetReaders", dbName, key).Return(nil, errors.New("error in provenance db"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET " + url + "' because error in provenance db",
		},
		constructTestCaseForSigVerificationFailure(t, url, submittingUserName),
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			assertTestCase(t, tt, &types.ResponseEnvelope{})
		})
	}
}

func TestGetDataWriters(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	dbName := "db1"
	key := "key1"
	genericResponse := &types.ResponseEnvelope{
		Payload: MarshalOrPanic(&types.Payload{
			Header: &types.ResponseHeader{
				NodeID: "testNodeID",
			},
			Response: MarshalOrPanic(&types.GetDataWritersResponse{
				WrittenBy: map[string]uint32{
					"user1": 5,
					"user2": 6,
				},
			}),
		}),
	}
	url := constants.URLForGetDataWriters(dbName, key)
	req := constructRequestForTestCase(
		t,
		url,
		&types.GetDataWritersQuery{
			UserID: submittingUserName,
			DBName: dbName,
			Key:    key,
		},
		aliceSigner,
		submittingUserName,
	)

	testCases := []testCase{
		{
			name:    "valid",
			request: req,
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetWriters", dbName, key).Return(genericResponse, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   genericResponse,
		},
		{
			name:    "internal server error",
			request: req,
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetWriters", dbName, key).Return(nil, errors.New("error in provenance db"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET " + url + "' because error in provenance db",
		},
		constructTestCaseForSigVerificationFailure(t, url, submittingUserName),
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			assertTestCase(t, tt, &types.ResponseEnvelope{})
		})
	}
}

func TestGetDataReadBy(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	targetUserID := "user1"
	genericResponse := &types.ResponseEnvelope{
		Payload: MarshalOrPanic(&types.Payload{
			Header: &types.ResponseHeader{
				NodeID: "testNodeID",
			},
			Response: MarshalOrPanic(&types.GetDataProvenanceResponse{
				KVs: []*types.KVWithMetadata{
					{
						Key:   "key1",
						Value: []byte("value1"),
					},
				},
			}),
		}),
	}

	url := constants.URLForGetDataReadBy(targetUserID)
	req := constructRequestForTestCase(
		t,
		url,
		&types.GetDataReadByQuery{
			UserID:       submittingUserName,
			TargetUserID: targetUserID,
		},
		aliceSigner,
		submittingUserName,
	)

	testCases := []testCase{
		{
			name:    "valid",
			request: req,
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetValuesReadByUser", targetUserID).Return(genericResponse, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   genericResponse,
		},
		{
			name:    "internal server error",
			request: req,
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetValuesReadByUser", targetUserID).Return(nil, errors.New("error in provenance db"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET " + url + "' because error in provenance db",
		},
		constructTestCaseForSigVerificationFailure(t, url, submittingUserName),
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			assertTestCase(t, tt, &types.ResponseEnvelope{})
		})
	}
}

func TestGetDataWrittenBy(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	targetUserID := "user1"
	genericResponse := &types.ResponseEnvelope{
		Payload: MarshalOrPanic(&types.Payload{
			Header: &types.ResponseHeader{
				NodeID: "testNodeID",
			},
			Response: MarshalOrPanic(&types.GetDataProvenanceResponse{
				KVs: []*types.KVWithMetadata{
					{
						Key:   "key1",
						Value: []byte("value1"),
					},
				},
			}),
		}),
	}

	url := constants.URLForGetDataWrittenBy(targetUserID)
	req := constructRequestForTestCase(
		t,
		url,
		&types.GetDataWrittenByQuery{
			UserID:       submittingUserName,
			TargetUserID: targetUserID,
		},
		aliceSigner,
		submittingUserName,
	)

	testCases := []testCase{
		{
			name:    "valid",
			request: req,
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetValuesWrittenByUser", targetUserID).Return(genericResponse, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   genericResponse,
		},
		{
			name:    "internal server error",
			request: req,
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetValuesWrittenByUser", targetUserID).Return(nil, errors.New("error in provenance db"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET " + url + "' because error in provenance db",
		},
		constructTestCaseForSigVerificationFailure(t, url, submittingUserName),
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			assertTestCase(t, tt, &types.ResponseEnvelope{})
		})
	}
}

func TestGetDataDeletedBy(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	targetUserID := "user1"
	genericResponse := &types.ResponseEnvelope{
		Payload: MarshalOrPanic(&types.Payload{
			Header: &types.ResponseHeader{
				NodeID: "testNodeID",
			},
			Response: MarshalOrPanic(&types.GetDataProvenanceResponse{
				KVs: []*types.KVWithMetadata{
					{
						Key:   "key1",
						Value: []byte("value1"),
					},
				},
			}),
		}),
	}

	url := constants.URLForGetDataDeletedBy(targetUserID)
	req := constructRequestForTestCase(
		t,
		url,
		&types.GetDataDeletedByQuery{
			UserID:       submittingUserName,
			TargetUserID: targetUserID,
		},
		aliceSigner,
		submittingUserName,
	)

	testCases := []testCase{
		{
			name:    "valid",
			request: req,
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetValuesDeletedByUser", targetUserID).Return(genericResponse, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   genericResponse,
		},
		{
			name:    "internal server error",
			request: req,
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetValuesDeletedByUser", targetUserID).Return(nil, errors.New("error in provenance db"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET " + url + "' because error in provenance db",
		},
		constructTestCaseForSigVerificationFailure(t, url, submittingUserName),
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			assertTestCase(t, tt, &types.ResponseEnvelope{})
		})
	}
}

func TestGetTxIDsSubmittedBy(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestClientCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestClientCrypto(t, cryptoDir, "alice")

	targetUserID := "user1"
	genericResponse := &types.ResponseEnvelope{
		Payload: MarshalOrPanic(&types.Payload{
			Header: &types.ResponseHeader{
				NodeID: "testNodeID",
			},
			Response: MarshalOrPanic(&types.GetTxIDsSubmittedByResponse{
				TxIDs: []string{"tx1", "tx5"},
			}),
		}),
	}

	url := constants.URLForGetTxIDsSubmittedBy(targetUserID)
	req := constructRequestForTestCase(
		t,
		url,
		&types.GetTxIDsSubmittedByQuery{
			UserID:       submittingUserName,
			TargetUserID: targetUserID,
		},
		aliceSigner,
		submittingUserName,
	)

	testCases := []testCase{
		{
			name:    "valid",
			request: req,
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetTxIDsSubmittedByUser", targetUserID).Return(genericResponse, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   genericResponse,
		},
		{
			name:    "internal server error",
			request: req,
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetTxIDsSubmittedByUser", targetUserID).Return(nil, errors.New("error in provenance db"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET " + url + "' because error in provenance db",
		},
		constructTestCaseForSigVerificationFailure(t, url, submittingUserName),
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			assertTestCase(t, tt, &types.ResponseEnvelope{})
		})
	}
}

func assertTestCase(t *testing.T, tt testCase, responseType interface{}) {
	logger, err := createLogger("debug")
	require.NoError(t, err)
	require.NotNil(t, logger)

	db := tt.dbMockFactory(tt.expectedResponse)
	rr := httptest.NewRecorder()
	handler := NewProvenanceRequestHandler(db, logger)
	handler.ServeHTTP(rr, tt.request)

	require.Equal(t, tt.expectedStatusCode, rr.Code)
	if tt.expectedStatusCode != http.StatusOK {
		respErr := &types.HttpResponseErr{}
		err := json.NewDecoder(rr.Body).Decode(respErr)
		require.NoError(t, err)
		require.Equal(t, tt.expectedErr, respErr.ErrMsg)
	}

	if tt.expectedResponse != nil {
		err = json.NewDecoder(rr.Body).Decode(responseType)
		require.NoError(t, err)
		require.Equal(t, tt.expectedResponse, responseType)
	}
}

func constructRequestForTestCase(t *testing.T, url string, query interface{}, signer crypto.Signer, signerID string) *http.Request {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)

	sig := testutils.SignatureFromQuery(
		t,
		signer,
		query,
	)

	req.Header.Set(constants.UserHeader, signerID)
	req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))

	return req
}

func constructTestCaseForSigVerificationFailure(t *testing.T, url string, submittingUserName string) testCase {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	req.Header.Set(constants.UserHeader, submittingUserName)
	req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte("random")))

	return testCase{
		name:    "submitting user does not exist",
		request: req,
		dbMockFactory: func(response interface{}) bcdb.DB {
			db := &mocks.DB{}
			db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"), nil)
			return db
		},
		expectedStatusCode: http.StatusUnauthorized,
		expectedErr:        "signature verification failed",
	}
}
