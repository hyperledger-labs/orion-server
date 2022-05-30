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

	"github.com/hyperledger-labs/orion-server/internal/bcdb"
	"github.com/hyperledger-labs/orion-server/internal/bcdb/mocks"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/crypto"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
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
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	dbName := "db1"
	key := "key1"
	version := &types.Version{
		BlockNum: 1,
		TxNum:    1,
	}
	genericResponse := &types.GetHistoricalDataResponseEnvelope{
		Response: &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: "testNodeID",
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("value1"),
				},
			},
		},
	}

	testCases := []testCase{
		{
			name: "valid: GetValues",
			request: constructRequestForTestCase(
				t,
				constants.URLForGetHistoricalData(dbName, key),
				&types.GetHistoricalDataQuery{
					UserId: submittingUserName,
					DbName: dbName,
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
					UserId:      submittingUserName,
					DbName:      dbName,
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
					UserId:  submittingUserName,
					DbName:  dbName,
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
					UserId:     submittingUserName,
					DbName:     dbName,
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
					UserId:    submittingUserName,
					DbName:    dbName,
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
					UserId:    submittingUserName,
					DbName:    dbName,
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
					UserId: submittingUserName,
					DbName: dbName,
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
			assertTestCase(t, tt, &types.GetHistoricalDataResponseEnvelope{})
		})
	}
}

func TestGetDataReaders(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	dbName := "db1"
	key := "key1"
	genericResponse := &types.GetDataReadersResponseEnvelope{
		Response: &types.GetDataReadersResponse{
			Header: &types.ResponseHeader{
				NodeId: "testNodeID",
			},
			ReadBy: map[string]uint32{
				"user1": 5,
				"user2": 6,
			},
		},
	}
	url := constants.URLForGetDataReaders(dbName, key)
	req := constructRequestForTestCase(
		t,
		url,
		&types.GetDataReadersQuery{
			UserId: submittingUserName,
			DbName: dbName,
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
			assertTestCase(t, tt, &types.GetDataReadersResponseEnvelope{})
		})
	}
}

func TestGetDataWriters(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	dbName := "db1"
	key := "key1"
	genericResponse := &types.GetDataWritersResponseEnvelope{
		Response: &types.GetDataWritersResponse{
			Header: &types.ResponseHeader{
				NodeId: "testNodeID",
			},
			WrittenBy: map[string]uint32{
				"user1": 5,
				"user2": 6,
			},
		},
	}
	url := constants.URLForGetDataWriters(dbName, key)
	req := constructRequestForTestCase(
		t,
		url,
		&types.GetDataWritersQuery{
			UserId: submittingUserName,
			DbName: dbName,
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
			assertTestCase(t, tt, &types.GetDataWritersResponseEnvelope{})
		})
	}
}

func TestGetDataReadBy(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	targetUserID := "user1"
	genericResponse := &types.GetDataProvenanceResponseEnvelope{
		Response: &types.GetDataProvenanceResponse{
			Header: &types.ResponseHeader{
				NodeId: "testNodeID",
			},
			DBKeyValues: map[string]*types.KVsWithMetadata{
				"db1": {
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key1",
							Value: []byte("value1"),
						},
					},
				},
			},
		},
	}

	url := constants.URLForGetDataReadBy(targetUserID)
	req := constructRequestForTestCase(
		t,
		url,
		&types.GetDataReadByQuery{
			UserId:       submittingUserName,
			TargetUserId: targetUserID,
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
			assertTestCase(t, tt, &types.GetDataProvenanceResponseEnvelope{})
		})
	}
}

func TestGetDataWrittenBy(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	targetUserID := "user1"
	genericResponse := &types.GetDataProvenanceResponseEnvelope{
		Response: &types.GetDataProvenanceResponse{
			Header: &types.ResponseHeader{
				NodeId: "testNodeID",
			},
			DBKeyValues: map[string]*types.KVsWithMetadata{
				"db1": {
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key1",
							Value: []byte("value1"),
						},
					},
				},
			},
		},
	}

	url := constants.URLForGetDataWrittenBy(targetUserID)
	req := constructRequestForTestCase(
		t,
		url,
		&types.GetDataWrittenByQuery{
			UserId:       submittingUserName,
			TargetUserId: targetUserID,
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
			assertTestCase(t, tt, &types.GetDataProvenanceResponseEnvelope{})
		})
	}
}

func TestGetDataDeletedBy(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	targetUserID := "user1"
	genericResponse := &types.GetDataProvenanceResponseEnvelope{
		Response: &types.GetDataProvenanceResponse{
			Header: &types.ResponseHeader{
				NodeId: "testNodeID",
			},
			DBKeyValues: map[string]*types.KVsWithMetadata{
				"db1": {
					KVs: []*types.KVWithMetadata{
						{
							Key:   "key1",
							Value: []byte("value1"),
						},
					},
				},
			},
		},
	}

	url := constants.URLForGetDataDeletedBy(targetUserID)
	req := constructRequestForTestCase(
		t,
		url,
		&types.GetDataDeletedByQuery{
			UserId:       submittingUserName,
			TargetUserId: targetUserID,
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
			assertTestCase(t, tt, &types.GetDataProvenanceResponseEnvelope{})
		})
	}
}

func TestGetTxIDsSubmittedBy(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	targetUserID := "user1"
	genericResponse := &types.GetTxIDsSubmittedByResponseEnvelope{
		Response: &types.GetTxIDsSubmittedByResponse{
			Header: &types.ResponseHeader{
				NodeId: "testNodeID",
			},
			TxIDs: []string{"tx1", "tx5"},
		},
	}

	url := constants.URLForGetTxIDsSubmittedBy(targetUserID)
	req := constructRequestForTestCase(
		t,
		url,
		&types.GetTxIDsSubmittedByQuery{
			UserId:       submittingUserName,
			TargetUserId: targetUserID,
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
			assertTestCase(t, tt, &types.GetTxIDsSubmittedByResponseEnvelope{})
		})
	}
}

func TestGetMostRecentNodeOrUser(t *testing.T) {
	t.Parallel()

	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	sampleVer := &types.Version{
		BlockNum: 5,
		TxNum:    10,
	}

	nodeResponse := &types.GetHistoricalDataResponseEnvelope{
		Response: &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: "testNodeID",
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						Version: sampleVer,
					},
				},
			},
		},
	}
	userResponse := &types.GetHistoricalDataResponseEnvelope{
		Response: &types.GetHistoricalDataResponse{
			Header: &types.ResponseHeader{
				NodeId: "testNodeID",
			},
			Values: []*types.ValueWithMetadata{
				{
					Value: []byte("value1"),
					Metadata: &types.Metadata{
						Version: sampleVer,
					},
				},
			},
		},
	}

	testCases := []testCase{
		{
			name: "valid: node request",
			request: constructRequestForTestCase(
				t,
				constants.URLForGetMostRecentNodeConfig("node1", sampleVer),
				&types.GetMostRecentUserOrNodeQuery{
					Type:    types.GetMostRecentUserOrNodeQuery_NODE,
					UserId:  submittingUserName,
					Id:      "node1",
					Version: sampleVer,
				},
				aliceSigner,
				submittingUserName,
			),
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetMostRecentValueAtOrBelow", worldstate.ConfigDBName, "node1", sampleVer).Return(nodeResponse, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   nodeResponse,
		},
		{
			name: "valid: user request",
			request: constructRequestForTestCase(
				t,
				constants.URLForGetMostRecentUserInfo("user1", sampleVer),
				&types.GetMostRecentUserOrNodeQuery{
					Type:    types.GetMostRecentUserOrNodeQuery_USER,
					UserId:  submittingUserName,
					Id:      "user1",
					Version: sampleVer,
				},
				aliceSigner,
				submittingUserName,
			),
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetMostRecentValueAtOrBelow", worldstate.UsersDBName, "user1", sampleVer).Return(userResponse, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			expectedResponse:   userResponse,
		},
		{
			name: "internal server error",
			request: constructRequestForTestCase(
				t,
				constants.URLForGetMostRecentUserInfo("user1", sampleVer),
				&types.GetMostRecentUserOrNodeQuery{
					Type:    types.GetMostRecentUserOrNodeQuery_USER,
					UserId:  submittingUserName,
					Id:      "user1",
					Version: sampleVer,
				},
				aliceSigner,
				submittingUserName,
			),
			dbMockFactory: func(response interface{}) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetMostRecentValueAtOrBelow", worldstate.UsersDBName, "user1", sampleVer).Return(nil, errors.New("error in provenance db"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET " + constants.URLForGetMostRecentUserInfo("user1", sampleVer) + "' because error in provenance db",
		},
		constructTestCaseForSigVerificationFailure(t, constants.URLForGetMostRecentUserInfo("user1", sampleVer), submittingUserName),
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			assertTestCase(t, tt, &types.GetHistoricalDataResponseEnvelope{})
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
