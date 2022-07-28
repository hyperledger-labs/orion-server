// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/hyperledger-labs/orion-server/internal/bcdb"
	"github.com/hyperledger-labs/orion-server/internal/bcdb/mocks"
	interrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/server/testutils"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestBlockQuery(t *testing.T) {
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	testCases := []struct {
		name           string
		requestFactory func() (*http.Request, error)
		dbMockFactory  func(response proto.Message) bcdb.DB
		//		dbMockFactory      func(response *types.GetBlockResponseEnvelope) bcdb.DB
		expectedResponse proto.Message
		//		expectedResponse   *types.GetBlockResponseEnvelope
		augmentedHeader    bool
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get header request",
			expectedResponse: &types.GetBlockResponseEnvelope{
				Response: &types.GetBlockResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					BlockHeader: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
					},
				},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerBlock(1, false), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetBlockQuery{UserId: submittingUserName, BlockNumber: 1})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response proto.Message) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetBlockHeader", submittingUserName, uint64(1)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			augmentedHeader:    false,
		},
		{
			name: "valid get header request with augmented=false",
			expectedResponse: &types.GetBlockResponseEnvelope{
				Response: &types.GetBlockResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					BlockHeader: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 1,
						},
					},
				},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.LedgerEndpoint+fmt.Sprintf("block/%d?augmented=%t", 1, false), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetBlockQuery{UserId: submittingUserName, BlockNumber: 1})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response proto.Message) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetBlockHeader", submittingUserName, uint64(1)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			augmentedHeader:    false,
		},

		{
			name: "valid get augmented header request",
			expectedResponse: &types.GetAugmentedBlockHeaderResponseEnvelope{
				Response: &types.GetAugmentedBlockHeaderResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					BlockHeader: &types.AugmentedBlockHeader{
						Header: &types.BlockHeader{
							BaseHeader: &types.BlockHeaderBase{
								Number: 1,
							},
							ValidationInfo: []*types.ValidationInfo{
								{
									Flag: types.Flag_VALID,
								},
								{
									Flag: types.Flag_VALID,
								},
							},
						},
						TxIds: []string{
							"tx1",
							"tx2",
						},
					},
				},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerBlock(1, true), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetBlockQuery{
					UserId:      submittingUserName,
					BlockNumber: 1,
					Augmented:   true,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response proto.Message) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetAugmentedBlockHeader", submittingUserName, uint64(1)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
			augmentedHeader:    true,
		},
		{
			name: "valid get last block header request",
			expectedResponse: &types.GetBlockResponseEnvelope{
				Response: &types.GetBlockResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					BlockHeader: &types.BlockHeader{
						BaseHeader: &types.BlockHeaderBase{
							Number: 100,
						},
					},
				},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLastLedgerBlock(), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetLastBlockQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response proto.Message) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetBlockHeader", submittingUserName, uint64(100)).Return(response, nil)
				db.On("Height").Return(uint64(100), nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:             "user doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerBlock(1, false), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetBlockQuery{UserId: submittingUserName, BlockNumber: 1})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response proto.Message) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name: "no block exist",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerBlock(1, false), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetBlockQuery{UserId: submittingUserName, BlockNumber: 1})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response proto.Message) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetBlockHeader", submittingUserName, uint64(1)).Return(nil, &interrors.NotFoundErr{Message: "block not found: 1"})
				return db
			},
			expectedStatusCode: http.StatusNotFound,
			expectedErr:        "error while processing 'GET /ledger/block/1' because block not found: 1",
			augmentedHeader:    false,
		},
		{
			name: "ledger height returns error",
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLastLedgerBlock(), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetLastBlockQuery{UserId: submittingUserName})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response proto.Message) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetBlockHeader", submittingUserName, uint64(1)).Return(nil, &interrors.NotFoundErr{Message: "block not found: 1"})
				db.On("Height").Return(uint64(0), errors.Errorf("unable to retrieve the state database height due to missing metadataDB"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /ledger/block/last' because unable to retrieve the state database height due to missing metadataDB",
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
			handler := NewLedgerRequestHandler(db, logger)
			handler.ServeHTTP(rr, req)

			require.Equal(t, tt.expectedStatusCode, rr.Code)
			if tt.expectedStatusCode != http.StatusOK {
				respErr := &types.HttpResponseErr{}
				err := json.NewDecoder(rr.Body).Decode(respErr)
				require.NoError(t, err)
				require.Equal(t, tt.expectedErr, respErr.ErrMsg)
			}

			if tt.expectedResponse != nil {
				var res proto.Message
				if tt.augmentedHeader {
					res = &types.GetAugmentedBlockHeaderResponseEnvelope{}
				} else {
					res = &types.GetBlockResponseEnvelope{}
				}

				requestBody, err := ioutil.ReadAll(rr.Body)
				require.NoError(t, err)
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
				//TODO verify signature on response
			}
		})
	}
}

func TestPathQuery(t *testing.T) {
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetLedgerPathResponseEnvelope) bcdb.DB
		expectedResponse   *types.GetLedgerPathResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get path request",
			expectedResponse: &types.GetLedgerPathResponseEnvelope{
				Response: &types.GetLedgerPathResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
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
				Signature: []byte{0, 0, 0},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerPath(1, 2), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetLedgerPathQuery{
					UserId:           submittingUserName,
					StartBlockNumber: 1,
					EndBlockNumber:   2,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
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
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetLedgerPathQuery{
					UserId:           submittingUserName,
					StartBlockNumber: 1,
					EndBlockNumber:   2,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				db.On("GetLedgerPath", submittingUserName, uint64(1), uint64(2)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
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
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetLedgerPathQuery{
					UserId:           submittingUserName,
					StartBlockNumber: 1,
					EndBlockNumber:   2,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetLedgerPath", submittingUserName, uint64(1), uint64(2)).Return(response, errors.Errorf("can't find path in blocks skip list between 2 1"))
				return db
			},
			expectedStatusCode: http.StatusInternalServerError,
			expectedErr:        "error while processing 'GET /ledger/path?start=1&end=2' because can't find path in blocks skip list between 2 1",
		},
		{
			name:             "end block not found",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerPath(1, 10), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetLedgerPathQuery{
					UserId:           submittingUserName,
					StartBlockNumber: 1,
					EndBlockNumber:   10,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetLedgerPath", submittingUserName, uint64(1), uint64(10)).Return(response, &interrors.NotFoundErr{Message: "can't find path in blocks skip list between 10 1: block not found: 10"})
				return db
			},
			expectedStatusCode: http.StatusNotFound,
			expectedErr:        "error while processing 'GET /ledger/path?start=1&end=10' because can't find path in blocks skip list between 10 1: block not found: 10",
		},
		{
			name:             "wrong url, endId not exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.LedgerEndpoint+fmt.Sprintf("path?start=%s", "1"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("DoesUserExist", submittingUserName).
					Return(true, nil)
				db.On("GetLedgerPath", submittingUserName, uint64(1), uint64(2)).Return(response, errors.Errorf("can't find path in blocks skip list between 2 1"))
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "query error - bad or missing start/end block number",
		},
		{
			name:             "endId < startId",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForLedgerPath(10, 1), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetLedgerPathQuery{
					UserId:           submittingUserName,
					StartBlockNumber: 10,
					EndBlockNumber:   1,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetLedgerPathResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "query error: startId=10 > endId=1",
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
			handler := NewLedgerRequestHandler(db, logger)
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
				res := &types.GetLedgerPathResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}

func TestTxProofQuery(t *testing.T) {
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetTxProofResponseEnvelope) bcdb.DB
		expectedResponse   *types.GetTxProofResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get path request, single digit",
			expectedResponse: &types.GetTxProofResponseEnvelope{
				Response: &types.GetTxProofResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					Hashes: [][]byte{[]byte("hash1"), []byte("hash2")},
				},
				Signature: []byte{0, 0, 0},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLTxProof(2, 1), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxProofQuery{
					UserId:      submittingUserName,
					BlockNumber: 2,
					TxIndex:     1,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetTxProof", submittingUserName, uint64(2), uint64(1)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "valid get path request, multi digit",
			expectedResponse: &types.GetTxProofResponseEnvelope{
				Response: &types.GetTxProofResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					Hashes: [][]byte{[]byte("hash1"), []byte("hash2")},
				},
				Signature: []byte{0, 0, 0},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLTxProof(22, 11), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxProofQuery{
					UserId:      submittingUserName,
					BlockNumber: 22,
					TxIndex:     11,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetTxProof", submittingUserName, uint64(22), uint64(11)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:             "user doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLTxProof(2, 1), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxProofQuery{
					UserId:      submittingUserName,
					BlockNumber: 2,
					TxIndex:     1,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				db.On("GetTxProof", submittingUserName, uint64(2), uint64(1)).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name:             "no tx exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLTxProof(2, 2), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxProofQuery{
					UserId:      submittingUserName,
					BlockNumber: 2,
					TxIndex:     2,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetTxProof", submittingUserName, uint64(2), uint64(2)).Return(response, &interrors.NotFoundErr{Message: "block not found: 2"})
				return db
			},
			expectedStatusCode: http.StatusNotFound,
			expectedErr:        "error while processing 'GET /ledger/proof/tx/2?idx=2' because block not found: 2",
		},
		{
			name:             "wrong url, idx param doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, path.Join(constants.LedgerEndpoint, "proof", "tx", "2"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "tx proof query error - bad or missing query parameter",
		},
		{
			name:             "wrong url, blockId idx params doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, path.Join(constants.LedgerEndpoint, "proof", "tx"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetTxProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "tx proof query error - bad or missing query parameter",
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
			handler := NewLedgerRequestHandler(db, logger)
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
				res := &types.GetTxProofResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}

func TestDataProofQuery(t *testing.T) {
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.GetDataProofResponseEnvelope) bcdb.DB
		expectedResponse   *types.GetDataProofResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get proof request, deleted false",
			expectedResponse: &types.GetDataProofResponseEnvelope{
				Response: &types.GetDataProofResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					Path: []*types.MPTrieProofElement{
						{
							Hashes: [][]byte{[]byte("hash1"), []byte("hash2")},
						},
						{
							Hashes: [][]byte{[]byte("hash3"), []byte("hash4")},
						},
						{
							Hashes: [][]byte{[]byte("hash5")},
						},
					},
				},
				Signature: []byte{0, 0, 0},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLDataProof(2, "bdb", "key1", false), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDataProofQuery{
					UserId:      submittingUserName,
					BlockNumber: 2,
					DbName:      "bdb",
					Key:         "key1",
					IsDeleted:   false,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDataProof", submittingUserName, uint64(2), "bdb", "key1", false).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name: "valid get proof request, deleted true",
			expectedResponse: &types.GetDataProofResponseEnvelope{
				Response: &types.GetDataProofResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					Path: []*types.MPTrieProofElement{
						{
							Hashes: [][]byte{[]byte("hash1"), []byte("hash2")},
						},
						{
							Hashes: [][]byte{[]byte("hash3"), []byte("hash4")},
						},
						{
							Hashes: [][]byte{[]byte("hash5")},
						},
					},
				},
				Signature: []byte{0, 0, 0},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLDataProof(2, "bdb", "key1", true), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDataProofQuery{
					UserId:      submittingUserName,
					BlockNumber: 2,
					DbName:      "bdb",
					Key:         "key1",
					IsDeleted:   true,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDataProof", submittingUserName, uint64(2), "bdb", "key1", true).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:             "user doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLDataProof(2, "bdb", "key1", true), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDataProofQuery{
					UserId:      submittingUserName,
					BlockNumber: 2,
					DbName:      "bdb",
					Key:         "key1",
					IsDeleted:   false,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				db.On("GetDataProof", submittingUserName, uint64(2), "bdb", "key1", false).Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name:             "no key exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLDataProof(2, "bdb", "key1", false), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDataProofQuery{
					UserId:      submittingUserName,
					BlockNumber: 2,
					DbName:      "bdb",
					Key:         "key1",
					IsDeleted:   false,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDataProof", submittingUserName, uint64(2), "bdb", "key1", false).Return(response, &interrors.NotFoundErr{Message: "no proof for block 2, db bdb, key key1, isDeleted false found"})
				return db
			},
			expectedStatusCode: http.StatusNotFound,
			expectedErr:        "error while processing 'GET /ledger/proof/data/bdb/key1?block=2' because no proof for block 2, db bdb, key key1, isDeleted false found",
		},
		{
			name:             "no key exist, deleted is true",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLDataProof(2, "bdb", "key1", true), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetDataProofQuery{
					UserId:      submittingUserName,
					BlockNumber: 2,
					DbName:      "bdb",
					Key:         "key1",
					IsDeleted:   true,
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetDataProof", submittingUserName, uint64(2), "bdb", "key1", true).Return(response, &interrors.NotFoundErr{Message: "no proof for block 2, db bdb, key key1, isDeleted true found"})
				return db
			},
			expectedStatusCode: http.StatusNotFound,
			expectedErr:        "error while processing 'GET /ledger/proof/data/bdb/key1?block=2&deleted=true' because no proof for block 2, db bdb, key key1, isDeleted true found",
		},
		{
			name:             "wrong url, block param missing",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, path.Join(constants.LedgerEndpoint, "proof", "data", "bdb", "key"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "data proof query error - bad or missing query parameter",
		},
		{
			name:             "wrong url, key param missing",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, path.Join(constants.LedgerEndpoint, "proof", "data", "bdb"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "data proof query error - bad or missing query parameter",
		},
		{
			name:             "wrong url, db and key param missing",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, path.Join(constants.LedgerEndpoint, "proof", "data"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "data proof query error - bad or missing query parameter",
		},
		{
			name:             "wrong url, blockId, db and key param missing",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, path.Join(constants.LedgerEndpoint, "proof", "data"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString([]byte{0}))
				return req, nil
			},
			dbMockFactory: func(response *types.GetDataProofResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				return db
			},
			expectedStatusCode: http.StatusBadRequest,
			expectedErr:        "data proof query error - bad or missing query parameter",
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
			handler := NewLedgerRequestHandler(db, logger)
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
				res := &types.GetDataProofResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}

func TestTxReceiptQuery(t *testing.T) {
	submittingUserName := "alice"
	cryptoDir := testutils.GenerateTestCrypto(t, []string{"alice"})
	aliceCert, aliceSigner := testutils.LoadTestCrypto(t, cryptoDir, "alice")

	testCases := []struct {
		name               string
		requestFactory     func() (*http.Request, error)
		dbMockFactory      func(response *types.TxReceiptResponseEnvelope) bcdb.DB
		expectedResponse   *types.TxReceiptResponseEnvelope
		expectedStatusCode int
		expectedErr        string
	}{
		{
			name: "valid get receipt request",
			expectedResponse: &types.TxReceiptResponseEnvelope{
				Response: &types.TxReceiptResponse{
					Header: &types.ResponseHeader{
						NodeId: "testNodeID",
					},
					Receipt: &types.TxReceipt{
						Header: &types.BlockHeader{
							BaseHeader: &types.BlockHeaderBase{
								Number: 2,
							},
						},
						TxIndex: 1,
					},
				},
				Signature: []byte{0, 0, 0},
			},
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetTransactionReceipt("tx1"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxReceiptQuery{
					UserId: submittingUserName,
					TxId:   "tx1",
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.TxReceiptResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetTxReceipt", submittingUserName, "tx1").Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusOK,
		},
		{
			name:             "user doesn't exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetTransactionReceipt("tx1"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxReceiptQuery{
					UserId: submittingUserName,
					TxId:   "tx1",
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.TxReceiptResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(nil, errors.New("user does not exist"))
				db.On("GetTxReceipt", submittingUserName, "tx1").Return(response, nil)
				return db
			},
			expectedStatusCode: http.StatusUnauthorized,
			expectedErr:        "signature verification failed",
		},
		{
			name:             "tx not exist",
			expectedResponse: nil,
			requestFactory: func() (*http.Request, error) {
				req, err := http.NewRequest(http.MethodGet, constants.URLForGetTransactionReceipt("tx1"), nil)
				if err != nil {
					return nil, err
				}
				req.Header.Set(constants.UserHeader, submittingUserName)
				sig := testutils.SignatureFromQuery(t, aliceSigner, &types.GetTxReceiptQuery{
					UserId: submittingUserName,
					TxId:   "tx1",
				})
				req.Header.Set(constants.SignatureHeader, base64.StdEncoding.EncodeToString(sig))
				return req, nil
			},
			dbMockFactory: func(response *types.TxReceiptResponseEnvelope) bcdb.DB {
				db := &mocks.DB{}
				db.On("GetCertificate", submittingUserName).Return(aliceCert, nil)
				db.On("GetTxReceipt", submittingUserName, "tx1").Return(response, &interrors.NotFoundErr{Message: "tx not found"})
				return db
			},
			expectedStatusCode: http.StatusNotFound,
			expectedErr:        "error while processing 'GET /ledger/tx/receipt/tx1' because tx not found",
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
			handler := NewLedgerRequestHandler(db, logger)
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
				res := &types.TxReceiptResponseEnvelope{}
				require.NoError(t, protojson.Unmarshal(requestBody, res))
				require.Equal(t, tt.expectedResponse, res)
			}
		})
	}
}
