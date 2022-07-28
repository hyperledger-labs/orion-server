// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package httphandler

import (
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/marshal"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"google.golang.org/protobuf/proto"
)

func extractVerifiedQueryPayload(w http.ResponseWriter, r *http.Request, queryType string, signVerifier *cryptoservice.SignatureVerifier) (interface{}, bool) {
	querierUserID, signature, err := validateAndParseHeader(&r.Header)
	if err != nil {
		utils.SendHTTPResponse(w, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return nil, true
	}

	var payload interface{}
	params := mux.Vars(r)

	switch queryType {
	case constants.GetData:
		payload = &types.GetDataQuery{
			UserId: querierUserID,
			DbName: params["dbname"],
			Key:    params["key"],
		}
	case constants.GetDataRange:
		limit, err := strconv.ParseUint(params["limit"], 10, 64)
		if err != nil {
			utils.SendHTTPResponse(w, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
			return nil, true
		}

		payload = &types.GetDataRangeQuery{
			UserId:   querierUserID,
			DbName:   params["dbname"],
			StartKey: params["startkey"][1 : len(params["startkey"])-1],
			EndKey:   params["endkey"][1 : len(params["endkey"])-1],
			Limit:    limit,
		}
	case constants.GetUser:
		payload = &types.GetUserQuery{
			UserId:       querierUserID,
			TargetUserId: params["userid"],
		}
	case constants.GetDBStatus:
		payload = &types.GetDBStatusQuery{
			UserId: querierUserID,
			DbName: params["dbname"],
		}
	case constants.GetDBIndex:
		payload = &types.GetDBIndexQuery{
			UserId: querierUserID,
			DbName: params["dbname"],
		}
	case constants.GetConfig:
		payload = &types.GetConfigQuery{
			UserId: querierUserID,
		}
	case constants.GetNodeConfig:
		payload = &types.GetNodeConfigQuery{
			UserId: querierUserID,
			NodeId: params["nodeId"],
		}
	case constants.GetLastConfigBlock:
		payload = &types.GetConfigBlockQuery{
			UserId:      querierUserID,
			BlockNumber: 0, // 0 means get last, as first block is 1
		}
	case constants.GetClusterStatus:
		noCertificates := false
		if value, ok := params["noCertificates"]; ok {
			noCertificates, err = strconv.ParseBool(value)
			if err != nil {
				utils.SendHTTPResponse(w, http.StatusBadRequest, err)
				return nil, true
			}
		}

		payload = &types.GetClusterStatusQuery{
			UserId:         querierUserID,
			NoCertificates: noCertificates,
		}
	case constants.GetBlockHeader:
		blockNum, err := utils.GetBlockNum(params)
		if err != nil {
			utils.SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		augmented := false
		if _, ok := params["isAugmented"]; ok {
			augmented, err = strconv.ParseBool(params["isAugmented"])
			if err != nil {
				utils.SendHTTPResponse(w, http.StatusBadRequest, err)
				return nil, true
			}
		}

		payload = &types.GetBlockQuery{
			UserId:      querierUserID,
			BlockNumber: blockNum,
			Augmented:   augmented,
		}
	case constants.GetLastBlockHeader:
		payload = &types.GetLastBlockQuery{
			UserId: querierUserID,
		}
	case constants.GetPath:
		startBlockNum, endBlockNum, err := utils.GetStartAndEndBlockNum(params)
		if err != nil {
			utils.SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		payload = &types.GetLedgerPathQuery{
			UserId:           querierUserID,
			StartBlockNumber: startBlockNum,
			EndBlockNumber:   endBlockNum,
		}
	case constants.GetTxProof:
		blockNum, txIndex, err := utils.GetBlockNumAndTxIndex(params)
		if err != nil {
			utils.SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		payload = &types.GetTxProofQuery{
			UserId:      querierUserID,
			BlockNumber: blockNum,
			TxIndex:     txIndex,
		}
	case constants.GetDataProof:
		blockNum, err := utils.GetBlockNum(params)
		if err != nil {
			utils.SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		deleted := false
		if _, ok := params["deleted"]; ok {
			deleted, err = strconv.ParseBool(params["deleted"])
			if err != nil {
				utils.SendHTTPResponse(w, http.StatusBadRequest, err)
				return nil, true
			}
		}

		payload = &types.GetDataProofQuery{
			UserId:      querierUserID,
			BlockNumber: blockNum,
			DbName:      params["dbname"],
			Key:         params["key"],
			IsDeleted:   deleted,
		}
	case constants.GetTxReceipt:
		payload = &types.GetTxReceiptQuery{
			UserId: querierUserID,
			TxId:   params["txId"],
		}
	case constants.GetHistoricalData:
		version, err := utils.GetVersion(params)
		if err != nil {
			utils.SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		v, isOnlyDeletesSet := params["onlydeletes"]
		if isOnlyDeletesSet && v != "true" {
			utils.SendHTTPResponse(w, http.StatusBadRequest, &types.HttpResponseErr{
				ErrMsg: "the onlydeletes parameters must be set only to 'true'",
			})
			return nil, true
		}

		_, isMostRecentSet := params["mostrecent"]

		payload = &types.GetHistoricalDataQuery{
			UserId:      querierUserID,
			DbName:      params["dbname"],
			Key:         params["key"],
			Version:     version,
			Direction:   params["direction"],
			OnlyDeletes: isOnlyDeletesSet,
			MostRecent:  isMostRecentSet,
		}
	case constants.GetDataReaders:
		payload = &types.GetDataReadersQuery{
			UserId: querierUserID,
			DbName: params["dbname"],
			Key:    params["key"],
		}
	case constants.GetDataWriters:
		payload = &types.GetDataWritersQuery{
			UserId: querierUserID,
			DbName: params["dbname"],
			Key:    params["key"],
		}
	case constants.GetDataReadBy:
		payload = &types.GetDataReadByQuery{
			UserId:       querierUserID,
			TargetUserId: params["userId"],
		}
	case constants.GetDataWrittenBy:
		payload = &types.GetDataWrittenByQuery{
			UserId:       querierUserID,
			TargetUserId: params["userId"],
		}
	case constants.GetDataDeletedBy:
		payload = &types.GetDataDeletedByQuery{
			UserId:       querierUserID,
			TargetUserId: params["userId"],
		}
	case constants.GetTxIDsSubmittedBy:
		payload = &types.GetTxIDsSubmittedByQuery{
			UserId:       querierUserID,
			TargetUserId: params["userId"],
		}
	case constants.GetMostRecentUserOrNode:
		version, err := utils.GetVersion(params)
		if err != nil {
			utils.SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		var queryType types.GetMostRecentUserOrNodeQuery_Type
		if params["type"] == "node" {
			queryType = types.GetMostRecentUserOrNodeQuery_NODE
		} else {
			queryType = types.GetMostRecentUserOrNodeQuery_USER
		}

		payload = &types.GetMostRecentUserOrNodeQuery{
			Type:    queryType,
			UserId:  querierUserID,
			Id:      params["id"],
			Version: version,
		}
	case constants.PostDataQuery:
		if r.Body == nil {
			utils.SendHTTPResponse(w, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: "query is empty"})
			return nil, true
		}

		b, err := io.ReadAll(r.Body)
		if err != nil {
			utils.SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		q, err := strconv.Unquote(string(b))
		if err != nil {
			utils.SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}
		payload = &types.DataJSONQuery{
			UserId: querierUserID,
			DbName: params["dbname"],
			Query:  q,
		}
	}

	err, status := VerifyRequestSignature(signVerifier, querierUserID, signature, payload)
	if err != nil {
		utils.SendHTTPResponse(w, status, err)
		return nil, true
	}

	return payload, false
}

func VerifyRequestSignature(
	sigVerifier *cryptoservice.SignatureVerifier,
	user string,
	signature []byte,
	requestPayload interface{},
) (error, int) {
	if _, ok := requestPayload.(proto.Message); !ok {
		return &types.HttpResponseErr{ErrMsg: "payload is not a protoreflect message"}, http.StatusInternalServerError
	}

	requestBytes, err := marshal.DefaultMarshaler().Marshal(requestPayload.(proto.Message))
	if err != nil {
		return &types.HttpResponseErr{ErrMsg: "failure during Marshal: " + err.Error()}, http.StatusInternalServerError
	}

	err = sigVerifier.Verify(user, signature, requestBytes)
	if err != nil {
		return &types.HttpResponseErr{ErrMsg: "signature verification failed"}, http.StatusUnauthorized
	}

	return nil, http.StatusOK
}

func validateAndParseHeader(h *http.Header) (string, []byte, error) {
	userID := h.Get(constants.UserHeader)
	if userID == "" {
		return "", nil, errors.New(constants.UserHeader + " is not set in the http request header")
	}

	signature := h.Get(constants.SignatureHeader)
	if signature == "" {
		return "", nil, errors.New(constants.SignatureHeader + " is not set in the http request header")
	}
	signatureBytes, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return "", nil, errors.New(constants.SignatureHeader + " is not encoded correctly")
	}

	return userID, signatureBytes, nil
}

func validateAndParseTxPostHeader(h *http.Header) (time.Duration, error) {
	timeoutStr := h.Get(constants.TimeoutHeader)
	if len(timeoutStr) == 0 {
		return 0, nil
	}

	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return 0, err
	}

	if timeout < 0 {
		return 0, errors.New("timeout can't be negative " + strconv.Quote(timeoutStr))
	}
	return timeout, nil
}
