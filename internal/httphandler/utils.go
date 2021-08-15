// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	internalerror "github.com/IBM-Blockchain/bcdb-server/internal/errors"

	"github.com/IBM-Blockchain/bcdb-server/internal/bcdb"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/cryptoservice"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/gorilla/mux"
)

func MarshalOrPanic(response interface{}) []byte {
	bytes, err := json.Marshal(response)
	if err != nil {
		panic(err)
	}

	return bytes
}

// SendHTTPResponse writes HTTP response back including HTTP code number and encode payload
func SendHTTPResponse(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if _, err := w.Write(response); err != nil {
		log.Printf("Warning: failed to write response [%v] to the response writer\n", w)
	}
}

type txHandler struct {
	db bcdb.DB
}

// HandleTransaction handles transaction submission
func (t *txHandler) handleTransaction(w http.ResponseWriter, tx interface{}, timeout time.Duration) {
	// If timeout == 0, tx is async, otherwise it is synchronous.
	resp, err := t.db.SubmitTransaction(tx, timeout)
	if err != nil {
		switch err.(type) {
		case *internalerror.DuplicateTxIDError:
			SendHTTPResponse(w, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		case *internalerror.TimeoutErr:
			SendHTTPResponse(w, http.StatusAccepted, &types.HttpResponseErr{ErrMsg: "Transaction processing timeout"})
		default:
			SendHTTPResponse(w, http.StatusInternalServerError, &types.HttpResponseErr{ErrMsg: err.Error()})
		}
		return
	}
	SendHTTPResponse(w, http.StatusOK, resp)
}

func extractVerifiedQueryPayload(w http.ResponseWriter, r *http.Request, queryType string, signVerifier *cryptoservice.SignatureVerifier) (interface{}, bool) {
	querierUserID, _, err := validateAndParseHeader(&r.Header)
	if err != nil {
		SendHTTPResponse(w, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
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
	case constants.GetConfig:
		payload = &types.GetConfigQuery{
			UserId: querierUserID,
		}
	case constants.GetNodeConfig:
		payload = &types.GetNodeConfigQuery{
			UserId: querierUserID,
			NodeId: params["nodeId"],
		}
	case constants.GetBlockHeader:
		blockNum, err := getUintParam("blockId", params)
		if err != nil {
			SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		payload = &types.GetBlockQuery{
			UserId:      querierUserID,
			BlockNumber: blockNum,
		}
	case constants.GetPath:
		startBlockNum, endBlockNum, err := getStartAndEndBlockNum(params)
		if err != nil {
			SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		payload = &types.GetLedgerPathQuery{
			UserId:           querierUserID,
			StartBlockNumber: startBlockNum,
			EndBlockNumber:   endBlockNum,
		}
	case constants.GetTxProof:
		blockNum, txIndex, err := getBlockNumAndTxIndex(params)
		if err != nil {
			SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		payload = &types.GetTxProofQuery{
			UserId:      querierUserID,
			BlockNumber: blockNum,
			TxIndex:     txIndex,
		}
	case constants.GetDataProof:
		blockNum, err := getBlockNum(params)
		if err != nil {
			SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		deleted := false
		if _, ok := params["deleted"]; ok {
			deleted, err = strconv.ParseBool(params["deleted"])
			if err != nil {
				SendHTTPResponse(w, http.StatusBadRequest, err)
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
		version, err := getVersion(params)
		if err != nil {
			SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		v, isOnlyDeletesSet := params["onlydeletes"]
		if isOnlyDeletesSet && v != "true" {
			SendHTTPResponse(w, http.StatusBadRequest, &types.HttpResponseErr{
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
		version, err := getVersion(params)
		if err != nil {
			SendHTTPResponse(w, http.StatusBadRequest, err)
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
			fmt.Println("body is empty")
			SendHTTPResponse(w, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: "query is empty"})
			return nil, true
		}

		b, err := io.ReadAll(r.Body)
		if err != nil {
			SendHTTPResponse(w, http.StatusInternalServerError, err)
			return nil, true
		}

		fmt.Println(string(b))
		// q, err := strconv.Unquote(string(b))
		// if err != nil {
		// 	SendHTTPResponse(w, http.StatusInternalServerError, err)
		// 	return nil, true
		// }
		payload = &types.DataJSONQuery{
			UserId: querierUserID,
			DbName: params["dbname"],
			Query:  string(b),
		}
	}

	// err, status := VerifyRequestSignature(signVerifier, querierUserID, signature, payload)
	// if err != nil {
	// 	SendHTTPResponse(w, status, err)
	// 	return nil, true
	// }

	return payload, false
}

func VerifyRequestSignature(
	sigVerifier *cryptoservice.SignatureVerifier,
	user string,
	signature []byte,
	requestPayload interface{},
) (error, int) {
	requestBytes, err := json.Marshal(requestPayload)
	if err != nil {
		return &types.HttpResponseErr{ErrMsg: "failure during json.Marshal: " + err.Error()}, http.StatusInternalServerError
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

func getBlockNumAndTxIndex(params map[string]string) (uint64, uint64, error) {
	blockNum, err := getUintParam("blockId", params)
	if err != nil {
		return 0, 0, err
	}

	txIndex, err := getUintParam("idx", params)
	if err != nil {
		return 0, 0, err
	}

	return blockNum, txIndex, nil
}

func getBlockNum(params map[string]string) (uint64, error) {
	blockNum, err := getUintParam("blockId", params)
	if err != nil {
		return 0, err
	}

	return blockNum, nil
}

func getStartAndEndBlockNum(params map[string]string) (uint64, uint64, error) {
	startBlockNum, err := getUintParam("startId", params)
	if err != nil {
		return 0, 0, err
	}

	endBlockNum, err := getUintParam("endId", params)
	if err != nil {
		return 0, 0, err
	}

	if endBlockNum < startBlockNum {
		return 0, 0, &types.HttpResponseErr{
			ErrMsg: fmt.Sprintf("query error: startId=%d > endId=%d", startBlockNum, endBlockNum),
		}
	}

	return startBlockNum, endBlockNum, nil
}

func getVersion(params map[string]string) (*types.Version, error) {
	if _, ok := params["blknum"]; !ok {
		return nil, nil
	}

	blockNum, err := getUintParam("blknum", params)
	if err != nil {
		return nil, err
	}

	txNum, err := getUintParam("txnum", params)
	if err != nil {
		return nil, err
	}

	return &types.Version{
		BlockNum: blockNum,
		TxNum:    txNum,
	}, nil
}

func getUintParam(key string, params map[string]string) (uint64, *types.HttpResponseErr) {
	valStr, ok := params[key]
	if !ok {
		return 0, &types.HttpResponseErr{
			ErrMsg: "query error - bad or missing literal: " + key,
		}
	}
	val, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return 0, &types.HttpResponseErr{
			ErrMsg: "query error - bad or missing literal: " + key + " " + err.Error(),
		}
	}
	return val, nil
}
