package httphandler

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/server/internal/bcdb"
	backend "github.ibm.com/blockchaindb/server/internal/bcdb"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// ResponseErr holds the error response
type ResponseErr struct {
	ErrMsg string `json:"error,omitempty"`
}

func (e *ResponseErr) Error() string {
	return e.ErrMsg
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
	db backend.DB
}

// HandleTransaction handles transaction submission
func (t *txHandler) handleTransaction(w http.ResponseWriter, tx interface{}) {
	if err := t.db.SubmitTransaction(tx); err != nil {
		if _, ok := err.(*bcdb.DuplicateTxIDError); ok {
			SendHTTPResponse(w, http.StatusBadRequest, &ResponseErr{ErrMsg: err.Error()})
		} else {
			SendHTTPResponse(w, http.StatusInternalServerError, &ResponseErr{ErrMsg: err.Error()})
		}
	}

	SendHTTPResponse(w, http.StatusOK, empty.Empty{})
}

func extractVerifiedQueryPayload(w http.ResponseWriter, r *http.Request, queryType string, signVerifier *cryptoservice.SignatureVerifier) (interface{}, bool) {
	querierUserID, signature, err := validateAndParseHeader(&r.Header)
	if err != nil {
		SendHTTPResponse(w, http.StatusBadRequest, &ResponseErr{ErrMsg: err.Error()})
		return nil, true
	}

	var payload interface{}
	params := mux.Vars(r)

	switch queryType {
	case constants.GetData:
		payload = &types.GetDataQuery{
			UserID: querierUserID,
			DBName: params["dbname"],
			Key:    params["key"],
		}
	case constants.GetUser:
		payload = &types.GetUserQuery{
			UserID:       querierUserID,
			TargetUserID: params["userid"],
		}
	case constants.GetDBStatus:
		payload = &types.GetDBStatusQuery{
			UserID: querierUserID,
			DBName: params["dbname"],
		}
	case constants.GetConfig:
		payload = &types.GetConfigQuery{
			UserID: querierUserID,
		}
	case constants.GetNodeConfig:
		payload = &types.GetNodeConfigQuery{
			UserID: querierUserID,
			NodeID: params["nodeId"],
		}
	case constants.GetBlockHeader:
		blockNum, err := getUintParam("blockId", params)
		if err != nil {
			SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		payload = &types.GetBlockQuery{
			UserID:      querierUserID,
			BlockNumber: blockNum,
		}
	case constants.GetPath:
		startBlockNum, endBlockNum, err := getStartAndEndBlockNum(params)
		if err != nil {
			SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		payload = &types.GetLedgerPathQuery{
			UserID:           querierUserID,
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
			UserID:      querierUserID,
			BlockNumber: blockNum,
			TxIndex:     txIndex,
		}
	case constants.GetTxReceipt:
		payload = &types.GetTxReceiptQuery{
			UserID: querierUserID,
			TxID:   params["txId"],
		}
	case constants.GetHistoricalData:
		version, err := getVersion(params)
		if err != nil {
			SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		payload = &types.GetHistoricalDataQuery{
			UserID:    querierUserID,
			DBName:    params["dbname"],
			Key:       params["key"],
			Version:   version,
			Direction: params["direction"],
		}
	case constants.GetDataReaders:
		payload = &types.GetDataReadersQuery{
			UserID: querierUserID,
			DBName: params["dbname"],
			Key:    params["key"],
		}
	case constants.GetDataWriters:
		payload = &types.GetDataWritersQuery{
			UserID: querierUserID,
			DBName: params["dbname"],
			Key:    params["key"],
		}
	case constants.GetDataReadBy:
		payload = &types.GetDataReadByQuery{
			UserID:       querierUserID,
			TargetUserID: params["userId"],
		}
	case constants.GetDataWrittenBy:
		payload = &types.GetDataWrittenByQuery{
			UserID:       querierUserID,
			TargetUserID: params["userId"],
		}
	case constants.GetTxIDsSubmittedBy:
		payload = &types.GetTxIDsSubmittedByQuery{
			UserID:       querierUserID,
			TargetUserID: params["userId"],
		}
	}

	err, status := VerifyRequestSignature(signVerifier, querierUserID, signature, payload)
	if err != nil {
		SendHTTPResponse(w, status, err)
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
	requestBytes, err := json.Marshal(requestPayload)
	if err != nil {
		return &ResponseErr{ErrMsg: "failure during json.Marshal: " + err.Error()}, http.StatusInternalServerError
	}

	err = sigVerifier.Verify(user, signature, requestBytes)
	if err != nil {
		return &ResponseErr{ErrMsg: "signature verification failed"}, http.StatusUnauthorized
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
		return 0, 0, &ResponseErr{
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

func getUintParam(key string, params map[string]string) (uint64, *ResponseErr) {
	valStr, ok := params[key]
	if !ok {
		return 0, &ResponseErr{
			ErrMsg: "query error - bad or missing literal: " + key,
		}
	}
	val, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return 0, &ResponseErr{
			ErrMsg: "query error - bad or missing literal: " + key + " " + err.Error(),
		}
	}
	return val, nil
}
