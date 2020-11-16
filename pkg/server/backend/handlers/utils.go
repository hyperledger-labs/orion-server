package handlers

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/common/constants"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
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
func (t *txHandler) HandleTransaction(w http.ResponseWriter, tx interface{}) {
	if err := t.db.SubmitTransaction(tx); err != nil {
		SendHTTPResponse(w, http.StatusInternalServerError, &ResponseErr{ErrMsg: err.Error()})
		return
	}

	SendHTTPResponse(w, http.StatusOK, empty.Empty{})
}

func extractDataQueryEnvelope(request *http.Request, responseWriter http.ResponseWriter) (env *types.GetDataQueryEnvelope, respondedErr bool) {
	querierUserID, signature, err := validateAndParseHeader(&request.Header)
	if err != nil {
		SendHTTPResponse(responseWriter, http.StatusBadRequest, &ResponseErr{ErrMsg: err.Error()})
		return nil, true
	}

	params := mux.Vars(request)
	dbName, ok := params["dbname"]
	if !ok {
		SendHTTPResponse(responseWriter,
			http.StatusBadRequest,
			&ResponseErr{
				ErrMsg: "query error - bad or missing database name",
			})
		return nil, true
	}
	key, ok := params["key"]
	if !ok {
		SendHTTPResponse(responseWriter,
			http.StatusBadRequest,
			&ResponseErr{
				ErrMsg: "query error - bad or missing key",
			})
		return nil, true
	}

	env = &types.GetDataQueryEnvelope{
		Payload: &types.GetDataQuery{
			UserID: querierUserID,
			DBName: dbName,
			Key:    key,
		},
		Signature: signature,
	}

	return env, respondedErr
}

func extractUserQueryEnvelope(request *http.Request, responseWriter http.ResponseWriter) (env *types.GetUserQueryEnvelope, respondedErr bool) {
	querierUserID, signature, err := validateAndParseHeader(&request.Header)
	if err != nil {
		SendHTTPResponse(responseWriter, http.StatusBadRequest, &ResponseErr{ErrMsg: err.Error()})
		return nil, true
	}

	params := mux.Vars(request)
	targetUserID, ok := params["userid"]
	if !ok {
		SendHTTPResponse(responseWriter, http.StatusBadRequest, &ResponseErr{"query error - bad or missing userid"})
		return
	}

	env = &types.GetUserQueryEnvelope{
		Payload: &types.GetUserQuery{
			UserID:       querierUserID,
			TargetUserID: targetUserID,
		},
		Signature: signature,
	}

	return env, respondedErr
}

func extractDBStatusQueryEnvelope(request *http.Request, responseWriter http.ResponseWriter) (env *types.GetDBStatusQueryEnvelope, respondedErr bool) {
	querierUserID, signature, err := validateAndParseHeader(&request.Header)
	if err != nil {
		SendHTTPResponse(responseWriter, http.StatusBadRequest, &ResponseErr{ErrMsg: err.Error()})
		return nil, true
	}

	params := mux.Vars(request)
	dbName, ok := params["dbname"]
	if !ok {
		SendHTTPResponse(responseWriter,
			http.StatusBadRequest,
			&ResponseErr{
				ErrMsg: "query error - bad or missing database name",
			})
		return nil, true
	}

	env = &types.GetDBStatusQueryEnvelope{
		Payload: &types.GetDBStatusQuery{
			UserID: querierUserID,
			DBName: dbName,
		},
		Signature: signature,
	}

	return env, respondedErr
}

func extractConfigQueryEnvelope(request *http.Request, responseWriter http.ResponseWriter) (env *types.GetConfigQueryEnvelope, respondedErr bool) {
	querierUserID, signature, err := validateAndParseHeader(&request.Header)
	if err != nil {
		SendHTTPResponse(responseWriter, http.StatusBadRequest, &ResponseErr{ErrMsg: err.Error()})
		return nil, true
	}

	env = &types.GetConfigQueryEnvelope{
		Payload: &types.GetConfigQuery{
			UserID: querierUserID,
		},
		Signature: signature,
	}

	return env, respondedErr
}

func extractLedgerPathQueryEnvelope(request *http.Request, responseWriter http.ResponseWriter) (env *types.GetLedgerPathQueryEnvelope, respondedErr bool) {
	querierUserID, signature, err := validateAndParseHeader(&request.Header)
	if err != nil {
		SendHTTPResponse(responseWriter, http.StatusBadRequest, &ResponseErr{ErrMsg: err.Error()})
		return nil, true
	}

	params := mux.Vars(request)
	startNum, respErr := getUintParam("startId", params)
	if respErr != nil {
		SendHTTPResponse(responseWriter, http.StatusBadRequest, respErr)
		return nil, true
	}

	endNum, respErr := getUintParam("endId", params)
	if respErr != nil {
		SendHTTPResponse(responseWriter, http.StatusBadRequest, respErr)
		return nil, true
	}

	env = &types.GetLedgerPathQueryEnvelope{
		Payload: &types.GetLedgerPathQuery{
			UserID:           querierUserID,
			StartBlockNumber: startNum,
			EndBlockNumber:   endNum,
		},
		Signature: signature,
	}

	return env, respondedErr
}

func extractBlockQueryEnvelope(request *http.Request, responseWriter http.ResponseWriter) (env *types.GetBlockQueryEnvelope, respondedErr bool) {
	querierUserID, signature, err := validateAndParseHeader(&request.Header)
	if err != nil {
		SendHTTPResponse(responseWriter, http.StatusBadRequest, &ResponseErr{ErrMsg: err.Error()})
		return nil, true
	}

	params := mux.Vars(request)
	blockNum, respErr := getUintParam("blockId", params)
	if respErr != nil {
		SendHTTPResponse(responseWriter, http.StatusBadRequest, respErr)
		return nil, true
	}

	env = &types.GetBlockQueryEnvelope{
		Payload: &types.GetBlockQuery{
			UserID:      querierUserID,
			BlockNumber: blockNum,
		},
		Signature: signature,
	}

	return env, respondedErr
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
