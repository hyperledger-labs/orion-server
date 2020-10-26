package handlers

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/golang/protobuf/ptypes/empty"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
)

// ResponseErr holds the error response
type ResponseErr struct {
	Error string `json:"error,omitempty"`
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
func (t *txHandler) HandleTransaction(w http.ResponseWriter, userID string, tx interface{}) {
	exist, err := t.db.DoesUserExist(userID)
	if err != nil {
		SendHTTPResponse(w, http.StatusBadRequest, &ResponseErr{Error: err.Error()})
		return
	}
	if !exist {
		SendHTTPResponse(
			w,
			http.StatusForbidden,
			&ResponseErr{
				Error: "transaction is rejected as the submitting user [" + userID + "] does not exist in the cluster",
			},
		)
		return
	}

	if err := t.db.SubmitTransaction(tx); err != nil {
		SendHTTPResponse(w, http.StatusInternalServerError, &ResponseErr{Error: err.Error()})
		return
	}

	SendHTTPResponse(w, http.StatusOK, empty.Empty{})
}

// ExtractCreds extracts user credentials (userID and signature) from the HTTP request body and
// checks whenever such user exists in the system
func ExtractCreds(db backend.DB, w http.ResponseWriter, r *http.Request) (string, []byte, bool) {
	composedErr := true

	querierUserID, signature, err := validateAndParseHeader(&r.Header)
	if err != nil {
		SendHTTPResponse(w, http.StatusBadRequest, &ResponseErr{Error: err.Error()})
		return "", nil, composedErr
	}

	exist, err := db.DoesUserExist(querierUserID)
	if err != nil {
		SendHTTPResponse(w, http.StatusInternalServerError, &ResponseErr{Error: err.Error()})
		return "", nil, composedErr
	}
	if !exist {
		SendHTTPResponse(
			w,
			http.StatusForbidden,
			&ResponseErr{
				Error: r.URL.String() + " query is rejected as the submitting user [" + querierUserID + "] does not exist in the cluster",
			},
		)
		return "", nil, composedErr
	}

	return querierUserID, signature, !composedErr
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
