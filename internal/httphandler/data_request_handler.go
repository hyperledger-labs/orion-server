// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/gorilla/mux"
	"github.com/IBM-Blockchain/bcdb-server/internal/bcdb"
	"github.com/IBM-Blockchain/bcdb-server/internal/errors"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/cryptoservice"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

// dataRequestHandler handles query and transaction associated
// the user's data/state
type dataRequestHandler struct {
	db          bcdb.DB
	sigVerifier *cryptoservice.SignatureVerifier
	router      *mux.Router
	txHandler   *txHandler
	logger      *logger.SugarLogger
}

// NewDataRequestHandler returns handler capable to serve incoming data requests
func NewDataRequestHandler(db bcdb.DB, logger *logger.SugarLogger) http.Handler {
	handler := &dataRequestHandler{
		db:          db,
		sigVerifier: cryptoservice.NewVerifier(db, logger),
		router:      mux.NewRouter(),
		txHandler: &txHandler{
			db: db,
		},
		logger: logger,
	}

	handler.router.HandleFunc(constants.GetData, handler.dataQuery).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.PostDataTx, handler.dataTransaction).Methods(http.MethodPost)

	return handler
}

func (d *dataRequestHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	d.router.ServeHTTP(response, request)
}

func (d *dataRequestHandler) dataQuery(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetData, d.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetDataQuery)

	if !d.db.IsDBExists(query.DBName) {
		SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{
			ErrMsg: "error db '" + query.DBName + "' doesn't exist",
		})
		return
	}

	data, err := d.db.GetData(query.DBName, query.UserID, query.Key)
	if err != nil {
		var status int

		switch err.(type) {
		case *errors.PermissionErr:
			status = http.StatusForbidden
		default:
			status = http.StatusInternalServerError
		}

		SendHTTPResponse(
			response,
			status,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	SendHTTPResponse(response, http.StatusOK, data)
}

func (d *dataRequestHandler) dataTransaction(response http.ResponseWriter, request *http.Request) {
	timeout, err := validateAndParseTxPostHeader(&request.Header)
	if err != nil {
		SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	requestData := json.NewDecoder(request.Body)
	requestData.DisallowUnknownFields()

	txEnv := &types.DataTxEnvelope{}
	if err := requestData.Decode(txEnv); err != nil {
		SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	if txEnv.Payload == nil {
		SendHTTPResponse(response, http.StatusBadRequest,
			&types.HttpResponseErr{ErrMsg: fmt.Sprintf("missing transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if len(txEnv.Payload.MustSignUserIDs) == 0 {
		SendHTTPResponse(response, http.StatusBadRequest,
			&types.HttpResponseErr{ErrMsg: fmt.Sprintf("missing UserID in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	var notSigned []string
	for _, user := range txEnv.Payload.MustSignUserIDs {
		if user == "" {
			SendHTTPResponse(response, http.StatusBadRequest,
				&types.HttpResponseErr{ErrMsg: "an empty UserID in MustSignUserIDs list present in the transaction envelope"})
			return
		}

		if _, ok := txEnv.Signatures[user]; !ok {
			notSigned = append(notSigned, user)
		}
	}
	if len(notSigned) > 0 {
		sort.Strings(notSigned)
		SendHTTPResponse(response, http.StatusBadRequest,
			&types.HttpResponseErr{ErrMsg: "users [" + strings.Join(notSigned, ",") + "] in the must sign list have not signed the transaction"})
		return
	}

	for _, userID := range txEnv.Payload.MustSignUserIDs {
		if err, code := VerifyRequestSignature(d.sigVerifier, userID, txEnv.Signatures[userID], txEnv.Payload); err != nil {
			SendHTTPResponse(response, code, &types.HttpResponseErr{ErrMsg: err.Error()})
			return
		}
	}

	d.txHandler.handleTransaction(response, txEnv, timeout)
}
