// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	backend "github.com/IBM-Blockchain/bcdb-server/internal/bcdb"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/cryptoservice"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

// dbRequestHandler handles query and transaction associated
// the database administration
type dbRequestHandler struct {
	db          backend.DB
	sigVerifier *cryptoservice.SignatureVerifier
	router      *mux.Router
	txHandler   *txHandler
	logger      *logger.SugarLogger
}

// NewDBRequestHandler returns DB requests handler
func NewDBRequestHandler(db backend.DB, logger *logger.SugarLogger) http.Handler {
	handler := &dbRequestHandler{
		db:          db,
		sigVerifier: cryptoservice.NewVerifier(db, logger),
		router:      mux.NewRouter(),
		txHandler: &txHandler{
			db: db,
		},
		logger: logger,
	}

	handler.router.HandleFunc(constants.GetDBStatus, handler.dbStatus).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.PostDBTx, handler.dbTransaction).Methods(http.MethodPost)

	return handler
}

func (d *dbRequestHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	d.router.ServeHTTP(response, request)
}

func (d *dbRequestHandler) dbStatus(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetDBStatus, d.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetDBStatusQuery)

	dbStatus, err := d.db.GetDBStatus(query.DBName)
	if err != nil {
		SendHTTPResponse(
			response,
			http.StatusInternalServerError,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			},
		)
		return
	}

	SendHTTPResponse(response, http.StatusOK, dbStatus)
}

func (d *dbRequestHandler) dbTransaction(response http.ResponseWriter, request *http.Request) {
	timeout, err := validateAndParseTxPostHeader(&request.Header)
	if err != nil {
		SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	dbRequestBody := json.NewDecoder(request.Body)
	dbRequestBody.DisallowUnknownFields()

	txEnv := &types.DBAdministrationTxEnvelope{}
	if err := dbRequestBody.Decode(txEnv); err != nil {
		SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	if txEnv.Payload == nil {
		SendHTTPResponse(response, http.StatusBadRequest,
			&types.HttpResponseErr{ErrMsg: fmt.Sprintf("missing transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if txEnv.Payload.UserID == "" {
		SendHTTPResponse(response, http.StatusBadRequest,
			&types.HttpResponseErr{ErrMsg: fmt.Sprintf("missing UserID in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if len(txEnv.Signature) == 0 {
		SendHTTPResponse(response, http.StatusBadRequest,
			&types.HttpResponseErr{ErrMsg: fmt.Sprintf("missing Signature in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if err, code := VerifyRequestSignature(d.sigVerifier, txEnv.Payload.UserID, txEnv.Signature, txEnv.Payload); err != nil {
		SendHTTPResponse(response, code, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	d.txHandler.handleTransaction(response, txEnv, timeout)
}
