// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	backend "github.com/hyperledger-labs/orion-server/internal/bcdb"
	"github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"google.golang.org/protobuf/encoding/protojson"
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
	handler.router.HandleFunc(constants.GetDBIndex, handler.dbIndex).Methods(http.MethodGet)
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

	dbStatus, err := d.db.GetDBStatus(query.DbName)
	if err != nil {
		utils.SendHTTPResponse(
			response,
			http.StatusInternalServerError,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			},
		)
		return
	}

	utils.SendHTTPResponse(response, http.StatusOK, dbStatus)
}

func (d *dbRequestHandler) dbIndex(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetDBIndex, d.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetDBIndexQuery)

	dbIndex, err := d.db.GetDBIndex(query.DbName, query.UserId)
	if err != nil {
		var status int

		switch err.(type) {
		case *errors.PermissionErr:
			status = http.StatusForbidden
		default:
			status = http.StatusInternalServerError
		}

		utils.SendHTTPResponse(
			response,
			status,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			},
		)
		return
	}

	utils.SendHTTPResponse(response, http.StatusOK, dbIndex)
}

func (d *dbRequestHandler) dbTransaction(response http.ResponseWriter, request *http.Request) {
	timeout, err := validateAndParseTxPostHeader(&request.Header)
	if err != nil {
		utils.SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	requestBytes, err := ioutil.ReadAll(request.Body)
	if err != nil {
		utils.SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	txEnv := &types.DBAdministrationTxEnvelope{}
	if err := protojson.Unmarshal(requestBytes, txEnv); err != nil {
		utils.SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	if txEnv.Payload == nil {
		utils.SendHTTPResponse(response, http.StatusBadRequest,
			&types.HttpResponseErr{ErrMsg: fmt.Sprintf("missing transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if txEnv.Payload.UserId == "" {
		utils.SendHTTPResponse(response, http.StatusBadRequest,
			&types.HttpResponseErr{ErrMsg: fmt.Sprintf("missing UserID in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if len(txEnv.Signature) == 0 {
		utils.SendHTTPResponse(response, http.StatusBadRequest,
			&types.HttpResponseErr{ErrMsg: fmt.Sprintf("missing Signature in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if err, code := VerifyRequestSignature(d.sigVerifier, txEnv.Payload.UserId, txEnv.Signature, txEnv.Payload); err != nil {
		utils.SendHTTPResponse(response, code, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	d.txHandler.handleTransaction(response, request, txEnv, timeout)
}
