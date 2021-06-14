// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/IBM-Blockchain/bcdb-server/internal/bcdb"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/cryptoservice"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/gorilla/mux"
)

// configRequestHandler handles query and transaction associated
// with the cluster configuration
type configRequestHandler struct {
	db          bcdb.DB
	sigVerifier *cryptoservice.SignatureVerifier
	router      *mux.Router
	txHandler   *txHandler
	logger      *logger.SugarLogger
}

// NewConfigRequestHandler return config query and transactions request handler
func NewConfigRequestHandler(db bcdb.DB, logger *logger.SugarLogger) http.Handler {
	handler := &configRequestHandler{
		db:          db,
		sigVerifier: cryptoservice.NewVerifier(db, logger),
		router:      mux.NewRouter(),
		txHandler: &txHandler{
			db: db,
		},
		logger: logger,
	}

	handler.router.HandleFunc(constants.GetConfig, handler.configQuery).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetNodeConfig, handler.nodeQuery).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.PostConfigTx, handler.configTransaction).Methods(http.MethodPost)

	return handler
}

func (c *configRequestHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	c.router.ServeHTTP(response, request)
}

func (c *configRequestHandler) configQuery(response http.ResponseWriter, request *http.Request) {
	_, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetConfig, c.sigVerifier)
	if respondedErr {
		return
	}

	config, err := c.db.GetConfig()
	if err != nil {
		SendHTTPResponse(
			response,
			http.StatusInternalServerError,
			&types.HttpResponseErr{ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error()},
		)
		return
	}
	SendHTTPResponse(response, http.StatusOK, config)
}

func (c *configRequestHandler) nodeQuery(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetNodeConfig, c.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetNodeConfigQuery)

	config, err := c.db.GetNodeConfig(query.NodeID)

	if err != nil {
		SendHTTPResponse(
			response,
			http.StatusInternalServerError,
			&types.HttpResponseErr{ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error()},
		)
		return
	}

	SendHTTPResponse(response, http.StatusOK, config)
}

func (c *configRequestHandler) configTransaction(response http.ResponseWriter, request *http.Request) {
	timeout, err := validateAndParseTxPostHeader(&request.Header)
	if err != nil {
		SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	d := json.NewDecoder(request.Body)
	d.DisallowUnknownFields()

	txEnv := &types.ConfigTxEnvelope{}
	if err := d.Decode(txEnv); err != nil {
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

	if err, code := VerifyRequestSignature(c.sigVerifier, txEnv.Payload.UserID, txEnv.Signature, txEnv.Payload); err != nil {
		SendHTTPResponse(response, code, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	c.txHandler.handleTransaction(response, txEnv, timeout)
}
