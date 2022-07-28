// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/hyperledger-labs/orion-server/internal/bcdb"
	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"google.golang.org/protobuf/encoding/protojson"
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
	handler.router.HandleFunc(constants.GetLastConfigBlock, handler.configBlockQuery).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetNodeConfig, handler.nodeQuery).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.PostConfigTx, handler.configTransaction).Methods(http.MethodPost)
	// HTTP GET "/config/cluster?nocert=true" returns nodes without certificates
	handler.router.HandleFunc(constants.GetClusterStatus, handler.clusterStatusQuery).Methods(http.MethodGet).Queries("nocert", "{noCertificates:true|false}")
	// HTTP GET "/config/cluster" returns nodes with certificates
	handler.router.HandleFunc(constants.GetClusterStatus, handler.clusterStatusQuery).Methods(http.MethodGet)

	return handler
}

func (c *configRequestHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	c.router.ServeHTTP(response, request)
}

func (c *configRequestHandler) configQuery(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetConfig, c.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetConfigQuery)

	config, err := c.db.GetConfig(query.GetUserId())
	if err != nil {
		utils.SendHTTPResponse(
			response,
			http.StatusInternalServerError,
			&types.HttpResponseErr{ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error()},
		)
		return
	}
	utils.SendHTTPResponse(response, http.StatusOK, config)
}

func (c *configRequestHandler) configBlockQuery(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetLastConfigBlock, c.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetConfigBlockQuery)

	configBlockResponseEnvelope, err := c.db.GetConfigBlock(query.GetUserId(), query.GetBlockNumber())
	if err != nil {
		var status int

		switch err.(type) {
		case *ierrors.PermissionErr:
			status = http.StatusForbidden
		case *ierrors.NotFoundErr:
			status = http.StatusNotFound
		case *ierrors.BadRequestError:
			status = http.StatusBadRequest
		default:
			status = http.StatusInternalServerError
		}

		utils.SendHTTPResponse(
			response,
			status,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	utils.SendHTTPResponse(response, http.StatusOK, configBlockResponseEnvelope)
}

func (c *configRequestHandler) clusterStatusQuery(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetClusterStatus, c.sigVerifier)
	if respondedErr {
		return
	}

	query := payload.(*types.GetClusterStatusQuery)
	clusterStatus, err := c.db.GetClusterStatus(query.NoCertificates)

	if err != nil {
		utils.SendHTTPResponse(
			response,
			http.StatusInternalServerError,
			&types.HttpResponseErr{ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error()},
		)
		return
	}

	utils.SendHTTPResponse(response, http.StatusOK, clusterStatus)
}

func (c *configRequestHandler) nodeQuery(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetNodeConfig, c.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetNodeConfigQuery)

	config, err := c.db.GetNodeConfig(query.NodeId)

	if err != nil {
		utils.SendHTTPResponse(
			response,
			http.StatusInternalServerError,
			&types.HttpResponseErr{ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error()},
		)
		return
	}

	utils.SendHTTPResponse(response, http.StatusOK, config)
}

func (c *configRequestHandler) configTransaction(response http.ResponseWriter, request *http.Request) {
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

	txEnv := &types.ConfigTxEnvelope{}
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

	if err, code := VerifyRequestSignature(c.sigVerifier, txEnv.Payload.UserId, txEnv.Signature, txEnv.Payload); err != nil {
		utils.SendHTTPResponse(response, code, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	c.txHandler.handleTransaction(response, request, txEnv, timeout)
}
