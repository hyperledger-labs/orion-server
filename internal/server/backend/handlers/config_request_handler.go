package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/server/internal/server/backend"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// configRequestHandler handles query and transaction associated
// with the cluster configuration
type configRequestHandler struct {
	db          backend.DB
	sigVerifier *cryptoservice.SignatureVerifier
	router      *mux.Router
	txHandler   *txHandler
	logger      *logger.SugarLogger
}

// NewConfigRequestHandler return config query and transactions request handler
func NewConfigRequestHandler(db backend.DB, logger *logger.SugarLogger) http.Handler {
	handler := &configRequestHandler{
		db:          db,
		sigVerifier: cryptoservice.NewVerifier(db),
		router:      mux.NewRouter(),
		txHandler: &txHandler{
			db: db,
		},
		logger: logger,
	}

	handler.router.HandleFunc(constants.GetConfig, handler.configQuery).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.PostConfigTx, handler.configTransaction).Methods(http.MethodPost)
	handler.router.HandleFunc(constants.GetNodeConfig, handler.nodeQuery).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetNodesConfig, handler.nodeQuery).Methods(http.MethodGet)

	return handler
}

func (c *configRequestHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	c.router.ServeHTTP(response, request)
}

func (c *configRequestHandler) configQuery(response http.ResponseWriter, request *http.Request) {
	queryEnv, respondedErr := extractConfigQueryEnvelope(request, response)
	if respondedErr {
		return
	}

	err, code := VerifyRequestSignature(c.sigVerifier, queryEnv.Payload.UserID, queryEnv.Signature, queryEnv.Payload)
	if err != nil {
		SendHTTPResponse(response, code, err)
		return
	}

	config, err := c.db.GetConfig()
	if err != nil {
		SendHTTPResponse(
			response,
			http.StatusInternalServerError,
			&ResponseErr{"error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error()},
		)
		return
	}
	SendHTTPResponse(response, http.StatusOK, config)
}

func (c *configRequestHandler) nodeQuery(response http.ResponseWriter, request *http.Request) {
	queryEnv, respondedErr := extractNodeConfigQueryEnvelope(request, response)
	if respondedErr {
		return
	}

	err, code := VerifyRequestSignature(c.sigVerifier, queryEnv.Payload.UserID, queryEnv.Signature, queryEnv.Payload)
	if err != nil {
		SendHTTPResponse(response, code, err)
		return
	}

	config, err := c.db.GetNodeConfig(queryEnv.GetPayload().GetNodeID())

	if err != nil {
		SendHTTPResponse(
			response,
			http.StatusInternalServerError,
			&ResponseErr{"error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error()},
		)
		return
	}

	SendHTTPResponse(response, http.StatusOK, config)
}

func (c *configRequestHandler) configTransaction(response http.ResponseWriter, request *http.Request) {
	d := json.NewDecoder(request.Body)
	d.DisallowUnknownFields()

	txEnv := &types.ConfigTxEnvelope{}
	if err := d.Decode(txEnv); err != nil {
		SendHTTPResponse(response, http.StatusBadRequest, &ResponseErr{err.Error()})
		return
	}

	if txEnv.Payload == nil {
		SendHTTPResponse(response, http.StatusBadRequest,
			&ResponseErr{fmt.Sprintf("missing transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if txEnv.Payload.UserID == "" {
		SendHTTPResponse(response, http.StatusBadRequest,
			&ResponseErr{fmt.Sprintf("missing UserID in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if len(txEnv.Signature) == 0 {
		SendHTTPResponse(response, http.StatusBadRequest,
			&ResponseErr{fmt.Sprintf("missing Signature in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if err, code := VerifyRequestSignature(c.sigVerifier, txEnv.Payload.UserID, txEnv.Signature, txEnv.Payload); err != nil {
		SendHTTPResponse(response, code, &ResponseErr{err.Error()})
		return
	}

	c.txHandler.handleTransaction(response, txEnv)
}

