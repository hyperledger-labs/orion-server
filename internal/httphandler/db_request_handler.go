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
		sigVerifier: cryptoservice.NewVerifier(db),
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
	queryEnv, respondedErr := extractDBStatusQueryEnvelope(request, response)
	if respondedErr {
		return
	}

	err, status := VerifyRequestSignature(d.sigVerifier, queryEnv.Payload.UserID, queryEnv.Signature, queryEnv.Payload)
	if err != nil {
		SendHTTPResponse(response, status, err)
		return
	}

	dbStatus, err := d.db.GetDBStatus(queryEnv.Payload.DBName)
	if err != nil {
		SendHTTPResponse(
			response,
			http.StatusInternalServerError,
			&ResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			},
		)
		return
	}

	SendHTTPResponse(response, http.StatusOK, dbStatus)
}

func (d *dbRequestHandler) dbTransaction(response http.ResponseWriter, request *http.Request) {
	dbRequestBody := json.NewDecoder(request.Body)
	dbRequestBody.DisallowUnknownFields()

	txEnv := &types.DBAdministrationTxEnvelope{}
	if err := dbRequestBody.Decode(txEnv); err != nil {
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

	if err, code := VerifyRequestSignature(d.sigVerifier, txEnv.Payload.UserID, txEnv.Signature, txEnv.Payload); err != nil {
		SendHTTPResponse(response, code, &ResponseErr{err.Error()})
		return
	}

	d.txHandler.handleTransaction(response, txEnv)
}
