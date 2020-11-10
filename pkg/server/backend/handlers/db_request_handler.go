package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
)

type dbRequestHandler struct {
	db          backend.DB
	sigVerifier *cryptoservice.SignatureVerifier
	router      *mux.Router
	txHandler   *txHandler
	logger      *logger.SugarLogger
}

func (d *dbRequestHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	d.router.ServeHTTP(response, request)
}

func (d *dbRequestHandler) dbStatus(response http.ResponseWriter, request *http.Request) {
	queryEnv, respondedErr := extractDBStatusQueryEnvelope(request, response)
	if respondedErr {
		return
	}

	err, status := VerifyQuerySignature(d.sigVerifier, queryEnv.Payload.UserID, queryEnv.Signature, queryEnv.Payload)
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

	tx := &types.DBAdministrationTxEnvelope{}
	if err := dbRequestBody.Decode(tx); err != nil {
		SendHTTPResponse(response, http.StatusBadRequest, &ResponseErr{err.Error()})
		return
	}

	// TODO: verify signature
	d.txHandler.HandleTransaction(response, tx.GetPayload().GetUserID(), tx)
}

// NewDBRequestHandler returns DB requests handler
func NewDBRequestHandler(db backend.DB, logger *logger.SugarLogger) *dbRequestHandler {
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
