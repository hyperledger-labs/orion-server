package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
)

type dbRequestHandler struct {
	db        backend.DB
	router    *mux.Router
	txHandler *txHandler
}

func (d *dbRequestHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	d.router.ServeHTTP(response, request)
}

func (d *dbRequestHandler) dbStatus(response http.ResponseWriter, request *http.Request) {
	_, _, composedErr := ExtractCreds(d.db, response, request)
	if composedErr {
		return
	}

	params := mux.Vars(request)
	dbName, ok := params["dbname"]
	if !ok {
		SendHTTPResponse(response, http.StatusBadRequest, "query error - bad or missing database name")
		return
	}

	//TODO: verify signature

	dbStatus, err := d.db.GetDBStatus(dbName)
	if err != nil {
		SendHTTPResponse(
			response,
			http.StatusInternalServerError,
			&ResponseErr{
				Error: "error while processing [" + request.URL.String() + "] because " + err.Error(),
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
func NewDBRequestHandler(db backend.DB) *dbRequestHandler {
	handler := &dbRequestHandler{
		db:     db,
		router: mux.NewRouter(),
		txHandler: &txHandler{
			db: db,
		},
	}

	handler.router.HandleFunc(constants.GetDBStatus, handler.dbStatus).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.PostDBTx, handler.dbTransaction).Methods(http.MethodPost)

	return handler
}
