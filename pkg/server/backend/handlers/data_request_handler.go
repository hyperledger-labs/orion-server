package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
)

type dataRequestHandler struct {
	db        backend.DB
	router    *mux.Router
	txHandler *txHandler
	logger    *logger.SugarLogger
}

func (d *dataRequestHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	d.router.ServeHTTP(response, request)
}

func (d *dataRequestHandler) dataQuery(response http.ResponseWriter, request *http.Request) {
	userID, _, composedErr := ExtractCreds(d.db, response, request)
	if composedErr {
		return
	}

	params := mux.Vars(request)
	dbName, ok := params["dbname"]
	if !ok {
		SendHTTPResponse(response,
			http.StatusBadRequest,
			&ResponseErr{
				Error: "query error - bad or missing database name",
			})
		return
	}

	key, ok := params["key"]
	if !ok {
		SendHTTPResponse(response,
			http.StatusBadRequest,
			&ResponseErr{
				Error: "query error - bad or missing key",
			})
		return
	}

	//TODO: verify signature
	data, err := d.db.GetData(dbName, userID, key)
	if err != nil {
		var status int

		switch err.(type) {
		case *backend.PermissionErr:
			status = http.StatusForbidden
		default:
			status = http.StatusInternalServerError
		}

		SendHTTPResponse(
			response,
			status,
			&ResponseErr{
				Error: "error while processing [" + request.URL.String() + "] because " + err.Error(),
			})
		return
	}

	SendHTTPResponse(response, http.StatusOK, data)
}

func (d *dataRequestHandler) dataTransaction(response http.ResponseWriter, request *http.Request) {
	requestData := json.NewDecoder(request.Body)
	requestData.DisallowUnknownFields()

	tx := &types.DataTxEnvelope{}
	if err := requestData.Decode(tx); err != nil {
		SendHTTPResponse(response, http.StatusBadRequest, &ResponseErr{err.Error()})
		return
	}

	// TODO: verify signature
	d.txHandler.HandleTransaction(response, tx.GetPayload().GetUserID(), tx)
}

// NewDataRequestHandler returns handler capable to serve incoming data requests
func NewDataRequestHandler(db backend.DB, logger *logger.SugarLogger) *dataRequestHandler {
	handler := &dataRequestHandler{
		db:     db,
		router: mux.NewRouter(),
		txHandler: &txHandler{
			db: db,
		},
		logger: logger,
	}

	handler.router.HandleFunc(constants.GetData, handler.dataQuery).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.PostDataTx, handler.dataTransaction).Methods(http.MethodPost)

	return handler
}
