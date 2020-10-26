package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
)

type configRequestHandler struct {
	db        backend.DB
	router    *mux.Router
	txHandler *txHandler
}

func (c *configRequestHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	c.router.ServeHTTP(response, request)
}

func (c *configRequestHandler) configQuery(response http.ResponseWriter, request *http.Request) {
	_, _, composedErr := ExtractCreds(c.db, response, request)
	if composedErr {
		return
	}

	config, err := c.db.GetConfig()
	if err != nil {
		SendHTTPResponse(
			response,
			http.StatusInternalServerError,
			&ResponseErr{"error while processing [" + request.URL.String() + "] because " + err.Error()},
		)
		return
	}

	SendHTTPResponse(response, http.StatusOK, config)
}

func (c *configRequestHandler) configTransaction(response http.ResponseWriter, request *http.Request) {
	d := json.NewDecoder(request.Body)
	d.DisallowUnknownFields()

	tx := &types.ConfigTxEnvelope{}
	if err := d.Decode(tx); err != nil {
		SendHTTPResponse(response, http.StatusBadRequest, &ResponseErr{err.Error()})
		return
	}

	// TODO: verify signature
	c.txHandler.HandleTransaction(response, tx.GetPayload().GetUserID(), tx)
}

// NewConfigRequestHandler return config transactions request handler
func NewConfigRequestHandler(db backend.DB) *configRequestHandler {
	handler := &configRequestHandler{
		db:     db,
		router: mux.NewRouter(),
		txHandler: &txHandler{
			db: db,
		},
	}

	handler.router.HandleFunc(constants.GetConfig, handler.configQuery).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.PostConfigTx, handler.configTransaction).Methods(http.MethodPost)

	return handler
}
