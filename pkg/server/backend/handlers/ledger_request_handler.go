package handlers

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
)

type ledgerRequestHandler struct {
	db     backend.DB
	router *mux.Router
	logger *logger.SugarLogger
}

// NewLedgerRequestHandler creates users request handler
func NewLedgerRequestHandler(db backend.DB, logger *logger.SugarLogger) *ledgerRequestHandler {
	handler := &ledgerRequestHandler{
		db:     db,
		router: mux.NewRouter(),
		logger: logger,
	}

	// HTTP GET "/ledger/block/{blockId}" gets block header
	handler.router.HandleFunc(constants.GetBlockHeader, handler.blockQuery).Methods(http.MethodGet)
	// HTTP GET "/ledger/path/{startId}/{endId}" gets shortest path between blocks
	handler.router.HandleFunc(constants.GetPath, handler.pathQuery).Methods(http.MethodGet)

	return handler
}

func (p *ledgerRequestHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	p.router.ServeHTTP(responseWriter, request)
}

func (p *ledgerRequestHandler) blockQuery(response http.ResponseWriter, request *http.Request) {
	userID, _, composedErr := ExtractCreds(p.db, response, request)
	if composedErr {
		return
	}

	params := mux.Vars(request)
	blockNum, respErr := getUintParam("blockId", params)
	if respErr != nil {
		SendHTTPResponse(response, http.StatusBadRequest, respErr)
		return
	}

	//TODO: verify signature
	data, err := p.db.GetBlockHeader(userID, blockNum)
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

func (p *ledgerRequestHandler) pathQuery(response http.ResponseWriter, request *http.Request) {
	userID, _, composedErr := ExtractCreds(p.db, response, request)
	if composedErr {
		return
	}

	params := mux.Vars(request)
	startNum, respErr := getUintParam("startId", params)
	if respErr != nil {
		SendHTTPResponse(response, http.StatusBadRequest, respErr)
		return
	}

	endNum, respErr := getUintParam("endId", params)
	if respErr != nil {
		SendHTTPResponse(response, http.StatusBadRequest, respErr)
		return
	}

	//TODO: verify signature
	data, err := p.db.GetLedgerPath(userID, startNum, endNum)
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

func getUintParam(key string, params map[string]string) (uint64, *ResponseErr) {
	valStr, ok := params[key]
	if !ok {
		return 0, &ResponseErr{
			Error: "query error - bad or missing block number literal" + key,
		}
	}
	val, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return 0, &ResponseErr{
			Error: "query error - bad or missing block number literal " + key + " " + err.Error(),
		}
	}
	return val, nil
}
