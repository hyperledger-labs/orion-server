package handlers

import (
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
)

type ledgerRequestHandler struct {
	db          backend.DB
	sigVerifier *cryptoservice.SignatureVerifier
	router      *mux.Router
	logger      *logger.SugarLogger
}

// NewLedgerRequestHandler creates users request handler
func NewLedgerRequestHandler(db backend.DB, logger *logger.SugarLogger) *ledgerRequestHandler {
	handler := &ledgerRequestHandler{
		db:          db,
		sigVerifier: cryptoservice.NewVerifier(db),
		router:      mux.NewRouter(),
		logger:      logger,
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
	queryEnv, respondedErr := extractBlockQueryEnvelope(request, response)
	if respondedErr {
		return
	}

	err, code := VerifyQuerySignature(p.sigVerifier, queryEnv.Payload.UserID, queryEnv.Signature, queryEnv.Payload)
	if err != nil {
		SendHTTPResponse(response, code, err)
		return
	}

	data, err := p.db.GetBlockHeader(queryEnv.Payload.UserID, queryEnv.Payload.BlockNumber)
	if err != nil {
		var status int

		switch err.(type) {
		case *backend.PermissionErr:
			status = http.StatusForbidden
		default:
			status = http.StatusInternalServerError // TODO deal with 404 not found, it's not a 5xx
		}

		SendHTTPResponse(
			response,
			status,
			&ResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	SendHTTPResponse(response, http.StatusOK, data)
}

func (p *ledgerRequestHandler) pathQuery(response http.ResponseWriter, request *http.Request) {
	queryEnv, respondedErr := extractLedgerPathQueryEnvelope(request, response)
	if respondedErr {
		return
	}

	err, code := VerifyQuerySignature(p.sigVerifier, queryEnv.Payload.UserID, queryEnv.Signature, queryEnv.Payload)
	if err != nil {
		SendHTTPResponse(response, code, err)
		return
	}

	data, err := p.db.GetLedgerPath(queryEnv.Payload.UserID, queryEnv.Payload.StartBlockNumber, queryEnv.Payload.EndBlockNumber)
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
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	SendHTTPResponse(response, http.StatusOK, data)
}

func getUintParam(key string, params map[string]string) (uint64, *ResponseErr) {
	valStr, ok := params[key]
	if !ok {
		return 0, &ResponseErr{
			ErrMsg: "query error - bad or missing block number literal" + key,
		}
	}
	val, err := strconv.ParseUint(valStr, 10, 64)
	if err != nil {
		return 0, &ResponseErr{
			ErrMsg: "query error - bad or missing block number literal " + key + " " + err.Error(),
		}
	}
	return val, nil
}
