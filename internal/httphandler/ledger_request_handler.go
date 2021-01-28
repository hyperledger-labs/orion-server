package httphandler

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/server/internal/bcdb"
	"github.ibm.com/blockchaindb/server/internal/errors"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// ledgerRequestHandler handles query associated with the
// chain of blocks
type ledgerRequestHandler struct {
	db          bcdb.DB
	sigVerifier *cryptoservice.SignatureVerifier
	router      *mux.Router
	logger      *logger.SugarLogger
}

// NewLedgerRequestHandler creates users request handler
func NewLedgerRequestHandler(db bcdb.DB, logger *logger.SugarLogger) http.Handler {
	handler := &ledgerRequestHandler{
		db:          db,
		sigVerifier: cryptoservice.NewVerifier(db, logger),
		router:      mux.NewRouter(),
		logger:      logger,
	}

	// HTTP GET "/ledger/block/{blockId}" gets block header
	handler.router.HandleFunc(constants.GetBlockHeader, handler.blockQuery).Methods(http.MethodGet)
	// HTTP GET "/ledger/path?start={startId}&end={endId}" gets shortest path between blocks
	handler.router.HandleFunc(constants.GetPath, handler.pathQuery).Methods(http.MethodGet).Queries("start", "{startId:[0-9]+}", "end", "{endId:[0-9]+}")
	// HTTP GET "/ledger/proof/{blockId}?idx={idx}" gets proof for tx with idx index inside block blockId
	handler.router.HandleFunc(constants.GetTxProof, handler.txProof).Methods(http.MethodGet).Queries("idx", "{idx:[0-9]+}")
	// HTTP GET "/ledger/tx/receipt/{txId}" gets transaction receipt
	handler.router.HandleFunc(constants.GetTxReceipt, handler.txReceipt).Methods(http.MethodGet)
	// HTTP GET "/ledger/path?start={startId}&end={endId}" with invalid query params
	handler.router.HandleFunc(constants.GetPath, handler.invalidPathQuery).Methods(http.MethodGet)
	// HTTP GET "/ledger/proof/{blockId}?idx={idx}" with invalid query params
	handler.router.HandleFunc(constants.GetTxProof, handler.invalidTxProof).Methods(http.MethodGet)

	return handler
}

func (p *ledgerRequestHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	p.router.ServeHTTP(responseWriter, request)
}

func (p *ledgerRequestHandler) blockQuery(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetBlockHeader, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetBlockQuery)

	data, err := p.db.GetBlockHeader(query.UserID, query.BlockNumber)
	if err != nil {
		var status int

		switch err.(type) {
		case *errors.PermissionErr:
			status = http.StatusForbidden
		case *errors.NotFoundErr:
			status = http.StatusNotFound
		default:
			status = http.StatusInternalServerError
		}

		SendHTTPResponse(
			response,
			status,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	SendHTTPResponse(response, http.StatusOK, data)
}

func (p *ledgerRequestHandler) pathQuery(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetPath, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetLedgerPathQuery)

	data, err := p.db.GetLedgerPath(query.UserID, query.StartBlockNumber, query.EndBlockNumber)
	if err != nil {
		var status int

		switch err.(type) {
		case *errors.PermissionErr:
			status = http.StatusForbidden
		case *errors.NotFoundErr:
			status = http.StatusNotFound
		default:
			status = http.StatusInternalServerError
		}

		SendHTTPResponse(
			response,
			status,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	SendHTTPResponse(response, http.StatusOK, data)
}

func (p *ledgerRequestHandler) txProof(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetTxProof, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetTxProofQuery)

	data, err := p.db.GetTxProof(query.UserID, query.BlockNumber, query.TxIndex)
	if err != nil {
		var status int

		switch err.(type) {
		case *errors.PermissionErr:
			status = http.StatusForbidden
		case *errors.NotFoundErr:
			status = http.StatusNotFound
		default:
			status = http.StatusInternalServerError
		}

		SendHTTPResponse(
			response,
			status,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	SendHTTPResponse(response, http.StatusOK, data)
}

func (p *ledgerRequestHandler) txReceipt(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetTxReceipt, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetTxReceiptQuery)

	data, err := p.db.GetTxReceipt(query.UserID, query.TxID)
	if err != nil {
		var status int

		switch err.(type) {
		case *errors.PermissionErr:
			status = http.StatusForbidden
		case *errors.NotFoundErr:
			status = http.StatusNotFound
		default:
			status = http.StatusInternalServerError
		}

		SendHTTPResponse(
			response,
			status,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	SendHTTPResponse(response, http.StatusOK, data)
}

func (p *ledgerRequestHandler) invalidPathQuery(response http.ResponseWriter, request *http.Request) {
	err := &types.HttpResponseErr{
		ErrMsg: "query error - bad or missing start/end block number",
	}
	SendHTTPResponse(response, http.StatusBadRequest, err)
}

func (p *ledgerRequestHandler) invalidTxProof(response http.ResponseWriter, request *http.Request) {
	err := &types.HttpResponseErr{
		ErrMsg: "query error - bad or missing tx index",
	}
	SendHTTPResponse(response, http.StatusBadRequest, err)
}
