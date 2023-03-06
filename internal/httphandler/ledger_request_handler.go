// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/hyperledger-labs/orion-server/internal/bcdb"
	"github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
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

	// HTTP GET "/ledger/block/{blockId}?augmented=true" gets augmented block header
	handler.router.HandleFunc(constants.GetBlockHeader, handler.blockQuery).Methods(http.MethodGet).Queries("augmented", "{isAugmented:true|false}")
	// HTTP GET "/ledger/block/{blockId}" gets block header
	handler.router.HandleFunc(constants.GetBlockHeader, handler.blockQuery).Methods(http.MethodGet)
	// HTTP GET "/ledger/block/last" gets last ledger block header
	handler.router.HandleFunc(constants.GetLastBlockHeader, handler.lastBlockQuery).Methods(http.MethodGet)
	// HTTP GET "/ledger/path?start={startId}&end={endId}" gets shortest path between blocks
	handler.router.HandleFunc(constants.GetPath, handler.pathQuery).Methods(http.MethodGet).Queries("start", "{startId:[0-9]+}", "end", "{endId:[0-9]+}")
	// HTTP GET "/ledger/proof/tx/{blockId}?idx={idx}" gets proof for tx with index idx inside block blockId
	handler.router.HandleFunc(constants.GetTxProof, handler.txProof).Methods(http.MethodGet).Queries("idx", "{idx:[0-9]+}")
	// HTTP GET "/ledger/proof/data/{blockId}/{dbname}/{key}?deleted={true|false}" gets proof for value associated with (dbname, key) in block blockId,
	// deleted indicates if value existed in the past and was deleted
	handler.router.HandleFunc(constants.GetDataProof, handler.dataProof).Methods(http.MethodGet).Queries("block", "{blockId:[0-9]+}", "deleted", "{deleted:true|false}")
	// HTTP GET "/ledger/proof/data/{blockId}/{dbname}/{key}" gets proof for value associated with (dbname, key) in block blockId
	handler.router.HandleFunc(constants.GetDataProof, handler.dataProof).Methods(http.MethodGet).Queries("block", "{blockId:[0-9]+}")
	// HTTP GET "/ledger/tx/receipt/{txId}" gets transaction receipt
	handler.router.HandleFunc(constants.GetTxReceipt, handler.txReceipt).Methods(http.MethodGet)
	// HTTP GET "/ledger/tx/content/{blockId}?idx={idx}" gets the tx envelope with index idx inside block blockId
	handler.router.HandleFunc(constants.GetTxContent, handler.txContent).Methods(http.MethodGet).Queries("idx", "{idx:[0-9]+}")
	// HTTP GET "/ledger/path?start={startId}&end={endId}" with invalid query params
	handler.router.HandleFunc(constants.GetPath, handler.invalidPathQuery).Methods(http.MethodGet)
	// HTTP GET "/ledger/proof/tx/{blockId}?idx={idx}" with invalid query params
	handler.router.HandleFunc(constants.GetTxProofPrefix, handler.invalidTxProof).Methods(http.MethodGet)
	// HTTP GET "/ledger/proof/tx/{blockId}?idx={idx}" with invalid query params
	handler.router.HandleFunc(constants.GetTxProof, handler.invalidTxProof).Methods(http.MethodGet)
	// HTTP GET "/ledger/proof/data/{blockId}/{dbname}/{key}" with invalid query params
	handler.router.HandleFunc(constants.GetDataProofPrefix, handler.invalidDataProof).Methods(http.MethodGet)
	// HTTP GET "/ledger/proof/data/{blockId}/{dbname}/{key}" with invalid query params
	handler.router.HandleFunc(constants.GetDataProofPrefix+"/{dbname}", handler.invalidDataProof).Methods(http.MethodGet)
	// HTTP GET "/ledger/proof/data/{blockId}/{dbname}/{key}" with invalid query params
	handler.router.HandleFunc(constants.GetDataProofPrefix+"/{dbname}/{key}", handler.invalidDataProof).Methods(http.MethodGet)
	// HTTP GET "/ledger/tx/content/{blockId}?idx={idx}" with invalid query params
	handler.router.HandleFunc(constants.GetTxContentPrefix, handler.invalidTxContent).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetTxContent, handler.invalidTxContent).Methods(http.MethodGet)

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

	var data interface{}
	var err error
	if query.Augmented {
		data, err = p.db.GetAugmentedBlockHeader(query.UserId, query.BlockNumber)
	} else {
		data, err = p.db.GetBlockHeader(query.UserId, query.BlockNumber)
	}
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

		utils.SendHTTPResponse(
			response,
			status,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	utils.SendHTTPResponse(response, http.StatusOK, data)
}

func (p *ledgerRequestHandler) lastBlockQuery(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetLastBlockHeader, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetLastBlockQuery)

	var data *types.GetBlockResponseEnvelope

	height, err := p.db.Height()
	if err == nil {
		data, err = p.db.GetBlockHeader(query.UserId, height)
	}
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

		utils.SendHTTPResponse(
			response,
			status,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	utils.SendHTTPResponse(response, http.StatusOK, data)
}

func (p *ledgerRequestHandler) pathQuery(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetPath, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetLedgerPathQuery)

	data, err := p.db.GetLedgerPath(query.UserId, query.StartBlockNumber, query.EndBlockNumber)
	if err != nil {
		var status int

		switch err.(type) {
		case *errors.PermissionErr:
			status = http.StatusForbidden
		case *errors.NotFoundErr:
			status = http.StatusNotFound
		case *errors.BadRequestError:
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

	utils.SendHTTPResponse(response, http.StatusOK, data)
}

func (p *ledgerRequestHandler) txProof(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetTxProof, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetTxProofQuery)

	data, err := p.db.GetTxProof(query.UserId, query.BlockNumber, query.TxIndex)
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

		utils.SendHTTPResponse(
			response,
			status,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	utils.SendHTTPResponse(response, http.StatusOK, data)
}

func (p *ledgerRequestHandler) dataProof(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetDataProof, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetDataProofQuery)
	data, err := p.db.GetDataProof(query.UserId, query.BlockNumber, query.DbName, query.Key, query.IsDeleted)
	if err != nil {
		var status int

		switch err.(type) {
		case *errors.PermissionErr:
			status = http.StatusForbidden
		case *errors.NotFoundErr:
			status = http.StatusNotFound
		case *errors.ServerRestrictionError:
			status = http.StatusServiceUnavailable
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

	utils.SendHTTPResponse(response, http.StatusOK, data)
}

func (p *ledgerRequestHandler) txReceipt(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetTxReceipt, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetTxReceiptQuery)

	data, err := p.db.GetTxReceipt(query.UserId, query.TxId)
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

		utils.SendHTTPResponse(
			response,
			status,
			&types.HttpResponseErr{
				ErrMsg: "error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error(),
			})
		return
	}

	utils.SendHTTPResponse(response, http.StatusOK, data)
}

func (p *ledgerRequestHandler) txContent(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetTxContent, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetTxContentQuery)

	data, err := p.db.GetTx(query.GetUserId(), query.GetBlockNumber(), query.GetTxIndex())
	if err != nil {
		var status int

		switch err.(type) {
		case *errors.PermissionErr:
			status = http.StatusForbidden
		case *errors.NotFoundErr:
			status = http.StatusNotFound
		case *errors.BadRequestError:
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

	utils.SendHTTPResponse(response, http.StatusOK, data)
}

func (p *ledgerRequestHandler) invalidPathQuery(response http.ResponseWriter, request *http.Request) {
	err := &types.HttpResponseErr{
		ErrMsg: "query error - bad or missing start/end block number",
	}
	utils.SendHTTPResponse(response, http.StatusBadRequest, err)
}

func (p *ledgerRequestHandler) invalidTxProof(response http.ResponseWriter, request *http.Request) {
	err := &types.HttpResponseErr{
		ErrMsg: "tx proof query error - bad or missing query parameter",
	}
	utils.SendHTTPResponse(response, http.StatusBadRequest, err)
}

func (p *ledgerRequestHandler) invalidDataProof(response http.ResponseWriter, request *http.Request) {
	err := &types.HttpResponseErr{
		ErrMsg: "data proof query error - bad or missing query parameter",
	}
	utils.SendHTTPResponse(response, http.StatusBadRequest, err)
}

func (p *ledgerRequestHandler) invalidTxContent(response http.ResponseWriter, request *http.Request) {
	err := &types.HttpResponseErr{
		ErrMsg: "tx content query error - bad or missing query parameter",
	}
	utils.SendHTTPResponse(response, http.StatusBadRequest, err)
}
