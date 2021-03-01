// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/server/internal/bcdb"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// provenanceRequestHandler handles query and transaction associated
// with the cluster configuration
type provenanceRequestHandler struct {
	db          bcdb.DB
	sigVerifier *cryptoservice.SignatureVerifier
	router      *mux.Router
	txHandler   *txHandler
	logger      *logger.SugarLogger
}

// NewProvenanceRequestHandler return config query and transactions request handler
func NewProvenanceRequestHandler(db bcdb.DB, logger *logger.SugarLogger) http.Handler {
	handler := &provenanceRequestHandler{
		db:          db,
		sigVerifier: cryptoservice.NewVerifier(db, logger),
		router:      mux.NewRouter(),
		txHandler: &txHandler{
			db: db,
		},
		logger: logger,
	}

	versionAndDirectionMatcher := []string{
		"blocknumber", "{blknum:[0-9]+}",
		"transactionnumber", "{txnum:[0-9]+}",
		"direction", "{direction:[previous|next]+}",
	}

	mostRecentMatcher := []string{
		"blocknumber", "{blknum:[0-9]+}",
		"transactionnumber", "{txnum:[0-9]+}",
		"mostrecent", "{mostrecent:true}",
	}
	handler.router.HandleFunc(constants.GetHistoricalData, handler.getHistoricalData).Methods(http.MethodGet).Queries(versionAndDirectionMatcher...)
	handler.router.HandleFunc(constants.GetHistoricalData, handler.getHistoricalData).Methods(http.MethodGet).Queries(mostRecentMatcher...)
	handler.router.HandleFunc(constants.GetHistoricalData, handler.getHistoricalData).Methods(http.MethodGet).Queries(versionAndDirectionMatcher[:4]...)
	handler.router.HandleFunc(constants.GetHistoricalData, handler.getHistoricalData).Methods(http.MethodGet).Queries("onlydeletes", "{onlydeletes:true}")
	handler.router.HandleFunc(constants.GetHistoricalData, handler.getHistoricalData).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetDataReaders, handler.getDataReaders).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetDataWriters, handler.getDataWriters).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetDataReadBy, handler.getDataReadByUser).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetDataWrittenBy, handler.getDataWrittenByUser).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetDataDeletedBy, handler.getDataDeletedByUser).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetTxIDsSubmittedBy, handler.getTxIDsSubmittedBy).Methods(http.MethodGet)

	return handler
}

func (p *provenanceRequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.router.ServeHTTP(w, r)
}

func (p *provenanceRequestHandler) getHistoricalData(w http.ResponseWriter, r *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(w, r, constants.GetHistoricalData, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetHistoricalDataQuery)

	var response *types.ResponseEnvelope
	var err error

	switch {
	case query.OnlyDeletes:
		response, err = p.db.GetDeletedValues(query.DBName, query.Key)
	case query.Version == nil:
		response, err = p.db.GetValues(query.DBName, query.Key)
	case query.Direction == "" && query.MostRecent:
		response, err = p.db.GetMostRecentValueAtOrBelow(query.DBName, query.Key, query.Version)
	case query.Direction == "":
		response, err = p.db.GetValueAt(query.DBName, query.Key, query.Version)
	case query.Direction == "previous":
		response, err = p.db.GetPreviousValues(query.DBName, query.Key, query.Version)
	case query.Direction == "next":
		response, err = p.db.GetNextValues(query.DBName, query.Key, query.Version)
	default:
		SendHTTPResponse(w, http.StatusBadRequest, &types.HttpResponseErr{
			ErrMsg: "direction must be either [previous] or [next]",
		})
	}

	if err != nil {
		processInternalError(w, r, err)
		return
	}

	SendHTTPResponse(w, http.StatusOK, response)
}

func (p *provenanceRequestHandler) getDataReaders(w http.ResponseWriter, r *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(w, r, constants.GetDataReaders, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetDataReadersQuery)

	response, err := p.db.GetReaders(query.DBName, query.Key)
	if err != nil {
		processInternalError(w, r, err)
		return
	}

	SendHTTPResponse(w, http.StatusOK, response)
}

func (p *provenanceRequestHandler) getDataWriters(w http.ResponseWriter, r *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(w, r, constants.GetDataWriters, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetDataWritersQuery)

	response, err := p.db.GetWriters(query.DBName, query.Key)
	if err != nil {
		processInternalError(w, r, err)
		return
	}

	SendHTTPResponse(w, http.StatusOK, response)
}

func (p *provenanceRequestHandler) getDataReadByUser(w http.ResponseWriter, r *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(w, r, constants.GetDataReadBy, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetDataReadByQuery)

	response, err := p.db.GetValuesReadByUser(query.TargetUserID)
	if err != nil {
		processInternalError(w, r, err)
		return
	}

	SendHTTPResponse(w, http.StatusOK, response)
}

func (p *provenanceRequestHandler) getDataWrittenByUser(w http.ResponseWriter, r *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(w, r, constants.GetDataWrittenBy, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetDataWrittenByQuery)

	response, err := p.db.GetValuesWrittenByUser(query.TargetUserID)
	if err != nil {
		processInternalError(w, r, err)
		return
	}

	SendHTTPResponse(w, http.StatusOK, response)
}

func (p *provenanceRequestHandler) getDataDeletedByUser(w http.ResponseWriter, r *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(w, r, constants.GetDataDeletedBy, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetDataDeletedByQuery)

	response, err := p.db.GetValuesDeletedByUser(query.TargetUserID)
	if err != nil {
		processInternalError(w, r, err)
		return
	}

	SendHTTPResponse(w, http.StatusOK, response)
}

func (p *provenanceRequestHandler) getTxIDsSubmittedBy(w http.ResponseWriter, r *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(w, r, constants.GetTxIDsSubmittedBy, p.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetTxIDsSubmittedByQuery)

	response, err := p.db.GetTxIDsSubmittedByUser(query.TargetUserID)
	if err != nil {
		processInternalError(w, r, err)
		return
	}

	SendHTTPResponse(w, http.StatusOK, response)
}

func processInternalError(w http.ResponseWriter, r *http.Request, err error) {
	SendHTTPResponse(
		w,
		http.StatusInternalServerError,
		&types.HttpResponseErr{
			ErrMsg: "error while processing '" + r.Method + " " + r.URL.String() + "' because " + err.Error(),
		},
	)
}
