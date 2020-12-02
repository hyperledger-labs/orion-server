package handlers

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/server/internal/server/backend"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// provenanceRequestHandler handles query and transaction associated
// with the cluster configuration
type provenanceRequestHandler struct {
	db          backend.DB
	sigVerifier *cryptoservice.SignatureVerifier
	router      *mux.Router
	txHandler   *txHandler
	logger      *logger.SugarLogger
}

// NewProvenanceRequestHandler return config query and transactions request handler
func NewProvenanceRequestHandler(db backend.DB, logger *logger.SugarLogger) http.Handler {
	handler := &provenanceRequestHandler{
		db:          db,
		sigVerifier: cryptoservice.NewVerifier(db),
		router:      mux.NewRouter(),
		txHandler: &txHandler{
			db: db,
		},
		logger: logger,
	}

	matcher := []string{
		"blocknumber", "{blknum:[0-9]+}",
		"transactionnumber", "{txnum:[0-9]+}",
		"direction", "{direction:[previous|next]+}",
	}
	handler.router.HandleFunc(constants.GetHistoricalData, handler.getHistoricalData).Methods(http.MethodGet).Queries(matcher...)
	handler.router.HandleFunc(constants.GetHistoricalData, handler.getHistoricalData).Methods(http.MethodGet).Queries(matcher[:4]...)
	handler.router.HandleFunc(constants.GetHistoricalData, handler.getHistoricalData).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetDataReaders, handler.getDataReaders).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetDataWriters, handler.getDataWriters).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetDataReadBy, handler.getDataReadByUser).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetDataWrittenBy, handler.getDataWrittenByUser).Methods(http.MethodGet)
	handler.router.HandleFunc(constants.GetTxIDsSubmittedBy, handler.getTxIDsSubmittedBy).Methods(http.MethodGet)

	return handler
}

func (p *provenanceRequestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.router.ServeHTTP(w, r)
}

func (p *provenanceRequestHandler) getHistoricalData(w http.ResponseWriter, r *http.Request) {
	payload, respondedErr := p.extractVerifiedQueryPayload(w, r, constants.GetHistoricalData)
	if respondedErr {
		return
	}
	query := payload.(*types.GetHistoricalDataQuery)

	var response *types.GetHistoricalDataResponseEnvelope
	var err error

	switch {
	case query.Version == nil:
		response, err = p.db.GetValues(query.DBName, query.Key)
	case query.Direction == "":
		response, err = p.db.GetValueAt(query.DBName, query.Key, query.Version)
	case query.Direction == "previous":
		response, err = p.db.GetPreviousValues(query.DBName, query.Key, query.Version)
	case query.Direction == "next":
		response, err = p.db.GetNextValues(query.DBName, query.Key, query.Version)
	default:
		SendHTTPResponse(w, http.StatusBadRequest, &ResponseErr{
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
	payload, respondedErr := p.extractVerifiedQueryPayload(w, r, constants.GetDataReaders)
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
	payload, respondedErr := p.extractVerifiedQueryPayload(w, r, constants.GetDataWriters)
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
	payload, respondedErr := p.extractVerifiedQueryPayload(w, r, constants.GetDataReadBy)
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
	payload, respondedErr := p.extractVerifiedQueryPayload(w, r, constants.GetDataWrittenBy)
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

func (p *provenanceRequestHandler) getTxIDsSubmittedBy(w http.ResponseWriter, r *http.Request) {
	payload, respondedErr := p.extractVerifiedQueryPayload(w, r, constants.GetTxIDsSubmittedBy)
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

func (p *provenanceRequestHandler) extractVerifiedQueryPayload(w http.ResponseWriter, r *http.Request, queryType string) (interface{}, bool) {
	querierUserID, signature, err := validateAndParseHeader(&r.Header)
	if err != nil {
		SendHTTPResponse(w, http.StatusBadRequest, &ResponseErr{ErrMsg: err.Error()})
		return nil, true
	}

	var payload interface{}
	params := mux.Vars(r)

	switch queryType {
	case constants.GetHistoricalData:
		version, err := getVersion(params)
		if err != nil {
			SendHTTPResponse(w, http.StatusBadRequest, err)
			return nil, true
		}

		payload = &types.GetHistoricalDataQuery{
			UserID:    querierUserID,
			DBName:    params["dbname"],
			Key:       params["key"],
			Version:   version,
			Direction: params["direction"],
		}
	case constants.GetDataReaders:
		payload = &types.GetDataReadersQuery{
			UserID: querierUserID,
			DBName: params["dbname"],
			Key:    params["key"],
		}
	case constants.GetDataWriters:
		payload = &types.GetDataWritersQuery{
			UserID: querierUserID,
			DBName: params["dbname"],
			Key:    params["key"],
		}
	case constants.GetDataReadBy:
		payload = &types.GetDataReadByQuery{
			UserID:       querierUserID,
			TargetUserID: params["userId"],
		}
	case constants.GetDataWrittenBy:
		payload = &types.GetDataWrittenByQuery{
			UserID:       querierUserID,
			TargetUserID: params["userId"],
		}
	case constants.GetTxIDsSubmittedBy:
		payload = &types.GetTxIDsSubmittedByQuery{
			UserID:       querierUserID,
			TargetUserID: params["userId"],
		}
	}

	fmt.Println(payload)

	err, status := VerifyRequestSignature(p.sigVerifier, querierUserID, signature, payload)
	if err != nil {
		SendHTTPResponse(w, status, err)
		return nil, true
	}

	return payload, false
}

func getVersion(params map[string]string) (*types.Version, error) {
	if _, ok := params["blknum"]; !ok {
		return nil, nil
	}

	blockNum, err := strconv.ParseUint(params["blknum"], 10, 64)
	if err != nil {
		return nil, &ResponseErr{
			ErrMsg: "query error - bad block number: " + err.Error(),
		}
	}

	txNum, err := strconv.ParseUint(params["txnum"], 10, 64)
	if err != nil {
		return nil, &ResponseErr{
			ErrMsg: "query error - bad transaction number: " + err.Error(),
		}
	}

	return &types.Version{
		BlockNum: blockNum,
		TxNum:    txNum,
	}, nil
}

func processInternalError(w http.ResponseWriter, r *http.Request, err error) {
	SendHTTPResponse(
		w,
		http.StatusInternalServerError,
		&ResponseErr{
			ErrMsg: "error while processing '" + r.Method + " " + r.URL.String() + "' because " + err.Error(),
		},
	)
}
