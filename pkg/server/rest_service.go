package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/server"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type DBServer struct {
	router *mux.Router
	qs     *queryProcessor
	ts     *transactionProcessor
}

type ResponseErr struct {
	Error string `json:"error,omitempty"`
}

func NewDBServer() (*DBServer, error) {
	rs := &DBServer{}

	dbConf := config.Database()
	if dbConf.Name != "leveldb" {
		return nil, errors.New("only leveldb is supported as the state database")
	}

	db, err := leveldb.NewLevelDB(dbConf.LedgerDirectory)
	if err != nil {
		return nil, errors.WithMessagef(err, "failed to create a new leveldb instance for the peer")
	}

	log.Println("Starting query processor")
	rs.qs = newQueryProcessor(db)
	log.Println("Starting transaction processor")
	rs.ts = newTransactionProcessor(db)

	rs.router = mux.NewRouter()
	rs.router.HandleFunc("/db/{dbname}/state/{key}", rs.handleDataQuery).Methods(http.MethodGet)
	rs.router.HandleFunc("/db/{dbname}", rs.handleStatusQuery).Methods(http.MethodGet)
	rs.router.HandleFunc("/tx", rs.handleTransactionSubmit).Methods(http.MethodPost)
	return rs, nil
}

func (rs *DBServer) handleStatusQuery(w http.ResponseWriter, r *http.Request) {
	userId, signature, err := validateAndParseQueryHeader(r)
	if err != nil {
		composeJSONResponse(w, http.StatusBadRequest, &ResponseErr{Error: err.Error()})
		return
	}

	params := mux.Vars(r)
	dbname, ok := params["dbname"]
	if !ok {
		composeJSONResponse(w, http.StatusBadRequest, &ResponseErr{Error: "query error - bad or missing database name"})
		return
	}
	dbQueryEnvelope := &types.GetStatusQueryEnvelope{
		Payload: &types.GetStatusQuery{
			UserID: userId,
			DBName: dbname,
		},
		Signature: signature,
	}

	statusEnvelope, err := rs.qs.GetStatus(context.Background(), dbQueryEnvelope)
	if err != nil {
		composeJSONResponse(w, http.StatusInternalServerError, &ResponseErr{Error: fmt.Sprintf("error while processing %v, %v", dbQueryEnvelope, err)})
		return
	}
	composeJSONResponse(w, http.StatusOK, statusEnvelope)
}

func (rs *DBServer) handleDataQuery(w http.ResponseWriter, r *http.Request) {
	userid, signature, err := validateAndParseQueryHeader(r)
	if err != nil {
		composeJSONResponse(w, http.StatusBadRequest, &ResponseErr{Error: err.Error()})
		return
	}

	params := mux.Vars(r)
	dbname, ok := params["dbname"]
	if !ok {
		composeJSONResponse(w, http.StatusBadRequest, &ResponseErr{Error: "query error - bad or missing database name"})
		return
	}
	key, ok := params["key"]
	if !ok {
		composeJSONResponse(w, http.StatusBadRequest, &ResponseErr{Error: "query error - bad or missing key"})
		return
	}
	dataQueryEnvelope := &types.GetStateQueryEnvelope{
		Payload: &types.GetStateQuery{
			UserID: userid,
			DBName: dbname,
			Key:    key,
		},
		Signature: signature,
	}
	valueEnvelope, err := rs.qs.GetState(context.Background(), dataQueryEnvelope)

	if err != nil {
		composeJSONResponse(w, http.StatusInternalServerError, &ResponseErr{Error: fmt.Sprintf("error while processing %v, %v", dataQueryEnvelope, err)})
		return
	}
	composeJSONResponse(w, http.StatusOK, valueEnvelope)
}

func (rs *DBServer) handleTransactionSubmit(w http.ResponseWriter, r *http.Request) {
	tx := new(types.TransactionEnvelope)
	err := json.NewDecoder(r.Body).Decode(tx)
	if err != nil {
		composeJSONResponse(w, http.StatusBadRequest, &ResponseErr{Error: err.Error()})
		return
	}
	err = rs.ts.SubmitTransaction(context.Background(), tx)
	if err != nil {
		composeJSONResponse(w, http.StatusInternalServerError, &ResponseErr{Error: err.Error()})
		return
	}
	composeJSONResponse(w, http.StatusOK, empty.Empty{})
}

func composeJSONResponse(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func validateAndParseQueryHeader(r *http.Request) (string, []byte, error) {
	userID := r.Header.Get(server.UserHeader)
	if userID == "" {
		return "", nil, errors.New("empty user")
	}

	signature := r.Header.Get(server.SignatureHeader)
	if signature == "" {
		return "", nil, errors.New("empty signature")
	}
	signatureBytes, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return "", nil, errors.New("wrongly encoded signature")
	}

	return userID, signatureBytes, nil
}
