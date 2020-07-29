package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/server"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

type httpAndDBServer struct {
	listenAddr string
	httpServ   *http.Server
	dbServ     *dbServer
}

var s *httpAndDBServer

// Start starts a the database server and a http server
func Start() error {
	var err error
	if err = config.Init(); err != nil {
		return errors.WithMessagef(err, "error while starting the server")
	}

	s = &httpAndDBServer{}
	s.dbServ, err = newDBServer()
	if err != nil {
		return errors.Wrap(err, "error while starting the database server")
	}

	// TODO: query block store to check whether the chain is empty. If it empty,
	// submit a config transaction
	configTx, err := prepareConfigTransaction()
	if err != nil {
		return errors.Wrap(err, "failed to prepare and commit a configuration transaction")
	}

	if err := s.dbServ.SubmitTransaction(context.Background(), configTx); err != nil {
		return errors.Wrap(err, "error while committing configuration transaction")
	}

	netConf := config.NodeNetwork()
	s.listenAddr = fmt.Sprintf("%s:%d", netConf.Address, netConf.Port)
	log.Printf("Starting the server listening on %s\n", s.listenAddr)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		s.httpServ = &http.Server{
			Addr:    s.listenAddr,
			Handler: s.dbServ.router,
		}

		err = s.httpServ.ListenAndServe()
	}()
	wg.Wait()

	return err
}

// Stop stops the http server
func Stop() error {
	log.Printf("Stopping the server listening on %s\n", s.listenAddr)
	if s == nil || s.httpServ == nil {
		return nil
	}
	return s.httpServ.Close()
}

type dbServer struct {
	router *mux.Router
	*queryProcessor
	*transactionProcessor
}

func newDBServer() (*dbServer, error) {
	var levelDB *leveldb.LevelDB
	var err error

	switch config.Database().Name {
	case "leveldb":
		if levelDB, err = leveldb.New(config.Database().LedgerDirectory); err != nil {
			return nil, errors.WithMessagef(err, "failed to create a new leveldb instance for the peer")
		}
	default:
		return nil, errors.New("only leveldb is supported as the state database")
	}

	db := &dbServer{
		mux.NewRouter(),
		newQueryProcessor(levelDB),
		newTransactionProcessor(levelDB),
	}

	db.router.HandleFunc("/db/{dbname}/state/{key}", db.handleDataQuery).Methods(http.MethodGet)
	db.router.HandleFunc("/db/{dbname}", db.handleStatusQuery).Methods(http.MethodGet)
	db.router.HandleFunc("/tx", db.handleTransaction).Methods(http.MethodPost)

	return db, nil
}

func (db *dbServer) handleStatusQuery(w http.ResponseWriter, r *http.Request) {
	userID, signature, err := validateAndParseHeader(&r.Header)
	if err != nil {
		composeResponse(w, http.StatusBadRequest, &ResponseErr{Error: err.Error()})
		return
	}

	params := mux.Vars(r)
	dbname, ok := params["dbname"]
	if !ok {
		composeResponse(w, http.StatusBadRequest, &ResponseErr{Error: "query error - bad or missing database name"})
		return
	}

	dbQueryEnvelope := &types.GetStatusQueryEnvelope{
		Payload: &types.GetStatusQuery{
			UserID: userID,
			DBName: dbname,
		},
		Signature: signature,
	}

	statusEnvelope, err := db.GetStatus(context.Background(), dbQueryEnvelope)
	if err != nil {
		composeResponse(w, http.StatusInternalServerError,
			&ResponseErr{Error: fmt.Sprintf("error while processing %v, %v", dbQueryEnvelope, err)})
		return
	}
	composeResponse(w, http.StatusOK, statusEnvelope)
}

func (db *dbServer) handleDataQuery(w http.ResponseWriter, r *http.Request) {
	userid, signature, err := validateAndParseHeader(&r.Header)
	if err != nil {
		composeResponse(w, http.StatusBadRequest, &ResponseErr{Error: err.Error()})
		return
	}

	params := mux.Vars(r)
	dbname, ok := params["dbname"]
	if !ok {
		composeResponse(w, http.StatusBadRequest, &ResponseErr{Error: "query error - bad or missing database name"})
		return
	}
	key, ok := params["key"]
	if !ok {
		composeResponse(w, http.StatusBadRequest, &ResponseErr{Error: "query error - bad or missing key"})
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

	valueEnvelope, err := db.GetState(context.Background(), dataQueryEnvelope)
	if err != nil {
		composeResponse(w, http.StatusInternalServerError, &ResponseErr{Error: fmt.Sprintf("error while processing %v, %v", dataQueryEnvelope, err)})
		return
	}
	composeResponse(w, http.StatusOK, valueEnvelope)
}

func (db *dbServer) handleTransaction(w http.ResponseWriter, r *http.Request) {
	tx := &types.TransactionEnvelope{}
	err := json.NewDecoder(r.Body).Decode(tx)
	if err != nil {
		composeResponse(w, http.StatusBadRequest, &ResponseErr{Error: err.Error()})
		return
	}

	err = db.SubmitTransaction(context.Background(), tx)
	if err != nil {
		composeResponse(w, http.StatusInternalServerError, &ResponseErr{Error: err.Error()})
		return
	}
	composeResponse(w, http.StatusOK, empty.Empty{})
}

func validateAndParseHeader(h *http.Header) (string, []byte, error) {
	userID := h.Get(server.UserHeader)
	if userID == "" {
		return "", nil, errors.New(server.UserHeader + " is not set in the http request header")
	}

	signature := h.Get(server.SignatureHeader)
	if signature == "" {
		return "", nil, errors.New(server.SignatureHeader + " is not set in the http request header")
	}
	signatureBytes, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return "", nil, errors.New(server.SignatureHeader + " is not encoded correctly")
	}

	return userID, signatureBytes, nil
}

func composeResponse(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if _, err := w.Write(response); err != nil {
		log.Printf("Warning: failed to write response [%v] to the response writer\n", w)
	}
}

// ResponseErr holds the error response
type ResponseErr struct {
	Error string `json:"error,omitempty"`
}

func prepareConfigTransaction() (*types.TransactionEnvelope, error) {
	certs, err := config.Certs()
	if err != nil {
		return nil, err
	}

	clusterConfig := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				ID:          config.NodeIdentity().ID,
				Certificate: certs.Node,
				Address:     config.NodeNetwork().Address,
				Port:        config.NodeNetwork().Port,
			},
		},
		Admins: []*types.Admin{
			{
				ID:          config.Admin().ID,
				Certificate: certs.Admin,
			},
		},
		RootCACertificate: certs.RootCA,
	}

	configValue, err := json.Marshal(clusterConfig)
	if err != nil {
		return nil, err
	}

	return &types.TransactionEnvelope{
		Payload: &types.Transaction{
			Type:      types.Transaction_CONFIG,
			DBName:    worldstate.ConfigDBName,
			TxID:      []byte(uuid.New().String()), // TODO: we need to change TxID to string
			DataModel: types.Transaction_KV,
			Writes: []*types.KVWrite{
				{
					Key:   "config", // TODO: need to define a constant and put in library package
					Value: configValue,
				},
			},
		},
		// TODO: we can make the node itself sign the transaction
	}, nil
}
