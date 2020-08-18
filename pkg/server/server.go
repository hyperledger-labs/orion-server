package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/config"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
	"github.ibm.com/blockchaindb/server/pkg/worldstate/leveldb"
)

// DBAndHTTPServer holds the database and http server objects
type DBAndHTTPServer struct {
	dbServ  *dbServer
	handler http.Handler
	listen  net.Listener
	conf    *config.Configurations
}

// New creates a object of DBAndHTTPServer
func New(conf *config.Configurations) (*DBAndHTTPServer, error) {
	dbServ, err := newDBServer(conf)
	if err != nil {
		return nil, errors.Wrap(err, "error while creating the database object")
	}

	router := mux.NewRouter()
	router.HandleFunc("/db/{dbname}/state/{key}", dbServ.handleDataQuery).Methods(http.MethodGet)
	router.HandleFunc("/db/{dbname}", dbServ.handleStatusQuery).Methods(http.MethodGet)
	router.HandleFunc("/tx", dbServ.handleTransaction).Methods(http.MethodPost)

	netConf := conf.Node.Network
	addr := fmt.Sprintf("%s:%d", netConf.Address, netConf.Port)
	listen, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "error while creating a tcp listener")
	}

	return &DBAndHTTPServer{
		dbServ:  dbServ,
		handler: router,
		listen:  listen,
		conf:    conf,
	}, nil
}

// Start starts the server
func (s *DBAndHTTPServer) Start() error {
	// TODO: query block store to check whether the chain is empty. If it empty,
	// submit a config transaction. We also need to check whether the node is a
	// master or slave
	if err := s.dbServ.prepareAndCommitConfigTx(s.conf); err != nil {
		return errors.Wrap(err, "error while preparing and committing config transaction")
	}

	log.Printf("Starting the server on %s", s.listen.Addr().String())

	go func() {
		if err := http.Serve(s.listen, s.handler); err != nil {
			switch err.(type) {
			case *net.OpError:
				log.Printf("network connection is closed")
			default:
				log.Fatalf("server stopped unexpectedly, %v", err)
			}
		}
	}()

	return nil
}

// Stop stops the server
func (s *DBAndHTTPServer) Stop() error {
	if s == nil || s.listen == nil {
		return nil
	}

	log.Printf("Stopping the server listening on %s\n", s.listen.Addr().String())
	return s.listen.Close()
}

type dbServer struct {
	*queryProcessor
	*transactionProcessor
}

func newDBServer(conf *config.Configurations) (*dbServer, error) {
	var levelDB *leveldb.LevelDB
	var err error

	dbConf := conf.Node.Database
	switch dbConf.Name {
	case "leveldb":
		if levelDB, err = leveldb.New(dbConf.LedgerDirectory); err != nil {
			return nil, errors.WithMessagef(err, "failed to create a new leveldb instance for the peer")
		}
	default:
		return nil, errors.New("only leveldb is supported as the state database")
	}

	txProcConf := &txProcessorConfig{
		db:                 levelDB,
		txQueueLength:      conf.Node.QueueLength.Transaction,
		txBatchQueueLength: conf.Node.QueueLength.ReorderedTransactionBatch,
		blockQueueLength:   conf.Node.QueueLength.Block,
		MaxTxCountPerBatch: conf.Consensus.MaxTransactionCountPerBlock,
		batchTimeout:       conf.Consensus.BlockTimeout,
	}
	return &dbServer{
		newQueryProcessor(levelDB, &conf.Node),
		newTransactionProcessor(txProcConf),
	}, nil
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

	//TODO: verify signature

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

	//TODO: verify signature

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

	// TODO: verify signature

	err = db.SubmitTransaction(context.Background(), tx)
	if err != nil {
		composeResponse(w, http.StatusInternalServerError, &ResponseErr{Error: err.Error()})
		return
	}
	composeResponse(w, http.StatusOK, empty.Empty{})
}

func (db *dbServer) prepareAndCommitConfigTx(conf *config.Configurations) error {
	configTx, err := prepareConfigTx(conf)
	if err != nil {
		return errors.Wrap(err, "failed to prepare and commit a configuration transaction")
	}

	if err := db.SubmitTransaction(context.Background(), configTx); err != nil {
		return errors.Wrap(err, "error while committing configuration transaction")
	}
	return nil
}

func validateAndParseHeader(h *http.Header) (string, []byte, error) {
	userID := h.Get(constants.UserHeader)
	if userID == "" {
		return "", nil, errors.New(constants.UserHeader + " is not set in the http request header")
	}

	signature := h.Get(constants.SignatureHeader)
	if signature == "" {
		return "", nil, errors.New(constants.SignatureHeader + " is not set in the http request header")
	}
	signatureBytes, err := base64.StdEncoding.DecodeString(signature)
	if err != nil {
		return "", nil, errors.New(constants.SignatureHeader + " is not encoded correctly")
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

func prepareConfigTx(conf *config.Configurations) (*types.TransactionEnvelope, error) {
	nodeCert, err := ioutil.ReadFile(conf.Node.Identity.CertificatePath)
	if err != nil {
		return nil, errors.Wrapf(err, "error while reading node certificate %s", conf.Node.Identity.CertificatePath)
	}

	adminCert, err := ioutil.ReadFile(conf.Admin.CertificatePath)
	if err != nil {
		return nil, errors.Wrapf(err, "error while reading admin certificate %s", conf.Admin.CertificatePath)
	}

	rootCACert, err := ioutil.ReadFile(conf.RootCA.CertificatePath)
	if err != nil {
		return nil, errors.Wrapf(err, "error while reading rootCA certificate %s", conf.RootCA.CertificatePath)
	}

	clusterConfig := &types.ClusterConfig{
		Nodes: []*types.NodeConfig{
			{
				ID:          conf.Node.Identity.ID,
				Certificate: nodeCert,
				Address:     conf.Node.Network.Address,
				Port:        conf.Node.Network.Port,
			},
		},
		Admins: []*types.Admin{
			{
				ID:          conf.Admin.ID,
				Certificate: adminCert,
			},
		},
		RootCACertificate: rootCACert,
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
