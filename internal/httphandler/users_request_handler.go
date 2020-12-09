package httphandler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/server/internal/bcdb"
	"github.ibm.com/blockchaindb/server/pkg/constants"
	"github.ibm.com/blockchaindb/server/pkg/cryptoservice"
	"github.ibm.com/blockchaindb/server/pkg/logger"
	"github.ibm.com/blockchaindb/server/pkg/types"
)

// usersRequestHandler handles query and transaction associated
// the user administration
type usersRequestHandler struct {
	db          bcdb.DB
	sigVerifier *cryptoservice.SignatureVerifier
	router      *mux.Router
	txHandler   *txHandler
	logger      *logger.SugarLogger
}

// NewUsersRequestHandler creates users request handler
func NewUsersRequestHandler(db bcdb.DB, logger *logger.SugarLogger) http.Handler {
	handler := &usersRequestHandler{
		db:          db,
		sigVerifier: cryptoservice.NewVerifier(db),
		router:      mux.NewRouter(),
		txHandler: &txHandler{
			db: db,
		},
		logger: logger,
	}

	// HTTP GET "/user/{userid}" get user record with given userID
	handler.router.HandleFunc(constants.GetUser, handler.getUser).Methods(http.MethodGet)
	// HTTP POST "user/tx" submit user creation transaction
	handler.router.HandleFunc(constants.PostUserTx, handler.userTransaction).Methods(http.MethodPost)

	return handler
}

func (u *usersRequestHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	u.router.ServeHTTP(responseWriter, request)
}

func (u *usersRequestHandler) getUser(response http.ResponseWriter, request *http.Request) {
	payload, respondedErr := extractVerifiedQueryPayload(response, request, constants.GetUser, u.sigVerifier)
	if respondedErr {
		return
	}
	query := payload.(*types.GetUserQuery)

	user, err := u.db.GetUser(query.UserID, query.TargetUserID)
	if err != nil {
		var status int

		switch err.(type) {
		case *bcdb.PermissionErr:
			status = http.StatusForbidden
		default:
			status = http.StatusInternalServerError
		}

		SendHTTPResponse(
			response,
			status,
			&ResponseErr{"error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error()},
		)
		u.logger.Errorf("failed to process request, due to %s", err.Error())
		return
	}

	SendHTTPResponse(response, http.StatusOK, user)
}

func (u *usersRequestHandler) userTransaction(response http.ResponseWriter, request *http.Request) {
	d := json.NewDecoder(request.Body)
	d.DisallowUnknownFields()

	txEnv := &types.UserAdministrationTxEnvelope{}
	if err := d.Decode(txEnv); err != nil {
		u.logger.Errorf(err.Error())
		SendHTTPResponse(response, http.StatusBadRequest, &ResponseErr{err.Error()})
		return
	}

	if txEnv.Payload == nil {
		u.logger.Errorf(fmt.Sprintf("missing transaction envelope payload (%T)", txEnv.Payload))
		SendHTTPResponse(response, http.StatusBadRequest,
			&ResponseErr{fmt.Sprintf("missing transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if txEnv.Payload.UserID == "" {
		u.logger.Errorf(fmt.Sprintf("missing UserID in transaction envelope payload (%T)", txEnv.Payload))
		SendHTTPResponse(response, http.StatusBadRequest,
			&ResponseErr{fmt.Sprintf("missing UserID in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if len(txEnv.Signature) == 0 {
		u.logger.Errorf(fmt.Sprintf("missing Signature in transaction envelope payload (%T)", txEnv.Payload))
		SendHTTPResponse(response, http.StatusBadRequest,
			&ResponseErr{fmt.Sprintf("missing Signature in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if err, code := VerifyRequestSignature(u.sigVerifier, txEnv.Payload.UserID, txEnv.Signature, txEnv.Payload); err != nil {
		SendHTTPResponse(response, code, &ResponseErr{err.Error()})
		return
	}

	u.txHandler.handleTransaction(response, txEnv)
}
