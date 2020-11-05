package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.ibm.com/blockchaindb/library/pkg/constants"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/server/backend"
)

type usersRequestHandler struct {
	db        backend.DB
	router    *mux.Router
	txHandler *txHandler
	logger    *logger.SugarLogger
}

func (u *usersRequestHandler) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	u.router.ServeHTTP(responseWriter, request)
}

func (u *usersRequestHandler) getUser(response http.ResponseWriter, request *http.Request) {
	querierUserID, _, composedErr := ExtractCreds(u.db, response, request)
	if composedErr {
		return
	}

	params := mux.Vars(request)
	targetUserID, ok := params["userid"]
	if !ok {
		SendHTTPResponse(response, http.StatusBadRequest, &ResponseErr{"query error - bad or missing userid"})
		return
	}

	//TODO: verify signature
	user, err := u.db.GetUser(querierUserID, targetUserID)
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
			&ResponseErr{"error while processing [" + request.URL.String() + "] because " + err.Error()},
		)
		u.logger.Errorf("failed to process request, due to %s", err.Error())
		return
	}

	SendHTTPResponse(response, http.StatusOK, user)
}

func (u *usersRequestHandler) userTransaction(response http.ResponseWriter, request *http.Request) {
	d := json.NewDecoder(request.Body)
	d.DisallowUnknownFields()

	tx := &types.UserAdministrationTxEnvelope{}
	if err := d.Decode(tx); err != nil {
		SendHTTPResponse(response, http.StatusBadRequest, &ResponseErr{err.Error()})
		return
	}

	// TODO: verify signature
	u.txHandler.HandleTransaction(response, tx.GetPayload().GetUserID(), tx)
}

// NewUsersRequestHandler creates users request handler
func NewUsersRequestHandler(db backend.DB, logger *logger.SugarLogger) *usersRequestHandler {
	handler := &usersRequestHandler{
		db:     db,
		router: mux.NewRouter(),
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
