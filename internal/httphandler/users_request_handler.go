// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package httphandler

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/hyperledger-labs/orion-server/internal/bcdb"
	"github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/utils"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/cryptoservice"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"google.golang.org/protobuf/encoding/protojson"
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
		sigVerifier: cryptoservice.NewVerifier(db, logger),
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

	user, err := u.db.GetUser(query.UserId, query.TargetUserId)
	if err != nil {
		var status int

		switch err.(type) {
		case *errors.PermissionErr:
			status = http.StatusForbidden
		default:
			status = http.StatusInternalServerError
		}

		utils.SendHTTPResponse(
			response,
			status,
			&types.HttpResponseErr{"error while processing '" + request.Method + " " + request.URL.String() + "' because " + err.Error()},
		)
		u.logger.Errorf("failed to process request, due to %s", err.Error())
		return
	}

	utils.SendHTTPResponse(response, http.StatusOK, user)
}

func (u *usersRequestHandler) userTransaction(response http.ResponseWriter, request *http.Request) {
	timeout, err := validateAndParseTxPostHeader(&request.Header)
	if err != nil {
		utils.SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	requestBytes, err := ioutil.ReadAll(request.Body)
	if err != nil {
		utils.SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	txEnv := &types.UserAdministrationTxEnvelope{}
	if err := protojson.Unmarshal(requestBytes, txEnv); err != nil {
		utils.SendHTTPResponse(response, http.StatusBadRequest, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	if txEnv.Payload == nil {
		u.logger.Errorf(fmt.Sprintf("missing transaction envelope payload (%T)", txEnv.Payload))
		utils.SendHTTPResponse(response, http.StatusBadRequest,
			&types.HttpResponseErr{ErrMsg: fmt.Sprintf("missing transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if txEnv.Payload.UserId == "" {
		u.logger.Errorf(fmt.Sprintf("missing UserID in transaction envelope payload (%T)", txEnv.Payload))
		utils.SendHTTPResponse(response, http.StatusBadRequest,
			&types.HttpResponseErr{ErrMsg: fmt.Sprintf("missing UserID in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if len(txEnv.Signature) == 0 {
		u.logger.Errorf(fmt.Sprintf("missing Signature in transaction envelope payload (%T)", txEnv.Payload))
		utils.SendHTTPResponse(response, http.StatusBadRequest,
			&types.HttpResponseErr{ErrMsg: fmt.Sprintf("missing Signature in transaction envelope payload (%T)", txEnv.Payload)})
		return
	}

	if err, code := VerifyRequestSignature(u.sigVerifier, txEnv.Payload.UserId, txEnv.Signature, txEnv.Payload); err != nil {
		utils.SendHTTPResponse(response, code, &types.HttpResponseErr{ErrMsg: err.Error()})
		return
	}

	u.txHandler.handleTransaction(response, request, txEnv, timeout)
}
