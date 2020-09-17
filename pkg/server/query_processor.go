package server

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/library/pkg/logger"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/blockstore"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/identity"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type queryProcessor struct {
	nodeID          []byte
	db              worldstate.DB
	blockStore      *blockstore.Store
	identityQuerier *identity.Querier
}

type queryProcessorConfig struct {
	nodeID     []byte
	db         worldstate.DB
	blockStore *blockstore.Store
	logger     *logger.SugarLogger
}

func newQueryProcessor(conf *queryProcessorConfig) *queryProcessor {
	return &queryProcessor{
		nodeID:          conf.nodeID,
		db:              conf.db,
		blockStore:      conf.blockStore,
		identityQuerier: identity.NewQuerier(conf.db),
	}
}

// getStatus returns the status about a database, i.e., whether a database exist or not
func (q *queryProcessor) getStatus(_ context.Context, req *types.GetStatusQueryEnvelope) (*types.GetStatusResponseEnvelope, error) {
	var err error
	if err = validateGetStatusQuery(req); err != nil {
		return nil, err
	}

	// ACL is meaningless here as this call is to check whether a DB exist. Even with ACL,
	// the user can infer the information.

	status := &types.GetStatusResponseEnvelope{
		Payload: &types.GetStatusResponse{
			Header: &types.ResponseHeader{
				NodeID: q.nodeID,
			},
			Exist: q.db.Exist(req.Payload.DBName),
		},
		Signature: nil,
	}

	if status.Signature, err = crypto.Sign(status.Payload); err != nil {
		return nil, err
	}
	return status, nil
}

// getState return the state associated with a given key
func (q *queryProcessor) getState(_ context.Context, req *types.GetStateQueryEnvelope) (*types.GetStateResponseEnvelope, error) {
	var err error
	if err = validateGetStateQuery(req); err != nil {
		return nil, err
	}

	r := req.Payload

	hasPerm, err := q.identityQuerier.HasReadAccess(r.UserID, r.DBName)
	if err != nil {
		return nil, err
	}
	if !hasPerm {
		return nil, errors.Errorf("the user [%s] has no permission to read from database [%s]", r.UserID, r.DBName)
	}

	value, metadata, err := q.db.Get(r.DBName, r.Key)
	if err != nil {
		return nil, err
	}

	acl := metadata.GetAccessControl()
	if acl != nil {
		if !acl.ReadUsers[r.UserID] && !acl.ReadWriteUsers[r.UserID] {
			return nil, errors.Errorf("the user [%s] has no permission to read key [%s] from database [%s]", r.UserID, r.Key, r.DBName)
		}
	}

	s := &types.GetStateResponseEnvelope{
		Payload: &types.GetStateResponse{
			Header: &types.ResponseHeader{
				NodeID: q.nodeID,
			},
			Value:    value,
			Metadata: metadata,
		},
		Signature: nil,
	}

	if s.Signature, err = crypto.Sign(s.Payload); err != nil {
		return nil, err
	}

	return s, nil
}

func (q *queryProcessor) close() error {
	if err := q.db.Close(); err != nil {
		return err
	}

	return q.blockStore.Close()
}

func validateGetStatusQuery(req *types.GetStatusQueryEnvelope) error {
	switch {
	case req == nil:
		return fmt.Errorf("`GetStatusQueryEnvelope` is nil, %v", req)
	case req.Payload == nil:
		return fmt.Errorf("`Payload` in `GetStatusQueryEnvelope` is nil, %v", req)
	case req.Payload.UserID == "":
		return fmt.Errorf("`UserID` is not set in `Payload`, %v", req.Payload)
	}

	queryBytes, err := json.Marshal(req.Payload)
	if err != nil {
		return errors.Wrapf(err, "error while encoding `Payload` in the `GetStatusQueryEnvelope`, %v", req)
	}

	if err := crypto.Validate(req.Payload.UserID, req.Signature, queryBytes); err != nil {
		return errors.Wrap(err, "signature validation failed")
	}
	return nil
}

func validateGetStateQuery(req *types.GetStateQueryEnvelope) error {
	switch {
	case req == nil:
		return fmt.Errorf("`GetStateQueryEnvelope` is nil")
	case req.Payload == nil:
		return fmt.Errorf("`Payload` in `GetStateQueryEnvelope` is nil, %v", req)
	case req.Payload.UserID == "":
		return fmt.Errorf("`UserID` is not set in `Payload`, %v", req.Payload)
	}

	queryBytes, err := json.Marshal(req.Payload)
	if err != nil {
		return errors.Wrapf(err, "error while encoding `Payload` in the `GetStateQueryEnvelope`, %v", req)
	}

	if err := crypto.Validate(req.Payload.UserID, req.Signature, queryBytes); err != nil {
		return errors.Wrap(err, "signature validation failed")
	}
	return nil
}
