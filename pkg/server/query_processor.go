package server

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/protos/types"
	"github.ibm.com/blockchaindb/server/pkg/crypto"
	"github.ibm.com/blockchaindb/server/pkg/worldstate"
)

type queryProcessor struct {
	db worldstate.DB
}

func newQueryProcessor(db worldstate.DB) *queryProcessor {
	return &queryProcessor{
		db: db,
	}
}

// GetStatus returns the status about a database, i.e., whether a database exist or not
func (q *queryProcessor) GetStatus(_ context.Context, req *types.GetStatusQueryEnvelope) (*types.GetStatusResponseEnvelope, error) {
	var err error
	if err = validateGetStatusQuery(req); err != nil {
		return nil, err
	}

	status := &types.GetStatusResponseEnvelope{
		Payload: &types.GetStatusResponse{
			Header: &types.ResponseHeader{
				NodeID: nil,
			},
			Exist: false,
		},
		Signature: nil,
	}

	if err = q.db.Open(req.Payload.DBName); err == nil {
		status.Payload.Exist = true
	}

	if status.Signature, err = crypto.Sign(status.Payload); err != nil {
		return nil, err
	}
	return status, nil
}

// GetState return the state associated with a given key
func (q *queryProcessor) GetState(_ context.Context, req *types.GetStateQueryEnvelope) (*types.GetStateResponseEnvelope, error) {
	var err error
	if err = validateGetStateQuery(req); err != nil {
		return nil, err
	}

	value, metadata, err := q.db.Get(req.Payload.DBName, req.Payload.Key)
	if err != nil {
		return nil, err
	}

	s := &types.GetStateResponseEnvelope{
		Payload: &types.GetStateResponse{
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
