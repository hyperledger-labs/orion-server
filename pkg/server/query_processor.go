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

func (qp *queryProcessor) GetStatus(ctx context.Context, req *types.GetStatusQueryEnvelope) (*types.GetStatusResponseEnvelope, error) {
	if err := validateDB(req); err != nil {
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
	var err error

	if err = qp.db.Open(req.Payload.DBName); err == nil {
		status.Payload.Exist = true
	}
	if status.Signature, err = crypto.Sign(status.Payload); err != nil {
		return nil, err
	}
	return status, nil
}

func (qp *queryProcessor) GetState(ctx context.Context, req *types.GetStateQueryEnvelope) (*types.GetStateResponseEnvelope, error) {
	if err := validateDataQuery(req); err != nil {
		return nil, err
	}

	result := &types.GetStateResponseEnvelope{
		Payload: &types.GetStateResponse{
			Header: &types.ResponseHeader{
				NodeID: nil,
			},
		},
		Signature: nil,
	}

	dbVal, err := qp.db.Get(req.Payload.DBName, req.Payload.Key)
	if err != nil {
		return nil, err
	}
	if dbVal != nil {
		result.Payload.Value = dbVal
	}
	if result.Signature, err = crypto.Sign(result.Payload); err != nil {
		return nil, err
	}
	return result, nil
}

func validateDataQuery(req *types.GetStateQueryEnvelope) error {
	if req == nil {
		return fmt.Errorf("dataQueryEnvelope request is nil")
	}
	if req.Payload == nil {
		return fmt.Errorf("DataQuery is nil [%v]", req)
	}
	if req.Payload.UserID == "" {
		return fmt.Errorf("DataQuery userid is empty [%v]", req)
	}
	queryBytes, err := json.Marshal(req.Payload)
	if err != nil {
		return errors.Wrapf(err, "error while encoding db query %v", req.Payload)
	}
	if err := crypto.Validate(req.Payload.UserID, req.Signature, queryBytes); err != nil {
		return errors.Wrap(err, "query error - wrong signature")
	}
	return nil
}

func validateDB(req *types.GetStatusQueryEnvelope) error {
	if req == nil {
		return fmt.Errorf("db request envelope is nil")
	}
	if req.Payload == nil {
		return fmt.Errorf("db query is nil %v", req)
	}
	if req.Payload.UserID == "" {
		return fmt.Errorf("db query userid is empty %v", req)
	}
	queryBytes, err := json.Marshal(req.Payload)
	if err != nil {
		return errors.Wrapf(err, "error while encoding db query %v", req.Payload)
	}
	if err := crypto.Validate(req.Payload.UserID, req.Signature, queryBytes); err != nil {
		return errors.Wrap(err, "query error - wrong signature")
	}
	return nil
}
