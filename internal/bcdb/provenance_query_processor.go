// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package bcdb

import (
	ierrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"github.com/hyperledger-labs/orion-server/internal/identity"
	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type provenanceQueryProcessor struct {
	provenanceStore *provenance.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

type provenanceQueryProcessorConfig struct {
	provenanceStore *provenance.Store
	identityQuerier *identity.Querier
	logger          *logger.SugarLogger
}

func newProvenanceQueryProcessor(conf *provenanceQueryProcessorConfig) *provenanceQueryProcessor {
	return &provenanceQueryProcessor{
		provenanceStore: conf.provenanceStore,
		identityQuerier: conf.identityQuerier,
		logger:          conf.logger,
	}
}

// GetValues returns all values associated with a given key
func (p *provenanceQueryProcessor) GetValues(userID, dbName, key string) (*types.GetHistoricalDataResponse, error) {
	if p.provenanceStore == nil {
		return nil, &ierrors.ServerRestrictionError{ErrMsg: "provenance store is disabled on this server"}
	}

	if err := p.aclCheckForData(userID); err != nil {
		return nil, err
	}

	values, err := p.provenanceStore.GetValues(dbName, key)
	if err != nil {
		return nil, err
	}

	return p.composeHistoricalDataResponse(values)
}

// GetValueAt returns the value of a given key at a particular version
func (p *provenanceQueryProcessor) GetValueAt(userID, dbName, key string, version *types.Version) (*types.GetHistoricalDataResponse, error) {
	if p.provenanceStore == nil {
		return nil, &ierrors.ServerRestrictionError{ErrMsg: "provenance store is disabled on this server"}
	}

	if err := p.aclCheckForData(userID); err != nil {
		return nil, err
	}

	value, err := p.provenanceStore.GetValueAt(dbName, key, version)
	if err != nil {
		return nil, err
	}

	if value == nil {
		return p.composeHistoricalDataResponse(nil)
	}

	return p.composeHistoricalDataResponse([]*types.ValueWithMetadata{value})
}

// GetMostRecentValueAtOrBelow returns the most recent value of a given key at or below the given version
func (p *provenanceQueryProcessor) GetMostRecentValueAtOrBelow(userID, dbName, key string, version *types.Version) (*types.GetHistoricalDataResponse, error) {
	if p.provenanceStore == nil {
		return nil, &ierrors.ServerRestrictionError{ErrMsg: "provenance store is disabled on this server"}
	}

	if err := p.aclCheckForData(userID); err != nil {
		return nil, err
	}

	value, err := p.provenanceStore.GetMostRecentValueAtOrBelow(dbName, key, version)
	if err != nil {
		return nil, err
	}

	if value == nil {
		return p.composeHistoricalDataResponse(nil)
	}

	return p.composeHistoricalDataResponse([]*types.ValueWithMetadata{value})
}

// GetPreviousValues returns previous values of a given key and a version. The number of records returned would be limited
// by the limit parameters.
func (p *provenanceQueryProcessor) GetPreviousValues(userID, dbName, key string, version *types.Version) (*types.GetHistoricalDataResponse, error) {
	if p.provenanceStore == nil {
		return nil, &ierrors.ServerRestrictionError{ErrMsg: "provenance store is disabled on this server"}
	}

	if err := p.aclCheckForData(userID); err != nil {
		return nil, err
	}

	values, err := p.provenanceStore.GetPreviousValues(dbName, key, version, -1)
	if err != nil {
		return nil, err
	}

	return p.composeHistoricalDataResponse(values)
}

// GetNextValues returns next values of a given key and a version. The number of records returned would be limited
// by the limit parameters.
func (p *provenanceQueryProcessor) GetNextValues(userID, dbName, key string, version *types.Version) (*types.GetHistoricalDataResponse, error) {
	if p.provenanceStore == nil {
		return nil, &ierrors.ServerRestrictionError{ErrMsg: "provenance store is disabled on this server"}
	}

	if err := p.aclCheckForData(userID); err != nil {
		return nil, err
	}

	values, err := p.provenanceStore.GetNextValues(dbName, key, version, -1)
	if err != nil {
		return nil, err
	}

	return p.composeHistoricalDataResponse(values)
}

func (p *provenanceQueryProcessor) GetDeletedValues(userID, dbName, key string) (*types.GetHistoricalDataResponse, error) {
	if p.provenanceStore == nil {
		return nil, &ierrors.ServerRestrictionError{ErrMsg: "provenance store is disabled on this server"}
	}

	if err := p.aclCheckForData(userID); err != nil {
		return nil, err
	}

	values, err := p.provenanceStore.GetDeletedValues(dbName, key)
	if err != nil {
		return nil, err
	}

	return p.composeHistoricalDataResponse(values)
}

// GetValuesReadByUser returns all values read by a given user
func (p *provenanceQueryProcessor) GetValuesReadByUser(querierUserID, targetUserID string) (*types.GetDataProvenanceResponse, error) {
	if p.provenanceStore == nil {
		return nil, &ierrors.ServerRestrictionError{ErrMsg: "provenance store is disabled on this server"}
	}

	if err := p.aclCheckForUserOperation(querierUserID, targetUserID); err != nil {
		return nil, err
	}

	kvs, err := p.provenanceStore.GetValuesReadByUser(targetUserID)
	if err != nil {
		return nil, err
	}

	return &types.GetDataProvenanceResponse{
		DBKeyValues: kvs,
	}, nil
}

// GetValuesReadByUser returns all values read by a given user
func (p *provenanceQueryProcessor) GetValuesWrittenByUser(querierUserID, targetUserID string) (*types.GetDataProvenanceResponse, error) {
	if p.provenanceStore == nil {
		return nil, &ierrors.ServerRestrictionError{ErrMsg: "provenance store is disabled on this server"}
	}

	if err := p.aclCheckForUserOperation(querierUserID, targetUserID); err != nil {
		return nil, err
	}

	kvs, err := p.provenanceStore.GetValuesWrittenByUser(targetUserID)
	if err != nil {
		return nil, err
	}

	return &types.GetDataProvenanceResponse{
		DBKeyValues: kvs,
	}, nil
}

func (p *provenanceQueryProcessor) GetValuesDeletedByUser(querierUserID, targetUserID string) (*types.GetDataProvenanceResponse, error) {
	if p.provenanceStore == nil {
		return nil, &ierrors.ServerRestrictionError{ErrMsg: "provenance store is disabled on this server"}
	}

	if err := p.aclCheckForUserOperation(querierUserID, targetUserID); err != nil {
		return nil, err
	}

	kvs, err := p.provenanceStore.GetValuesDeletedByUser(targetUserID)
	if err != nil {
		return nil, err
	}

	return &types.GetDataProvenanceResponse{
		DBKeyValues: kvs,
	}, nil
}

// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
func (p *provenanceQueryProcessor) GetReaders(userID, dbName, key string) (*types.GetDataReadersResponse, error) {
	if p.provenanceStore == nil {
		return nil, &ierrors.ServerRestrictionError{ErrMsg: "provenance store is disabled on this server"}
	}

	if err := p.aclCheckForData(userID); err != nil {
		return nil, err
	}

	users, err := p.provenanceStore.GetReaders(dbName, key)
	if err != nil {
		return nil, err
	}

	if len(users) == 0 {
		users = nil
	}

	return &types.GetDataReadersResponse{
		ReadBy: users,
	}, nil
}

// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
func (p *provenanceQueryProcessor) GetWriters(userID, dbName, key string) (*types.GetDataWritersResponse, error) {
	if p.provenanceStore == nil {
		return nil, &ierrors.ServerRestrictionError{ErrMsg: "provenance store is disabled on this server"}
	}

	if err := p.aclCheckForData(userID); err != nil {
		return nil, err
	}

	users, err := p.provenanceStore.GetWriters(dbName, key)
	if err != nil {
		return nil, err
	}

	if len(users) == 0 {
		users = nil
	}

	return &types.GetDataWritersResponse{
		WrittenBy: users,
	}, nil
}

// GetTxIDsSubmittedByUser returns all ids of all transactions submitted by a given user
func (p *provenanceQueryProcessor) GetTxIDsSubmittedByUser(querierUserID, targetUserID string) (*types.GetTxIDsSubmittedByResponse, error) {
	if p.provenanceStore == nil {
		return nil, &ierrors.ServerRestrictionError{ErrMsg: "provenance store is disabled on this server"}
	}

	if err := p.aclCheckForUserOperation(querierUserID, targetUserID); err != nil {
		return nil, err
	}

	txIDs, err := p.provenanceStore.GetTxIDsSubmittedByUser(targetUserID)
	if err != nil {
		return nil, err
	}

	return &types.GetTxIDsSubmittedByResponse{
		TxIDs: txIDs,
	}, nil
}

func (p *provenanceQueryProcessor) aclCheckForUserOperation(querierUserID, targetUserID string) error {
	isAdmin, err := p.identityQuerier.HasAdministrationPrivilege(querierUserID)
	if err != nil {
		return err
	}

	if !isAdmin && querierUserID != targetUserID {
		return &ierrors.PermissionErr{
			ErrMsg: "The querier [" + querierUserID + "] is neither an admin nor requesting operations performed by [" + querierUserID + "]. Only an admin can query operations performed by other users.",
		}
	}

	return nil
}

func (p *provenanceQueryProcessor) aclCheckForData(querierUserID string) error {
	isAdmin, err := p.identityQuerier.HasAdministrationPrivilege(querierUserID)
	if err != nil {
		return err
	}

	if !isAdmin {
		return &ierrors.PermissionErr{
			ErrMsg: "The querier [" + querierUserID + "] is not an admin. Only an admin can query historical data",
		}
	}

	return nil
}

func (p *provenanceQueryProcessor) composeHistoricalDataResponse(values []*types.ValueWithMetadata) (*types.GetHistoricalDataResponse, error) {
	return &types.GetHistoricalDataResponse{
		Values: values,
	}, nil
}
