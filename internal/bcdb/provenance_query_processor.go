// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package bcdb

import (
	"github.com/hyperledger-labs/orion-server/internal/provenance"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type provenanceQueryProcessor struct {
	provenanceStore *provenance.Store
	logger          *logger.SugarLogger
}

type provenanceQueryProcessorConfig struct {
	provenanceStore *provenance.Store
	logger          *logger.SugarLogger
}

func newProvenanceQueryProcessor(conf *provenanceQueryProcessorConfig) *provenanceQueryProcessor {
	return &provenanceQueryProcessor{
		provenanceStore: conf.provenanceStore,
		logger:          conf.logger,
	}
}

// GetValues returns all values associated with a given key
func (p *provenanceQueryProcessor) GetValues(dbName, key string) (*types.GetHistoricalDataResponse, error) {
	values, err := p.provenanceStore.GetValues(dbName, key)
	if err != nil {
		return nil, err
	}

	return p.composeHistoricalDataResponse(values)
}

// GetValueAt returns the value of a given key at a particular version
func (p *provenanceQueryProcessor) GetValueAt(dbName, key string, version *types.Version) (*types.GetHistoricalDataResponse, error) {
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
func (p *provenanceQueryProcessor) GetMostRecentValueAtOrBelow(dbName, key string, version *types.Version) (*types.GetHistoricalDataResponse, error) {
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
func (p *provenanceQueryProcessor) GetPreviousValues(dbName, key string, version *types.Version) (*types.GetHistoricalDataResponse, error) {
	values, err := p.provenanceStore.GetPreviousValues(dbName, key, version, -1)
	if err != nil {
		return nil, err
	}

	return p.composeHistoricalDataResponse(values)
}

// GetNextValues returns next values of a given key and a version. The number of records returned would be limited
// by the limit parameters.
func (p *provenanceQueryProcessor) GetNextValues(dbName, key string, version *types.Version) (*types.GetHistoricalDataResponse, error) {
	values, err := p.provenanceStore.GetNextValues(dbName, key, version, -1)
	if err != nil {
		return nil, err
	}

	return p.composeHistoricalDataResponse(values)
}

func (p *provenanceQueryProcessor) GetDeletedValues(dbName, key string) (*types.GetHistoricalDataResponse, error) {
	values, err := p.provenanceStore.GetDeletedValues(dbName, key)
	if err != nil {
		return nil, err
	}

	return p.composeHistoricalDataResponse(values)
}

// GetValuesReadByUser returns all values read by a given user
func (p *provenanceQueryProcessor) GetValuesReadByUser(userID string) (*types.GetDataProvenanceResponse, error) {
	kvs, err := p.provenanceStore.GetValuesReadByUser(userID)
	if err != nil {
		return nil, err
	}

	return &types.GetDataProvenanceResponse{
		DBKeyValues: kvs,
	}, nil
}

// GetValuesReadByUser returns all values read by a given user
func (p *provenanceQueryProcessor) GetValuesWrittenByUser(userID string) (*types.GetDataProvenanceResponse, error) {
	kvs, err := p.provenanceStore.GetValuesWrittenByUser(userID)
	if err != nil {
		return nil, err
	}

	return &types.GetDataProvenanceResponse{
		DBKeyValues: kvs,
	}, nil
}

func (p *provenanceQueryProcessor) GetValuesDeletedByUser(userID string) (*types.GetDataProvenanceResponse, error) {
	kvs, err := p.provenanceStore.GetValuesDeletedByUser(userID)
	if err != nil {
		return nil, err
	}

	return &types.GetDataProvenanceResponse{
		DBKeyValues: kvs,
	}, nil
}

// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
func (p *provenanceQueryProcessor) GetReaders(dbName, key string) (*types.GetDataReadersResponse, error) {
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
func (p *provenanceQueryProcessor) GetWriters(dbName, key string) (*types.GetDataWritersResponse, error) {
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
func (p *provenanceQueryProcessor) GetTxIDsSubmittedByUser(userID string) (*types.GetTxIDsSubmittedByResponse, error) {
	txIDs, err := p.provenanceStore.GetTxIDsSubmittedByUser(userID)
	if err != nil {
		return nil, err
	}

	return &types.GetTxIDsSubmittedByResponse{
		TxIDs: txIDs,
	}, nil
}

func (p *provenanceQueryProcessor) composeHistoricalDataResponse(values []*types.ValueWithMetadata) (*types.GetHistoricalDataResponse, error) {
	return &types.GetHistoricalDataResponse{
		Values: values,
	}, nil
}
