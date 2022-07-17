// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package provenance

import (
	"context"
	"encoding/json"
	"fmt"
	interrors "github.com/hyperledger-labs/orion-server/internal/errors"
	"sort"
	"strings"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

const (
	// SUBMITTED edge from userID to txID
	// denotes that the txID was submitted by the userID
	SUBMITTED = "s"
	// INCLUDES edge from blockNum to txID
	// denotes that the block includes the txID
	INCLUDES = "i"
	// WRITES edge from txID to value
	// denotes that the txID written the value
	WRITES = "w"
	// READS edge from txID to value
	// denotes that the txID read the value
	READS = "r"
	// DELETES edge from txID to the value
	// denotes that the txID deleted the value
	// including the key
	DELETES = "d"
	// NEXT edge from one value to another
	// denotes that the next version of the value
	NEXT = "n"
	// PREVIOUS edge from one to another
	// denotes that the previous version of the value
	PREVIOUS = "p"
)

// TxDataForProvenance holds the transaction data that is
// needed for the provenance store
type TxDataForProvenance struct {
	IsValid            bool
	DBName             string
	UserID             string
	TxID               string
	Reads              []*KeyWithVersion
	Writes             []*types.KVWithMetadata
	Deletes            map[string]*types.Version
	OldVersionOfWrites map[string]*types.Version
}

// KeyWithVersion holds a key and a version
type KeyWithVersion struct {
	Key     string
	Version *types.Version
}

// TxIDLocation refers to the location of a TxID
// in the block
type TxIDLocation struct {
	BlockNum uint64 `json:"block_num"`
	TxIndex  int    `json:"tx_index"`
}

// Commit commits the txsData to a graph database. The following relationships are stored
//  1. userID--(submitted)-->txID
//  2. blockNum--(includes)->txID
//  3. txID--(reads)-->value
//  4. txID--(write)-->value
//  5. txID--(delete)-->value
//  6. key--(version)-->value
//  7. value<--(previous)--value
//  8. value--(next)-->value
func (s *Store) Commit(blockNum uint64, txsData []*TxDataForProvenance) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	batch := graph.NewWriter(s.cayleyGraph.QuadWriter)
	for txNum, tx := range txsData {
		loc, err := json.Marshal(&TxIDLocation{blockNum, txNum})
		if err != nil {
			return errors.WithMessage(err, "error while marshaling txID location")
		}
		s.logger.Debugf("loc[%s]]---(includes)--->txID[%s]", loc, tx.TxID)
		batch.WriteQuad(quad.Make(string(loc), INCLUDES, tx.TxID, ""))

		if !tx.IsValid {
			s.logger.Debugf("as txID [%s] is invalid, we created vertex and edge to represent only the relation [location--(includes)-->txID]", tx.TxID)
			// if needed in the future, we can store more details about the invalid transactions.
			continue
		}

		s.logger.Debugf("userID[%s]---(submitted)--->txID[%s]", tx.UserID, tx.TxID)
		batch.WriteQuad(quad.Make(tx.UserID, SUBMITTED, tx.TxID, ""))

		if err := s.addReads(tx, batch); err != nil {
			return err
		}

		if err := s.addWrites(tx, batch); err != nil {
			return err
		}

		if err := s.addDeletes(tx, batch); err != nil {
			return err
		}
	}

	return batch.Close()
}

func (s *Store) addReads(tx *TxDataForProvenance, batch graph.BatchWriter) error {
	for _, read := range tx.Reads {
		value, err := s.getValueVertex(tx.DBName, read.Key, read.Version)
		if err != nil {
			return err
		}

		s.logger.Debugf("txID[%s]---(reads)--->value[%s]", tx.TxID, quad.NativeOf(value))
		batch.WriteQuad(quad.Make(tx.TxID, READS, value, ""))
	}

	return nil
}

func (s *Store) addWrites(tx *TxDataForProvenance, batch graph.BatchWriter) error {
	for _, write := range tx.Writes {
		actualKey := write.Key
		write.Key = constructCompositeKey(tx.DBName, write.Key)
		newValue, err := json.Marshal(write)
		if err != nil {
			return err
		}

		newVersion, err := json.Marshal(write.Metadata.Version)
		if err != nil {
			return err
		}
		s.logger.Debugf("key[%s]---(version[%s])--->value[%s]", write.Key, string(newVersion), string(newValue))
		batch.WriteQuad(quad.Make(write.Key, string(newVersion), string(newValue), ""))

		s.logger.Debugf("txID[%s]---(writes)--->value[%s]", tx.TxID, string(newValue))
		batch.WriteQuad(quad.Make(tx.TxID, WRITES, string(newValue), ""))

		oldVersion, ok := tx.OldVersionOfWrites[actualKey]
		if !ok {
			// old version would not have been passed if it was deleted in the worldstate database already
			// but we can find the old version from the provenance store even if it was deleted already
			s.logger.Debug("fetching last deleted version of key [" + actualKey + "] from db [" + tx.DBName + "]")
			lastVer, err := s.getLastDeletedVersion(tx.DBName, actualKey)
			if err != nil {
				return err
			}
			if lastVer == nil {
				s.logger.Debug("previous version of key [" + actualKey + "] does not exist in db [" + tx.DBName + "]")
				continue
			}

			oldVersion = lastVer
		}

		oldValue, err := s.getValueVertex(tx.DBName, actualKey, oldVersion)
		if err != nil {
			return err
		}

		if oldValue == nil {
			s.logger.Debugf("key [%s] version [%d,%d] for which oldValue is not found", actualKey, oldVersion.BlockNum, oldVersion.TxNum)
			return errors.Errorf("error while finding the previous version of the key[%s]", write.Key)
		}

		s.logger.Debugf("oldValue[%s]<---(previous)---newValue[%s]", quad.NativeOf(oldValue), string(newValue))
		batch.WriteQuad(quad.Make(string(newValue), PREVIOUS, oldValue, ""))

		s.logger.Debugf("oldValue[%s]---(next)--->newValue[%s]", quad.NativeOf(oldValue), string(newValue))
		batch.WriteQuad(quad.Make(oldValue, NEXT, string(newValue), ""))
	}

	return nil
}

func (s *Store) addDeletes(tx *TxDataForProvenance, batch graph.BatchWriter) error {
	for k, v := range tx.Deletes {
		s.logger.Debugf("fetch value of key [%s] at version (%d, %d)", k, v.BlockNum, v.TxNum)
		value, err := s.getValueVertex(tx.DBName, k, v)
		if err != nil {
			return err
		}

		if value == nil {
			// no such value exist and the delete of non-existing value is a non-op
			continue
		}
		s.logger.Debugf("txID[%s]---(deletes)--->value[%s]", tx.TxID, quad.NativeOf(value))
		batch.WriteQuad(quad.Make(tx.TxID, DELETES, value, ""))
	}
	return nil
}

// GetValues returns all values associated with a given key
func (s *Store) GetValues(dbName, key string) ([]*types.ValueWithMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	s.logger.Debugf("fetch all historical values associated with the key [%s] in db [%s]", key, dbName)
	cKey := constructCompositeKey(dbName, key)
	p := cayley.StartPath(s.cayleyGraph, quad.String(cKey)).Out()

	valueVertices, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph)
	if err != nil {
		return nil, err
	}

	return verticesToValues(valueVertices)
}

// GetPreviousValues returns previous values of a given key and a version. The number of records returned would be limited
// by the limit parameters.
func (s *Store) GetPreviousValues(dbName, key string, version *types.Version, limit int) ([]*types.ValueWithMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.getValuesRecursively(dbName, key, version, PREVIOUS, limit)
}

// GetNextValues returns next values of a given key and a version. The number of records returned would be limited
// by the limit parameters.
func (s *Store) GetNextValues(dbName, key string, version *types.Version, limit int) ([]*types.ValueWithMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.getValuesRecursively(dbName, key, version, NEXT, limit)
}

// GetValueAt returns the value of a given key at a particular version
func (s *Store) GetValueAt(dbName, key string, version *types.Version) (*types.ValueWithMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	s.logger.Debugf("fetch value of key [%s] at version (%d, %d)", key, version.BlockNum, version.TxNum)
	valueVertex, err := s.getValueVertex(dbName, key, version)
	if err != nil {
		return nil, err
	}

	if valueVertex == nil {
		return nil, nil
	}

	return vertexToValue(valueVertex)
}

// GetValuesReadByUser returns all values read by a given user
func (s *Store) GetValuesReadByUser(userID string) (map[string]*types.KVsWithMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	txIDs, err := s.GetTxIDsSubmittedByUser(userID)
	if err != nil {
		return nil, err
	}

	return s.outEdgesFrom(txIDs, READS)
}

// GetValuesWrittenByUser returns all values written by a given user
func (s *Store) GetValuesWrittenByUser(userID string) (map[string]*types.KVsWithMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	txIDs, err := s.GetTxIDsSubmittedByUser(userID)
	if err != nil {
		return nil, err
	}

	return s.outEdgesFrom(txIDs, WRITES)
}

// GetValuesDeletedByUser returns all values deleted by a given user
func (s *Store) GetValuesDeletedByUser(userID string) (map[string]*types.KVsWithMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	txIDs, err := s.GetTxIDsSubmittedByUser(userID)
	if err != nil {
		return nil, err
	}

	return s.outEdgesFrom(txIDs, DELETES)
}

// GetDeletedValues returns all deleted values associated with a given key present in the
// given database name
func (s *Store) GetDeletedValues(dbName, key string) ([]*types.ValueWithMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.getDeletedValuesWithoutLock(dbName, key)
}

func (s *Store) getDeletedValuesWithoutLock(dbName, key string) ([]*types.ValueWithMetadata, error) {
	s.logger.Debugf("fetch all historical deleted values associated with the key [%s] in db [%s]", key, dbName)
	cKey := constructCompositeKey(dbName, key)
	p := cayley.StartPath(s.cayleyGraph, quad.String(cKey)).Out().Tag("deleted_value").In(quad.String(DELETES)).Back("deleted_value")
	// p := cayley.StartPath(s.cayleyGraph, quad.String(cKey)).Out().In(quad.String(DELETES))

	valueVertices, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph)
	if err != nil {
		return nil, err
	}

	return verticesToValues(valueVertices)
}

// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
func (s *Store) GetReaders(dbName, key string) (map[string]uint32, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	cKey := constructCompositeKey(dbName, key)
	p := cayley.StartPath(s.cayleyGraph, quad.String(cKey)).Out().In(quad.String(READS)).In(quad.String(SUBMITTED))
	vertices, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph)
	if err != nil {
		return nil, err
	}

	userIDs := make(map[string]uint32)
	for _, qv := range vertices {
		userIDs[quad.ToString(qv)]++
	}

	return userIDs, err
}

// GetWriters returns all userIDs who have modified a given key as well as the modifcation frequency
func (s *Store) GetWriters(dbName, key string) (map[string]uint32, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	cKey := constructCompositeKey(dbName, key)
	p := cayley.StartPath(s.cayleyGraph, quad.String(cKey)).Out().In(quad.String(WRITES)).In(quad.String(SUBMITTED))
	vertices, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph)
	if err != nil {
		return nil, err
	}

	userIDs := make(map[string]uint32)
	for _, qv := range vertices {
		userIDs[quad.ToString(qv)]++
	}

	return userIDs, err
}

// GetTxIDsSubmittedByUser returns all ids of all transactions submitted by a given user
func (s *Store) GetTxIDsSubmittedByUser(userID string) ([]string, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	p := cayley.StartPath(s.cayleyGraph, quad.String(userID)).Out(quad.String(SUBMITTED))

	vertices, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph)
	if err != nil {
		return nil, err
	}

	var txIDs []string
	for _, qv := range vertices {
		txIDs = append(txIDs, quad.ToString(qv))
	}

	return txIDs, err
}

// GetTxIDLocation returns the location, i.e, block number and the tx index, of a given txID
func (s *Store) GetTxIDLocation(txID string) (*TxIDLocation, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	p := cayley.StartPath(s.cayleyGraph, quad.String(txID)).In(quad.String(INCLUDES))

	vertex, err := p.Iterate(context.Background()).FirstValue(s.cayleyGraph)
	if err != nil {
		s.logger.Errorf("cayley iteration error: %s", err)
		return nil, errors.Wrap(err, "cayley iteration")
	}

	if vertex == nil {
		s.logger.Debugf("TxID not found: %s", txID)
		return nil, &interrors.NotFoundErr{Message: fmt.Sprintf("TxID not found: %s", txID)}
	}

	var loc *TxIDLocation
	loc, err = vertexToTxIDLocation(vertex)
	if err != nil {
		s.logger.Errorf("vertex to TxID translation error: %s", err)
		return nil, errors.Wrap(err, "vertex to TxID translation")
	}

	return loc, nil
}

// GetMostRecentValueAtOrBelow returns the most recent value hold by the given key at or below a given version
func (s *Store) GetMostRecentValueAtOrBelow(dbName, key string, version *types.Version) (*types.ValueWithMetadata, error) {
	values, err := s.GetValues(dbName, key)
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return nil, nil
	}

	// sort the values based on the version (descending order)
	sort.Slice(values[:], func(i, j int) bool {
		// if block number is the same, then we need to compare the transaction number. Note that
		// the transaction number cannot be the same (as the transaction can commit only one value per key)
		return (values[i].Metadata.Version.BlockNum > values[j].Metadata.Version.BlockNum) ||
			((values[i].Metadata.Version.BlockNum == values[j].Metadata.Version.BlockNum) &&
				values[i].Metadata.Version.TxNum > values[j].Metadata.Version.TxNum)
	})

	for _, v := range values {
		valVersion := v.Metadata.Version
		if valVersion.BlockNum > version.BlockNum {
			continue
		}

		if ((valVersion.BlockNum == version.BlockNum) && (valVersion.TxNum <= version.TxNum)) ||
			valVersion.BlockNum < version.BlockNum {
			return v, nil
		}
	}

	return nil, nil
}

func (s *Store) getLastDeletedVersion(dbName, key string) (*types.Version, error) {
	valuesWithMetadata, err := s.getDeletedValuesWithoutLock(dbName, key)
	if err != nil {
		return nil, errors.Wrapf(err, "error finding the last deleted version")
	}

	lastVer := &types.Version{}
	for _, val := range valuesWithMetadata {
		if lastVer.BlockNum > val.Metadata.Version.BlockNum {
			continue
		}
		lastVer.BlockNum = val.Metadata.Version.BlockNum
		lastVer.TxNum = val.Metadata.Version.TxNum
	}

	if lastVer.BlockNum == 0 {
		return nil, nil
	}
	return lastVer, nil
}

func (s *Store) getValuesRecursively(dbName, key string, version *types.Version, predicate string, limit int) ([]*types.ValueWithMetadata, error) {
	valueVertex, err := s.getValueVertex(dbName, key, version)
	if err != nil {
		return nil, err
	}

	p := cayley.StartPath(s.cayleyGraph, valueVertex).FollowRecursive(quad.String(predicate), limit, nil)
	valueVertices, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph)
	if err != nil {
		return nil, err
	}

	return verticesToValues(valueVertices)
}

func (s *Store) getValueVertex(dbName, key string, version *types.Version) (quad.Value, error) {
	cKey := constructCompositeKey(dbName, key)
	ver, err := json.Marshal(version)
	if err != nil {
		return nil, errors.WithMessage(err, "error while marshaling version")
	}
	predicate := string(ver)

	p := cayley.StartPath(s.cayleyGraph, quad.String(cKey)).Out(quad.String(predicate))
	return p.Iterate(context.Background()).FirstValue(s.cayleyGraph)
}

func (s *Store) outEdgesFrom(verticies []string, predicate string) (map[string]*types.KVsWithMetadata, error) {
	// TODO: convert the array to map to include counts for each value. For now, the returned array
	// might contain duplicate entries if more than two vertices connects to the same vertex with an
	// edge for a given predicate
	values := map[string]*types.KVsWithMetadata{}

	for _, vertex := range verticies {
		s.logger.Debugf("finding all out edges from vertex [%s] with predicate [%s]", vertex, predicate)
		path := cayley.StartPath(s.cayleyGraph, quad.String(vertex)).Out(quad.String(predicate))

		vertices, err := path.Iterate(context.Background()).AllValues(s.cayleyGraph)
		if err != nil {
			return nil, err
		}

		DBsKV, err := verticesToKVs(vertices)
		if err != nil {
			return nil, err
		}
		for db, KVs := range DBsKV {
			if _, ok := values[db]; !ok {
				values[db] = &types.KVsWithMetadata{}
			}
			values[db].KVs = append(values[db].KVs, KVs.KVs...)
		}
	}

	return values, nil
}

func verticesToKVs(qvs []quad.Value) (map[string]*types.KVsWithMetadata, error) {
	DBsKV := map[string]*types.KVsWithMetadata{}
	var dbName string

	for _, qv := range qvs {
		kv := &types.KVWithMetadata{}
		if err := json.Unmarshal([]byte(quad.ToString(qv)), kv); err != nil {
			return nil, err
		}
		dbName, kv.Key = splitCompositeKey(kv.Key)
		if _, ok := DBsKV[dbName]; !ok {
			DBsKV[dbName] = &types.KVsWithMetadata{}
		}
		DBsKV[dbName].KVs = append(DBsKV[dbName].KVs, kv)
	}

	return DBsKV, nil
}

func verticesToValues(qvs []quad.Value) ([]*types.ValueWithMetadata, error) {
	var values []*types.ValueWithMetadata

	for _, qv := range qvs {
		v, err := vertexToValue(qv)
		if err != nil {
			return nil, err
		}

		values = append(values, v)
	}

	return values, nil
}

func vertexToValue(qv quad.Value) (*types.ValueWithMetadata, error) {
	kv := &types.KVWithMetadata{}
	if err := json.Unmarshal([]byte(quad.ToString(qv)), kv); err != nil {
		return nil, err
	}

	return &types.ValueWithMetadata{
		Value:    kv.Value,
		Metadata: kv.Metadata,
	}, nil
}

func vertexToTxIDLocation(qv quad.Value) (*TxIDLocation, error) {
	loc := &TxIDLocation{}
	if err := json.Unmarshal([]byte(quad.ToString(qv)), loc); err != nil {
		return nil, err
	}

	return loc, nil
}

const separator = "$"

func constructCompositeKey(dbName, key string) string {
	return dbName + separator + key
}

func splitCompositeKey(dbNameKey string) (dbName string, key string) {
	strs := strings.Split(dbNameKey, separator)
	return strs[0], strs[1]
}
