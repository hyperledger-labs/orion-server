package provenance

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/cayleygraph/cayley"
	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/quad"
	"github.com/pkg/errors"
	"github.ibm.com/blockchaindb/server/pkg/types"
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
	DBName             string
	UserID             string
	TxID               string
	Reads              []*KeyWithVersion
	Writes             []*types.KVWithMetadata
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
//  4. key--(version)-->value
//  5. value<--(previous)--value
//  6. value--(next)-->value
func (s *Store) Commit(blockNum uint64, txsData []*TxDataForProvenance) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	cayleyTx := graph.NewTransaction()
	values := make(map[string]string)

	for index, tx := range txsData {
		s.logger.Debugf("userID[%s]---(submitted)--->txID[%s]", tx.UserID, tx.TxID)
		cayleyTx.AddQuad(quad.Make(tx.UserID, SUBMITTED, tx.TxID, ""))

		loc, err := json.Marshal(&TxIDLocation{blockNum, index})
		if err != nil {
			return errors.WithMessage(err, "error while marshaling txID location")
		}
		s.logger.Debugf("loc[%s]]---(includes)--->txID[%s]", loc, tx.TxID)
		cayleyTx.AddQuad(quad.Make(string(loc), INCLUDES, tx.TxID, ""))

		for _, read := range tx.Reads {
			value, err := s.getValueVertex(tx.DBName, read.Key, read.Version)
			if err != nil {
				return err
			}

			s.logger.Debugf("txID[%s]---(reads)--->value[%s]", tx.TxID, quad.NativeOf(value))
			cayleyTx.AddQuad(quad.Make(tx.TxID, READS, value, ""))
		}

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
			cayleyTx.AddQuad(quad.Make(write.Key, string(newVersion), string(newValue), ""))

			s.logger.Debugf("txID[%s]---(writes)--->value[%s]", tx.TxID, string(newValue))
			cayleyTx.AddQuad(quad.Make(tx.TxID, WRITES, string(newValue), ""))

			oldVersion, ok := tx.OldVersionOfWrites[actualKey]
			if !ok {
				values[actualKey] = string(newValue)
				continue
			}

			oldValue, err := s.getValueVertex(tx.DBName, actualKey, oldVersion)
			if err != nil {
				return err
			}

			if oldValue == nil {
				oldValueStr, ok := values[actualKey]
				if !ok {
					s.logger.Debugf("key [%s] version [%d,%d] for which oldValue is not found", actualKey, oldVersion.BlockNum, oldVersion.TxNum)
					return errors.Errorf("error while finding the previous version of the key[%s]", write.Key)
				}

				oldValue = quad.String(oldValueStr)
			}

			s.logger.Debugf("oldValue[%s]<---(previous)---newValue[%s]", quad.NativeOf(oldValue), string(newValue))
			cayleyTx.AddQuad(quad.Make(string(newValue), PREVIOUS, oldValue, ""))

			s.logger.Debugf("oldValue[%s]---(next)--->newValue[%s]", quad.NativeOf(oldValue), string(newValue))
			cayleyTx.AddQuad(quad.Make(oldValue, NEXT, string(newValue), ""))

			values[actualKey] = string(newValue)
		}
	}

	return s.cayleyGraph.ApplyTransaction(cayleyTx)
}

// GetValues returns all values associated with a given key
func (s *Store) GetValues(dbName, key string) ([]*types.ValueWithMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	s.logger.Debugf("fetch all historical values associated with the key [%s] in db [%s]", key, dbName)
	cKey := constructCompositeKey(dbName, key)
	p := cayley.StartPath(s.cayleyGraph, quad.String(cKey)).Out()

	valueVertices, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph.QuadStore)
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
func (s *Store) GetValuesReadByUser(userID string) ([]*types.KVWithMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	txIDs, err := s.GetTxIDsSubmittedByUser(userID)
	if err != nil {
		return nil, err
	}

	return s.outEdgesFrom(txIDs, READS)
}

// GetValuesWrittenByUser returns all values written by a given user
func (s *Store) GetValuesWrittenByUser(userID string) ([]*types.KVWithMetadata, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	txIDs, err := s.GetTxIDsSubmittedByUser(userID)
	if err != nil {
		return nil, err
	}

	return s.outEdgesFrom(txIDs, WRITES)
}

// GetReaders returns all userIDs who have accessed a given key as well as the access frequency
func (s *Store) GetReaders(dbName, key string) (map[string]int, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	cKey := constructCompositeKey(dbName, key)
	p := cayley.StartPath(s.cayleyGraph, quad.String(cKey)).Out().In(quad.String(READS)).In(quad.String(SUBMITTED))
	vertices, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph)
	if err != nil {
		return nil, err
	}

	userIDs := make(map[string]int)
	for _, qv := range vertices {
		userIDs[quad.ToString(qv)]++
	}

	return userIDs, err
}

// GetWriters returns all userIDs who have modified a given key as well as the modifcation frequency
func (s *Store) GetWriters(dbName, key string) (map[string]int, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	cKey := constructCompositeKey(dbName, key)
	p := cayley.StartPath(s.cayleyGraph, quad.String(cKey)).Out().In(quad.String(WRITES)).In(quad.String(SUBMITTED))
	vertices, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph)
	if err != nil {
		return nil, err
	}

	userIDs := make(map[string]int)
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

	vertices, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph.QuadStore)
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

	vertex, err := p.Iterate(context.Background()).FirstValue(s.cayleyGraph.QuadStore)
	if err != nil {
		return nil, err
	}

	return vertexToTxIDLocation(vertex)
}

func (s *Store) getValuesRecursively(dbName, key string, version *types.Version, predicate string, limit int) ([]*types.ValueWithMetadata, error) {
	valueVertex, err := s.getValueVertex(dbName, key, version)
	if err != nil {
		return nil, err
	}

	p := cayley.StartPath(s.cayleyGraph, valueVertex).FollowRecursive(quad.String(predicate), limit, nil)
	valueVertices, err := p.Iterate(context.Background()).AllValues(s.cayleyGraph.QuadStore)
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
	return p.Iterate(context.Background()).FirstValue(s.cayleyGraph.QuadStore)
}

func (s *Store) outEdgesFrom(verticies []string, predicate string) ([]*types.KVWithMetadata, error) {
	// TODO: convert the array to map to include counts for each value. For now, the returned array
	// might contain duplicate entries if more than two vertices connects to the same vertex with an
	// edge for a given predicate
	var values []*types.KVWithMetadata

	for _, vertex := range verticies {
		s.logger.Debugf("finding all out edges from vertex [%s] with predicate [%s]", vertex, predicate)
		path := cayley.StartPath(s.cayleyGraph, quad.String(vertex)).Out(quad.String(predicate))

		vertices, err := path.Iterate(context.Background()).AllValues(s.cayleyGraph.QuadStore)
		if err != nil {
			return nil, err
		}

		kvs, err := verticesToKVs(vertices)
		if err != nil {
			return nil, err
		}
		values = append(values, kvs...)
	}

	return values, nil
}

func verticesToKVs(qvs []quad.Value) ([]*types.KVWithMetadata, error) {
	var KVs []*types.KVWithMetadata

	for _, qv := range qvs {
		kv := &types.KVWithMetadata{}
		if err := json.Unmarshal([]byte(quad.ToString(qv)), kv); err != nil {
			return nil, err
		}
		_, kv.Key = splitCompositeKey(kv.Key)

		KVs = append(KVs, kv)
	}

	return KVs, nil
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
