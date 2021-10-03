package queryexecutor

import (
	"sync"

	"github.com/IBM-Blockchain/bcdb-server/internal/stateindex"
)

func (e *WorldStateJSONQueryExecutor) executeAND(dbName string, attrsConds attributeToConditions) (map[string]bool, error) {
	attrKeys, err := e.executeAllConditions(dbName, attrsConds)
	if err != nil {
		return nil, err
	}

	var minKeys map[string]bool
	var minKeysAttr string

	for attr, keys := range attrKeys {
		if minKeys == nil {
			minKeys = keys
			minKeysAttr = attr
			continue
		}

		if len(minKeys) > len(keys) {
			minKeys = keys
			minKeysAttr = attr
		}
	}

	if len(minKeys) == 0 {
		return nil, nil
	}

	for k := range minKeys {
		for attr, keys := range attrKeys {
			if attr == minKeysAttr {
				continue
			}

			if !keys[k] {
				delete(minKeys, k)
				break
			}
		}
	}

	if len(minKeys) == 0 {
		return nil, nil
	}

	return minKeys, nil
}

func (e *WorldStateJSONQueryExecutor) executeOR(dbName string, attrsConds attributeToConditions) (map[string]bool, error) {
	attrKeys, err := e.executeAllConditions(dbName, attrsConds)
	if err != nil {
		return nil, err
	}

	unionOfKeys := make(map[string]bool)

	for _, keys := range attrKeys {
		for k := range keys {
			unionOfKeys[k] = true
		}
	}

	if len(unionOfKeys) == 0 {
		return nil, nil
	}

	return unionOfKeys, nil
}

type attributesKeys struct {
	k map[string]map[string]bool
	m sync.Mutex
}

func (e *WorldStateJSONQueryExecutor) executeAllConditions(dbName string, attrsConds attributeToConditions) (map[string]map[string]bool, error) {
	// Note that we simply create query plan for each condition and execute the same without
	// performing any kind of query optimization. In the future, we can use statistics on number
	// index entries per attribute and type to come up with complex query optimization methods.
	attrKeys := &attributesKeys{
		k: make(map[string]map[string]bool),
	}

	var wg sync.WaitGroup
	wg.Add(len(attrsConds))
	errC := make(chan error, len(attrsConds))

	for attr, conds := range attrsConds {
		go func(attr string, conds *attributeTypeAndConditions) {
			defer wg.Done()

			keys, err := e.execute(dbName, attr, conds)
			if err != nil {
				e.logger.Errorf("error while executing conditions [%v] on attribute [%s] in the database [%s]", conds, attr, dbName)
				errC <- err
				return
			}

			attrKeys.m.Lock()
			attrKeys.k[attr] = keys
			attrKeys.m.Unlock()
		}(attr, conds)
	}

	wg.Wait()

	select {
	case err := <-errC:
		return nil, err
	default:
		return attrKeys.k, nil
	}
}

func (e *WorldStateJSONQueryExecutor) execute(dbName string, attribute string, conds *attributeTypeAndConditions) (map[string]bool, error) {
	plan, err := createQueryPlan(attribute, conds)
	if err != nil {
		return nil, err
	}

	startKey, err := plan.startKey.String()
	if err != nil {
		return nil, err
	}
	endKey, err := plan.endKey.String()
	if err != nil {
		return nil, err
	}

	iter, err := e.db.GetIterator(stateindex.IndexDB(dbName), startKey, endKey)
	if err != nil {
		return nil, err
	}
	if iter.Error() != nil {
		return nil, err
	}

	keys := make(map[string]bool)
	for iter.Next() {
		if iter.Error() != nil {
			return nil, err
		}

		indexEntry := &stateindex.IndexEntry{}
		if err := indexEntry.Load(iter.Key()); err != nil {
			return nil, err
		}

		keys[indexEntry.Key] = true
	}

	return keys, nil
}
