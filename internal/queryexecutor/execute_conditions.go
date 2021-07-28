package queryexecutor

import (
	"encoding/json"
	"sync"

	"github.com/IBM-Blockchain/bcdb-server/internal/stateindex"
)

func (e *WorldStateQueryExecutor) executeAND(dbName string, attrsConds attributesConditions) (map[string]bool, error) {
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

func (e *WorldStateQueryExecutor) executeOR(dbName string, attrsConds attributesConditions) (map[string]bool, error) {
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

func (e *WorldStateQueryExecutor) executeAllConditions(dbName string, attrsConds attributesConditions) (map[string]map[string]bool, error) {
	attrKeys := &attributesKeys{
		k: make(map[string]map[string]bool),
	}

	var wg sync.WaitGroup
	wg.Add(len(attrsConds))
	errC := make(chan error, len(attrsConds))

	for attr, conds := range attrsConds {
		go func(attr string, conds *attrConditions) {
			defer wg.Done()

			keys, err := e.execute(dbName, attr, conds)
			if err != nil {
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

func (e *WorldStateQueryExecutor) execute(dbName string, attribute string, conds *attrConditions) (map[string]bool, error) {
	plan, err := createQueryPlan(attribute, conds)
	if err != nil {
		return nil, err
	}

	startKey, err := json.Marshal(plan.startKey)
	if err != nil {
		return nil, err
	}
	endKey, err := json.Marshal(plan.endKey)
	if err != nil {
		return nil, err
	}

	iter, err := e.db.GetIterator(stateindex.IndexDB(dbName), string(startKey), string(endKey))
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
		if err := json.Unmarshal([]byte(iter.Key()), indexEntry); err != nil {
			return nil, err
		}

		keys[indexEntry.Key] = true
	}

	return keys, nil
}
