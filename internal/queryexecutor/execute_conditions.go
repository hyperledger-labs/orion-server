package queryexecutor

import (
	"context"
	"sync"

	"github.com/hyperledger-labs/orion-server/internal/stateindex"
)

func (e *WorldStateJSONQueryExecutor) executeAND(ctx context.Context, dbName string, attrsConds attributeToConditions) (map[string]bool, error) {
	attrKeys, err := e.executeAllConditions(ctx, dbName, attrsConds)
	if err != nil {
		return nil, err
	}

	keys := intersection(ctx, attrKeys)
	return keys, nil
}

func intersection(ctx context.Context, attrToKeys map[string]map[string]bool) map[string]bool {
	var minKeys map[string]bool
	var minKeysAttr string

	for attr, keys := range attrToKeys {
		select {
		case <-ctx.Done():
			return nil
		default:
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
	}

	if len(minKeys) == 0 {
		return nil
	}

	for k := range minKeys {
		for attr, keys := range attrToKeys {
			select {
			case <-ctx.Done():
				return nil
			default:
				if attr == minKeysAttr {
					continue
				}

				if !keys[k] {
					delete(minKeys, k)
					break
				}
			}
		}
	}

	if len(minKeys) == 0 {
		return nil
	}

	return minKeys
}

func (e *WorldStateJSONQueryExecutor) executeOR(ctx context.Context, dbName string, attrsConds attributeToConditions) (map[string]bool, error) {
	attrKeys, err := e.executeAllConditions(ctx, dbName, attrsConds)
	if err != nil {
		return nil, err
	}

	keys := union(ctx, attrKeys)
	return keys, nil
}

func union(ctx context.Context, attrToKeys map[string]map[string]bool) map[string]bool {
	unionOfKeys := make(map[string]bool)

	for _, keys := range attrToKeys {
		for k := range keys {
			select {
			case <-ctx.Done():
				return nil
			default:
				unionOfKeys[k] = true
			}
		}
	}

	if len(unionOfKeys) == 0 {
		return nil
	}

	return unionOfKeys
}

type attributesKeys struct {
	k map[string]map[string]bool
	m sync.Mutex
}

func (e *WorldStateJSONQueryExecutor) executeAllConditions(ctx context.Context, dbName string, attrsConds attributeToConditions) (map[string]map[string]bool, error) {
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

			keys, err := e.execute(ctx, dbName, attr, conds)
			select {
			case <-ctx.Done():
				return
			default:
				if err != nil {
					e.logger.Errorf("error while executing conditions [%v] on attribute [%s] in the database [%s]", conds, attr, dbName)
					errC <- err
					return
				}

				attrKeys.m.Lock()
				attrKeys.k[attr] = keys
				attrKeys.m.Unlock()
			}
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

func (e *WorldStateJSONQueryExecutor) execute(ctx context.Context, dbName string, attribute string, conds *attributeTypeAndConditions) (map[string]bool, error) {
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
		select {
		case <-ctx.Done():
			return nil, nil
		default:
			if iter.Error() != nil {
				return nil, err
			}

			indexEntry := &stateindex.IndexEntry{}
			if err := indexEntry.Load(iter.Key()); err != nil {
				return nil, err
			}

			if len(plan.excludeKeys) == 0 {
				keys[indexEntry.Key] = true
				continue
			}

			// we may need to skip entries continously
			for {
				if _, ok := plan.excludeKeys[indexEntry.Value]; !ok {
					keys[indexEntry.Key] = true
					break
				}

				seekKey := plan.excludeKeys[indexEntry.Value]
				key, err := seekKey.String()
				if err != nil {
					return nil, err
				}
				e.logger.Debug("skipping to the next entry of [" + key + "]")

				itemExist := iter.Seek([]byte(key))
				if !itemExist {
					break
				}

				delete(plan.excludeKeys, indexEntry.Value)

				indexEntry = &stateindex.IndexEntry{}
				if err := indexEntry.Load(iter.Key()); err != nil {
					return nil, err
				}
			}
		}
	}

	return keys, nil
}
