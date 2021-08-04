// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package stateindex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

// IndexDBPrefix is the prefix added to each user database to create an index
// database for that user database
const (
	IndexDBPrefix  = "_index_"
	PositiveNumber = "p"
	NegativeNumber = "n"
)

func constructIndexEntries(updates map[string]*worldstate.DBUpdates, db worldstate.DB) (map[string]*worldstate.DBUpdates, error) {
	indexEntries := make(map[string]*worldstate.DBUpdates)

	for dbName, update := range updates {
		indexDef, _, err := db.GetIndexDefinition(dbName)
		if err != nil {
			return nil, err
		}

		if indexDef == nil {
			continue
		}

		index := map[string]types.Type{}
		if err := json.Unmarshal(indexDef, &index); err != nil {
			return nil, err
		}

		newIndexToBeCreated, oldIndexToBeDeleted, err := indexEntriesForWrites(update.Writes, index, db, dbName)
		if err != nil {
			return nil, err
		}

		toBeDeletedIndexEntries, err := indexEntriesForDeletes(update.Deletes, index, db, dbName)
		if err != nil {
			return nil, err
		}
		oldIndexToBeDeleted = append(oldIndexToBeDeleted, toBeDeletedIndexEntries...)

		dbUpdates := &worldstate.DBUpdates{}
		for _, ind := range newIndexToBeCreated {
			dbUpdates.Writes = append(dbUpdates.Writes, &worldstate.KVWithMetadata{
				Key: ind,
			})
		}
		dbUpdates.Deletes = append(dbUpdates.Deletes, oldIndexToBeDeleted...)

		if len(dbUpdates.Writes) > 0 || len(dbUpdates.Deletes) > 0 {
			indexEntries[IndexDBPrefix+dbName] = dbUpdates
		}
	}

	return indexEntries, nil
}

func indexEntriesForWrites(
	writes []*worldstate.KVWithMetadata,
	index map[string]types.Type,
	db worldstate.DB,
	dbName string,
) ([]string, []string, error) {
	newIndexEntries, err := indexEntriesForNewValues(writes, index)
	if err != nil {
		return nil, nil, err
	}

	var keysUpdated []string
	for _, w := range writes {
		keysUpdated = append(keysUpdated, w.Key)
	}
	existingIndexEntries, err := indexEntriesOfExistingValue(keysUpdated, index, db, dbName)
	if err != nil {
		return nil, nil, err
	}

	newEntries, err := toStrings(newIndexEntries)
	if err != nil {
		return nil, nil, err
	}

	existingEntries, err := toStrings(existingIndexEntries)
	if err != nil {
		return nil, nil, err
	}

	newIndexToBeCreated, oldIndexToBeDeleted := removeDuplicateIndexEntries(newEntries, existingEntries)
	return newIndexToBeCreated, oldIndexToBeDeleted, nil
}

func indexEntriesForDeletes(deletes []string, index map[string]types.Type, db worldstate.DB, dbName string) ([]string, error) {
	existingIndexOfDeletedValues, err := indexEntriesOfExistingValue(deletes, index, db, dbName)
	if err != nil {
		return nil, err
	}

	return toStrings(existingIndexOfDeletedValues)
}

type indexEntry struct {
	Attribute string      `json:"a"`
	Type      types.Type  `json:"t"`
	Metadata  string      `json:"m"`
	Value     interface{} `json:"v"`
	Key       string      `json:"k"`
}

func indexEntriesForNewValues(kvs []*worldstate.KVWithMetadata, index map[string]types.Type) ([]*indexEntry, error) {
	var indexEntriesToBeCreated []*indexEntry

	for _, kv := range kvs {
		indexEntriesToBeCreated = append(
			indexEntriesToBeCreated,
			decodeJSONAndConstructIndexEntries(kv.Key, kv.Value, index)...,
		)
	}

	return indexEntriesToBeCreated, nil
}

func indexEntriesOfExistingValue(deletes []string, index map[string]types.Type, db worldstate.DB, dbName string) ([]*indexEntry, error) {
	var indexEntriesToBeDeleted []*indexEntry

	for _, k := range deletes {
		v, _, err := db.Get(dbName, k)
		if err != nil {
			return nil, err
		}

		indexEntriesToBeDeleted = append(
			indexEntriesToBeDeleted,
			decodeJSONAndConstructIndexEntries(k, v, index)...,
		)
	}

	return indexEntriesToBeDeleted, nil
}

func decodeJSONAndConstructIndexEntries(key string, value []byte, index map[string]types.Type) []*indexEntry {
	val := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(value))
	decoder.UseNumber()
	if err := decoder.Decode(&val); err != nil {
		// if the existing value is not of JSON type, we can skip and move
		// to the next item
		return nil
	}
	partialIndexes := partialIndexEntriesForValue(reflect.ValueOf(val), index)

	var indexEntries []*indexEntry
	for _, partialIndex := range partialIndexes {
		partialIndex.Key = key
		indexEntries = append(indexEntries, partialIndex)
	}

	return indexEntries
}

func partialIndexEntriesForValue(v reflect.Value, index map[string]types.Type) []*indexEntry {
	if v.IsNil() {
		return nil
	}
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	var partialIndexEntries []*indexEntry

	if v.Kind() != reflect.Map {
		return nil
	}

	for _, attr := range v.MapKeys() {
		actualType := getType(v.MapIndex(attr))
		if actualType != reflect.String && actualType != reflect.Bool {
			partialIndexEntries = append(partialIndexEntries, partialIndexEntriesForValue(v.MapIndex(attr), index)...)
			continue
		}

		for attrToBeIndexed, valueType := range index {
			if attr.String() != attrToBeIndexed {
				continue
			}

			same, value := isTypeSame(v.MapIndex(attr), valueType)
			if same {
				e := &indexEntry{
					Attribute: attr.String(),
					Type:      valueType,
				}
				if valueType == types.Type_NUMBER {
					num := value.(int64)
					if num >= 0 {
						e.Metadata = PositiveNumber
						e.Value = encodeOrderPreservingVarUint64(uint64(num))
					} else {
						e.Metadata = NegativeNumber
						e.Value = encodeReverseOrderVarUint64(uint64(-num))
					}
				} else {
					e.Value = value
				}
				partialIndexEntries = append(partialIndexEntries, e)
			}
			break
		}
	}

	return partialIndexEntries
}

func getType(v reflect.Value) reflect.Kind {
	if v.IsNil() {
		return reflect.Invalid
	}
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	return v.Kind()
}

func isTypeSame(v reflect.Value, t types.Type) (bool, interface{}) {
	if v.IsNil() {
		return false, nil
	}
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.String:
		if v.Type().Name() == "Number" {
			if t == types.Type_NUMBER {
				num, err := strconv.ParseInt(fmt.Sprintf(`%v`, v), 10, 64)
				if err != nil {
					// float is not supported in index
					return false, nil
				}
				return true, num
			}
			return false, nil
		}

		if t == types.Type_STRING {
			return true, fmt.Sprintf(`%v`, v)
		}

	case reflect.Bool:
		if t == types.Type_BOOLEAN {
			return true, v.Bool()
		}
		return false, nil
	}

	return false, nil
}

func removeDuplicateIndexEntries(indexOfNewValues, indexOfExistingValues []string) ([]string, []string) {
	newIndexEntries := make(map[string]bool)
	for _, e := range indexOfNewValues {
		newIndexEntries[e] = true
	}

	existingIndexEntries := make(map[string]bool)
	for _, e := range indexOfExistingValues {
		existingIndexEntries[e] = true
	}

	for _, e := range indexOfNewValues {
		if existingIndexEntries[e] {
			delete(newIndexEntries, e)
			delete(existingIndexEntries, e)
		}
	}

	var newIndex []string
	var existingIndex []string

	if len(indexOfNewValues) == len(newIndexEntries) {
		// no duplicates have been found
		return indexOfNewValues, indexOfExistingValues
	}

	// only if there exist a duplicate entry, we would reach here
	for e := range newIndexEntries {
		newIndex = append(newIndex, e)
	}
	for ind := range existingIndexEntries {
		existingIndex = append(existingIndex, ind)
	}

	return newIndex, existingIndex
}

func toStrings(indexEntries []*indexEntry) ([]string, error) {
	var entries []string
	for _, indexEntry := range indexEntries {
		b, err := json.Marshal(indexEntry)
		if err != nil {
			return nil, err
		}
		entries = append(entries, string(b))
	}

	return entries, nil
}
