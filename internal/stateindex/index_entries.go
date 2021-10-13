// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package stateindex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

const (
	// indexDBPrefix is the prefix added to each user database to create an index
	// database for that user database
	indexDBPrefix = "_index_"
)

const (
	Beginning = iota + 1
	Existing
	Ending
)

// ConstructIndexEntries constructs index entries for the supplied the world state updates
func ConstructIndexEntries(updates map[string]*worldstate.DBUpdates, db worldstate.DB) (map[string]*worldstate.DBUpdates, error) {
	indexEntries := make(map[string]*worldstate.DBUpdates)

	for dbName, update := range updates {
		indexDef, _, err := db.GetIndexDefinition(dbName)
		if err != nil {
			return nil, err
		}

		if indexDef == nil {
			continue
		}

		index := map[string]types.IndexAttributeType{}
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
			indexEntries[IndexDB(dbName)] = dbUpdates
		}
	}

	return indexEntries, nil
}

func indexEntriesForWrites(
	writes []*worldstate.KVWithMetadata,
	index map[string]types.IndexAttributeType,
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

func indexEntriesForDeletes(deletes []string, index map[string]types.IndexAttributeType, db worldstate.DB, dbName string) ([]string, error) {
	existingIndexOfDeletedValues, err := indexEntriesOfExistingValue(deletes, index, db, dbName)
	if err != nil {
		return nil, err
	}

	return toStrings(existingIndexOfDeletedValues)
}

// IndexEntry hold metadata associated with the attribute being indexed along with the attribute value and key
type IndexEntry struct {
	Attribute     string                   `json:"a"`
	Type          types.IndexAttributeType `json:"t"`
	ValuePosition int8                     `json:"vp"` // ValuePosition is used such that range query for lesser than, greater than can be executed easily
	Value         interface{}              `json:"v"`
	KeyPosition   int8                     `json:"kp"` // KeyPosition is used such that range query for lesser than, greater than can be executed easily
	Key           string                   `json:"k"`
}

// String returns a string representation of the indexEntry
func (e *IndexEntry) String() (string, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

// Load loads the string representation of IndexEntry into the IndexEntry object
func (e *IndexEntry) Load(entry []byte) error {
	return json.Unmarshal(entry, e)
}

func indexEntriesForNewValues(kvs []*worldstate.KVWithMetadata, index map[string]types.IndexAttributeType) ([]*IndexEntry, error) {
	var indexEntriesToBeCreated []*IndexEntry

	for _, kv := range kvs {
		indexEntriesToBeCreated = append(
			indexEntriesToBeCreated,
			decodeJSONAndConstructIndexEntries(kv.Key, kv.Value, index)...,
		)
	}

	return indexEntriesToBeCreated, nil
}

func indexEntriesOfExistingValue(deletes []string, index map[string]types.IndexAttributeType, db worldstate.DB, dbName string) ([]*IndexEntry, error) {
	var indexEntriesToBeDeleted []*IndexEntry

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

func decodeJSONAndConstructIndexEntries(key string, value []byte, index map[string]types.IndexAttributeType) []*IndexEntry {
	val := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(value))
	decoder.UseNumber()
	if err := decoder.Decode(&val); err != nil {
		// if the existing value is not of JSON type, we can skip and move
		// to the next item
		return nil
	}
	partialIndexes := partialIndexEntriesForValue(reflect.ValueOf(val), index)

	var indexEntries []*IndexEntry
	for _, partialIndex := range partialIndexes {
		partialIndex.Key = key
		indexEntries = append(indexEntries, partialIndex)
	}

	return indexEntries
}

func partialIndexEntriesForValue(v reflect.Value, index map[string]types.IndexAttributeType) []*IndexEntry {
	if v.IsNil() {
		return nil
	}
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	var partialIndexEntries []*IndexEntry

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
				e := &IndexEntry{
					Attribute:     attr.String(),
					Type:          valueType,
					ValuePosition: Existing,
					KeyPosition:   Existing,
				}
				e.Value = GetValue(value, valueType)
				partialIndexEntries = append(partialIndexEntries, e)
			}
			break
		}
	}

	return partialIndexEntries
}

// GetValue returns the value used by the index creator and the associated metadata
func GetValue(value interface{}, t types.IndexAttributeType) interface{} {
	if t != types.IndexAttributeType_NUMBER {
		return value
	}

	return EncodeInt64(value.(int64))
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

func isTypeSame(v reflect.Value, t types.IndexAttributeType) (bool, interface{}) {
	if v.IsNil() {
		return false, nil
	}
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.String:
		if v.Type().Name() == "Number" {
			if t == types.IndexAttributeType_NUMBER {
				num, err := strconv.ParseInt(fmt.Sprintf(`%v`, v), 10, 64)
				if err != nil {
					// float is not supported in index
					return false, nil
				}
				return true, num
			}
			return false, nil
		}

		if t == types.IndexAttributeType_STRING {
			return true, fmt.Sprintf(`%v`, v)
		}

	case reflect.Bool:
		if t == types.IndexAttributeType_BOOLEAN {
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

func toStrings(indexEntries []*IndexEntry) ([]string, error) {
	var entries []string
	for _, indexEntry := range indexEntries {
		e, err := indexEntry.String()
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}

	return entries, nil
}

func IndexDB(dbName string) string {
	return indexDBPrefix + dbName
}
