// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package stateindex

import (
	"bytes"
	"encoding/json"
	"math"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

type indexTestEnv struct {
	db      *leveldb.LevelDB
	dbPath  string
	cleanup func()
}

func newIndexTestEnv(t *testing.T) *indexTestEnv {
	lc := &logger.Config{
		Level:         "debug",
		OutputPath:    []string{"stdout"},
		ErrOutputPath: []string{"stderr"},
		Encoding:      "console",
	}
	logger, err := logger.New(lc)
	require.NoError(t, err)

	dir := t.TempDir()

	dbPath := filepath.Join(dir, "leveldb")
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    logger,
		},
	)
	if err != nil {
		t.Fatalf("error while creating leveldb, %v", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			t.Errorf("error while closing the db instance, %v", err)
		}
	}

	return &indexTestEnv{
		db:      db,
		dbPath:  dbPath,
		cleanup: cleanup,
	}
}

func TestConstructIndexEntries(t *testing.T) {
	indexDB1 := map[string]types.IndexAttributeType{
		"a1": types.IndexAttributeType_NUMBER,
		"a2": types.IndexAttributeType_STRING,
		"a3": types.IndexAttributeType_BOOLEAN,
	}
	indexDB1Json, err := json.Marshal(indexDB1)
	require.NoError(t, err)

	indexDB2 := map[string]types.IndexAttributeType{
		"a2": types.IndexAttributeType_STRING,
	}
	indexDB2Json, err := json.Marshal(indexDB2)
	require.NoError(t, err)

	encoded10 := EncodeInt64(10)
	createDBsWithIndex := map[string]*worldstate.DBUpdates{
		worldstate.DatabasesDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   "db1",
					Value: indexDB1Json,
				},
				{
					Key:   "db2",
					Value: indexDB2Json,
				},
			},
		},
	}

	testCases := []struct {
		name                 string
		setup                func(db worldstate.DB)
		updates              map[string]*worldstate.DBUpdates
		expectedIndexEntries map[string]*worldstate.DBUpdates
	}{
		{
			name: "no index definition",
			setup: func(db worldstate.DB) {
				createDB := map[string]*worldstate.DBUpdates{
					worldstate.DatabasesDBName: {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key: "db1",
							},
							{
								Key: "db2",
							},
						},
					},
				}
				require.NoError(t, db.Commit(createDB, 1))
			},
			updates: map[string]*worldstate.DBUpdates{
				"db1": {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key:   "person1",
							Value: []byte(`{"a1":10,"a2":"ten","a3":true}`),
						},
					},
				},
				"db2": {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key:   "person1",
							Value: []byte(`{"a1":11,"a2":"eleven","a3":true}`),
						},
					},
				},
			},
			expectedIndexEntries: nil,
		},
		{
			name: "index definition exist but nothing matches the index",
			setup: func(db worldstate.DB) {
				require.NoError(t, db.Commit(createDBsWithIndex, 1))
			},
			updates: map[string]*worldstate.DBUpdates{
				"db1": {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key:   "person1",
							Value: []byte(`{"a4":10,"a5":"ten","a6":true}`),
						},
					},
				},
				"db2": {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key:   "person1",
							Value: []byte(`{"a1":11,"a3":"eleven","a5":true}`),
						},
					},
				},
			},
			expectedIndexEntries: nil,
		},
		{
			name: "only writes of new kv pairs",
			setup: func(db worldstate.DB) {
				require.NoError(t, db.Commit(createDBsWithIndex, 1))
			},
			updates: map[string]*worldstate.DBUpdates{
				"db1": {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key:   "person1",
							Value: []byte(`{"a1":10,"a2":"ten","a3":true}`),
						},
					},
				},
				"db2": {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key:   "person2",
							Value: []byte(`{"a1":11,"a2":"eleven","a3":true}`),
						},
					},
				},
			},
			expectedIndexEntries: map[string]*worldstate.DBUpdates{
				IndexDB("db1"): {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key: `{"a":"a1","t":0,"vp":2,"v":"` + encoded10 + `","kp":2,"k":"person1"}`,
						},
						{
							Key: `{"a":"a2","t":1,"vp":2,"v":"ten","kp":2,"k":"person1"}`,
						},
						{
							Key: `{"a":"a3","t":2,"vp":2,"v":true,"kp":2,"k":"person1"}`,
						},
					},
				},
				IndexDB("db2"): {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key: `{"a":"a2","t":1,"vp":2,"v":"eleven","kp":2,"k":"person2"}`,
						},
					},
				},
			},
		},
		{
			name: "writes of existing kv pairs",
			setup: func(db worldstate.DB) {
				require.NoError(t, db.Commit(createDBsWithIndex, 1))
				updates := map[string]*worldstate.DBUpdates{
					"db1": {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   "person1",
								Value: []byte(`{"a1":10,"a2":"ten","a3":false}`),
							},
						},
					},
					"db2": {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   "person2",
								Value: []byte(`{"a1":10,"a2":"ten","a3":false}`),
							},
						},
					},
				}
				require.NoError(t, db.Commit(updates, 2))
			},
			updates: map[string]*worldstate.DBUpdates{
				"db1": {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key:   "person1",
							Value: []byte(`{"a1":10,"a2":"10","a3":true}`),
						},
					},
				},
				"db2": {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key:   "person2",
							Value: []byte(`{"a1":11,"a2":"eleven","a3":true}`),
						},
					},
				},
			},
			expectedIndexEntries: map[string]*worldstate.DBUpdates{
				IndexDB("db1"): {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key: `{"a":"a3","t":2,"vp":2,"v":true,"kp":2,"k":"person1"}`,
						},
						{
							Key: `{"a":"a2","t":1,"vp":2,"v":"10","kp":2,"k":"person1"}`,
						},
					},
					Deletes: []string{
						`{"a":"a3","t":2,"vp":2,"v":false,"kp":2,"k":"person1"}`,
						`{"a":"a2","t":1,"vp":2,"v":"ten","kp":2,"k":"person1"}`,
					},
				},
				IndexDB("db2"): {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key: `{"a":"a2","t":1,"vp":2,"v":"eleven","kp":2,"k":"person2"}`,
						},
					},
					Deletes: []string{
						`{"a":"a2","t":1,"vp":2,"v":"ten","kp":2,"k":"person2"}`,
					},
				},
			},
		},
		{
			name: "delete of existing kv pairs",
			setup: func(db worldstate.DB) {
				require.NoError(t, db.Commit(createDBsWithIndex, 1))
				updates := map[string]*worldstate.DBUpdates{
					"db1": {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   "person1",
								Value: []byte(`{"a1":10,"a2":"ten","a3":true}`),
							},
						},
					},
					"db2": {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   "person2",
								Value: []byte(`{"a1":11,"a2":"eleven","a3":true}`),
							},
						},
					},
				}
				require.NoError(t, db.Commit(updates, 2))
			},
			updates: map[string]*worldstate.DBUpdates{
				"db1": {
					Deletes: []string{"person1"},
				},
				"db2": {
					Deletes: []string{"person2"},
				},
			},
			expectedIndexEntries: map[string]*worldstate.DBUpdates{
				IndexDB("db1"): {
					Deletes: []string{
						`{"a":"a1","t":0,"vp":2,"v":"` + encoded10 + `","kp":2,"k":"person1"}`,
						`{"a":"a2","t":1,"vp":2,"v":"ten","kp":2,"k":"person1"}`,
						`{"a":"a3","t":2,"vp":2,"v":true,"kp":2,"k":"person1"}`,
					},
				},
				IndexDB("db2"): {
					Deletes: []string{
						`{"a":"a2","t":1,"vp":2,"v":"eleven","kp":2,"k":"person2"}`,
					},
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			env := newIndexTestEnv(t)
			tt.setup(env.db)
			indexEntries, err := ConstructIndexEntries(tt.updates, env.db)
			require.NoError(t, err)
			require.Equal(t, len(tt.expectedIndexEntries), len(indexEntries))
			for dbName, expectedEntries := range tt.expectedIndexEntries {
				require.ElementsMatch(t, expectedEntries.Writes, indexEntries[dbName].Writes)
				require.ElementsMatch(t, expectedEntries.Deletes, indexEntries[dbName].Deletes)
			}
		})
	}
}

func TestIndexEntriesForNewValues(t *testing.T) {
	indexDef := map[string]types.IndexAttributeType{
		"age": types.IndexAttributeType_NUMBER,
	}

	encoded25 := EncodeInt64(25)
	encoded26 := EncodeInt64(26)

	testCases := []struct {
		name                 string
		kvs                  []*worldstate.KVWithMetadata
		expectedIndexEntries []*IndexEntry
	}{
		{
			name: "non-json values",
			kvs: []*worldstate.KVWithMetadata{
				{
					Key:   "person1",
					Value: []byte("value1"),
				},
				{
					Key:   "person2",
					Value: []byte("value2"),
				},
			},
			expectedIndexEntries: nil,
		},
		{
			name: "json values but does not have index attributes",
			kvs: []*worldstate.KVWithMetadata{
				{
					Key:   "person1",
					Value: []byte(`{"weight": 25}`),
				},
				{
					Key:   "person2",
					Value: []byte(`{"weight": 26}`),
				},
			},
			expectedIndexEntries: nil,
		},
		{
			name: "json values with index attributes",
			kvs: []*worldstate.KVWithMetadata{
				{
					Key:   "person1",
					Value: []byte(`{"age": 25}`),
				},
				{
					Key:   "person2",
					Value: []byte(`{"age": 26}`),
				},
			},
			expectedIndexEntries: []*IndexEntry{
				{
					Attribute:     "age",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: Existing,
					Value:         encoded25,
					KeyPosition:   Existing,
					Key:           "person1",
				},
				{
					Attribute:     "age",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: Existing,
					Value:         encoded26,
					KeyPosition:   Existing,
					Key:           "person2",
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			indexEntries, err := indexEntriesForNewValues(tt.kvs, indexDef)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expectedIndexEntries, indexEntries)
		})
	}
}

func TestIndexEntriesOfExistingValues(t *testing.T) {
	indexDef := map[string]types.IndexAttributeType{
		"age": types.IndexAttributeType_NUMBER,
	}

	encoded25 := EncodeInt64(25)
	encodedNegative26 := EncodeInt64(-26)
	encoded0 := EncodeInt64(0)
	encodedMax := EncodeInt64(math.MaxInt64)

	testCases := []struct {
		name                 string
		setup                func(db worldstate.DB)
		dbName               string
		deletedKeys          []string
		expectedIndexEntries []*IndexEntry
	}{
		{
			name: "non-json values",
			setup: func(db worldstate.DB) {
				updates := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   "person1",
								Value: []byte("value1"),
							},
							{
								Key:   "person2",
								Value: []byte("value2"),
							},
						},
					},
				}
				require.NoError(t, db.Commit(updates, 1))
			},
			dbName:               worldstate.DefaultDBName,
			deletedKeys:          []string{"person1", "person2"},
			expectedIndexEntries: nil,
		},
		{
			name: "json values but does not have index attributes",
			setup: func(db worldstate.DB) {
				updates := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   "person1",
								Value: []byte(`{"weight": 25}`),
							},
							{
								Key:   "person2",
								Value: []byte(`{"weight": 26}`),
							},
						},
					},
				}
				require.NoError(t, db.Commit(updates, 1))
			},
			dbName:               worldstate.DefaultDBName,
			deletedKeys:          []string{"person1", "person2"},
			expectedIndexEntries: nil,
		},
		{
			name: "json values with index attributes",
			setup: func(db worldstate.DB) {
				updates := map[string]*worldstate.DBUpdates{
					worldstate.DefaultDBName: {
						Writes: []*worldstate.KVWithMetadata{
							{
								Key:   "person1",
								Value: []byte(`{"age": 25}`),
							},
							{
								Key:   "person2",
								Value: []byte(`{"age": -26}`),
							},
							{
								Key:   "person3",
								Value: []byte(`{"age": 0}`),
							},
							{
								Key:   "person4",
								Value: []byte(`{"age": 9223372036854775807}`),
							},
						},
					},
				}
				require.NoError(t, db.Commit(updates, 1))
			},
			dbName:      worldstate.DefaultDBName,
			deletedKeys: []string{"person1", "person2", "person3", "person4"},
			expectedIndexEntries: []*IndexEntry{
				{
					Attribute:     "age",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: Existing,
					Value:         encoded25,
					KeyPosition:   Existing,
					Key:           "person1",
				},
				{
					Attribute:     "age",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: Existing,
					Value:         encodedNegative26,
					KeyPosition:   Existing,
					Key:           "person2",
				},
				{
					Attribute:     "age",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: Existing,
					Value:         encoded0,
					KeyPosition:   Existing,
					Key:           "person3",
				},
				{
					Attribute:     "age",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: Existing,
					Value:         encodedMax,
					KeyPosition:   Existing,
					Key:           "person4",
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			env := newIndexTestEnv(t)
			tt.setup(env.db)
			indexEntries, err := indexEntriesOfExistingValue(tt.deletedKeys, indexDef, env.db, tt.dbName)
			require.NoError(t, err)
			require.ElementsMatch(t, tt.expectedIndexEntries, indexEntries)
		})
	}
}

func TestPartialIndexEntriesForValue(t *testing.T) {
	encoded10 := EncodeInt64(10)
	expectedIndexEntries :=
		[]*IndexEntry{
			{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         encoded10,
				KeyPosition:   Existing,
			},
			{
				Attribute:     "a2",
				Type:          types.IndexAttributeType_STRING,
				ValuePosition: Existing,
				Value:         "female",
				KeyPosition:   Existing,
			},
			{
				Attribute:     "a3",
				Type:          types.IndexAttributeType_BOOLEAN,
				ValuePosition: Existing,
				Value:         true,
				KeyPosition:   Existing,
			},
		}

	testCases := []struct {
		name  string
		json  []byte
		index map[string]types.IndexAttributeType
	}{
		{
			name: "number, string, boolean in a simple JSON",
			json: []byte(
				`{
					"a1":10,
					"a2":"female",
					"a3":true,
					"a4":"engineer"
				}`),
			index: map[string]types.IndexAttributeType{
				"a1": types.IndexAttributeType_NUMBER,
				"a2": types.IndexAttributeType_STRING,
				"a3": types.IndexAttributeType_BOOLEAN,
			},
		},
		{
			name: "number, string, boolean in a two level JSON",
			json: []byte(
				`{
					 "first1":{
					 	"a1":10
					 },
					 "first2":{
					 	"a2":"female",
						"a6": false,
						"a3": []
					 },
					 "first3":{
					 	"a3":true,
						"a5": 11,
						"a2": [1, 2, 3, 4],
						"a1": 10.3
					 },
					 "a4":"engineer"
				}`,
			),
			index: map[string]types.IndexAttributeType{
				"a1": types.IndexAttributeType_NUMBER,
				"a2": types.IndexAttributeType_STRING,
				"a3": types.IndexAttributeType_BOOLEAN,
			},
		},
		{
			name: "number, string, boolean in a three levels JSON",
			json: []byte(
				`{
					 "first1":{
						"second1": {
							"a1": 10
						}
					 },
					 "first2":{
						"second2": {
							"a2": "female"
						},
						"a6": true
					 },
					 "first3":{
						"second3" : {
							"a3": true
						},
						"a5": 11,
						"a1": 23.564
					 },
					 "a4":"engineer"
				}`,
			),
			index: map[string]types.IndexAttributeType{
				"a1": types.IndexAttributeType_NUMBER,
				"a2": types.IndexAttributeType_STRING,
				"a3": types.IndexAttributeType_BOOLEAN,
			},
		},
		{
			name: "number, string, boolean in a three levels JSON but duplicate attributes",
			json: []byte(
				`{
					 "first1":{
					 	"a1": false,
						"second1": {
							"a1": 10
						}
					 },
					 "first2":{
					 	"a2": 10,
						"second2": {
							"a2": "female"
						},
						"a6": true
					 },
					 "first3":{
					 	"a3": "person1",
						"second3" : {
							"a3": true
						},
						"a5": 11
					 },
					 "a4":"engineer"
				}`,
			),
			index: map[string]types.IndexAttributeType{
				"a1": types.IndexAttributeType_NUMBER,
				"a2": types.IndexAttributeType_STRING,
				"a3": types.IndexAttributeType_BOOLEAN,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			val := make(map[string]interface{})
			decoder := json.NewDecoder(bytes.NewBuffer(tt.json))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&val))
			indexEntries := partialIndexEntriesForValue(reflect.ValueOf(val), tt.index)
			require.ElementsMatch(t, expectedIndexEntries, indexEntries)
		})
	}
}

func TestRemoveDuplicateIndexEntries(t *testing.T) {
	testCases := []struct {
		name                          string
		indexOfNewValues              []string
		indexOfExistingValues         []string
		expectedIndexOfNewValues      []string
		expectedIndexOfExistingValues []string
	}{
		{
			name: "no duplicates",
			indexOfNewValues: []string{
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person1"}`,
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person2"}`,
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person3"}`,
			},
			indexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person1"}`,
				`{"a":"age","t":0,"vp":2,"v":27,"kp":2,"k":"person2"}`,
				`{"a":"age","t":0,"vp":2,"v":28,"kp":2,"k":"person3"}`,
			},
			expectedIndexOfNewValues: []string{
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person1"}`,
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person2"}`,
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person3"}`,
			},
			expectedIndexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person1"}`,
				`{"a":"age","t":0,"vp":2,"v":27,"kp":2,"k":"person2"}`,
				`{"a":"age","t":0,"vp":2,"v":28,"kp":2,"k":"person3"}`,
			},
		},
		{
			name: "no duplicates as there is no existing value",
			indexOfNewValues: []string{
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person1"}`,
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person2"}`,
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person3"}`,
			},
			indexOfExistingValues: []string{},
			expectedIndexOfNewValues: []string{
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person1"}`,
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person2"}`,
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person3"}`,
			},
			expectedIndexOfExistingValues: []string{},
		},
		{
			name:             "no duplicates as the new value is empty",
			indexOfNewValues: []string{},
			indexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person1"}`,
				`{"a":"age","t":0,"vp":2,"v":27,"kp":2,"k":"person2"}`,
				`{"a":"age","t":0,"vp":2,"v":28,"kp":2,"k":"person3"}`,
			},
			expectedIndexOfNewValues: []string{},
			expectedIndexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person1"}`,
				`{"a":"age","t":0,"vp":2,"v":27,"kp":2,"k":"person2"}`,
				`{"a":"age","t":0,"vp":2,"v":28,"kp":2,"k":"person3"}`,
			},
		},
		{
			name: "two duplicate entries",
			indexOfNewValues: []string{
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person1"}`,
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person2"}`,
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person3"}`,
			},
			indexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person1"}`,
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person2"}`,
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person3"}`,
			},
			expectedIndexOfNewValues: []string{
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person1"}`,
			},
			expectedIndexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person1"}`,
			},
		},
		{
			name: "all are duplicate entries",
			indexOfNewValues: []string{
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person1"}`,
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person2"}`,
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person3"}`,
			},
			indexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person1"}`,
				`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person2"}`,
				`{"a":"age","t":0,"vp":2,"v":26,"kp":2,"k":"person3"}`,
			},
			expectedIndexOfNewValues:      []string{},
			expectedIndexOfExistingValues: []string{},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			indexOfNewValues, indexOfExistingValues := removeDuplicateIndexEntries(tt.indexOfNewValues, tt.indexOfExistingValues)
			require.ElementsMatch(t, tt.expectedIndexOfNewValues, indexOfNewValues)
			require.ElementsMatch(t, tt.expectedIndexOfExistingValues, indexOfExistingValues)
		})
	}
}

func TestLoadIndexEntry(t *testing.T) {
	tests := []struct {
		name               string
		indexEntry         []byte
		expectedIndexEntry *IndexEntry
		expectedErr        string
	}{
		{
			name:       "load correctly formatted full entry",
			indexEntry: []byte(`{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person1"}`),
			expectedIndexEntry: &IndexEntry{
				Attribute:     "age",
				Type:          0,
				ValuePosition: Existing,
				Value:         float64(25),
				KeyPosition:   Existing,
				Key:           "person1",
			},
		},
		{
			name:       "load correctly formatted partial entry",
			indexEntry: []byte(`{"a":"age","t":0,"vp":2,"v":25,"kp":2}`),
			expectedIndexEntry: &IndexEntry{
				Attribute:     "age",
				Type:          0,
				ValuePosition: Existing,
				Value:         float64(25),
				KeyPosition:   Existing,
				Key:           "",
			},
		},
		{
			name:               "error case",
			indexEntry:         []byte("abc"),
			expectedIndexEntry: nil,
			expectedErr:        "invalid character 'a' looking for beginning of value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &IndexEntry{}
			if tt.expectedErr == "" {
				require.NoError(t, e.Load(tt.indexEntry))
				require.Equal(t, tt.expectedIndexEntry, e)
			} else {
				err := e.Load(tt.indexEntry)
				require.EqualError(t, err, tt.expectedErr)
			}
		})
	}
}

func TestIndexEntryToString(t *testing.T) {
	tests := []struct {
		name               string
		indexEntry         *IndexEntry
		expectedIndexEntry string
		expectedErr        string
	}{
		{
			name: "load correctly formatted full entry",
			indexEntry: &IndexEntry{
				Attribute:     "age",
				Type:          0,
				ValuePosition: Existing,
				Value:         25,
				KeyPosition:   Existing,
				Key:           "person1",
			},
			expectedIndexEntry: `{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":"person1"}`,
		},
		{
			name: "load correctly formatted partial entry",
			indexEntry: &IndexEntry{
				Attribute:     "age",
				Type:          0,
				ValuePosition: Existing,
				Value:         25,
				KeyPosition:   Existing,
				Key:           "",
			},
			expectedIndexEntry: `{"a":"age","t":0,"vp":2,"v":25,"kp":2,"k":""}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, err := tt.indexEntry.String()
			require.NoError(t, err)
			require.Equal(t, tt.expectedIndexEntry, entry)
		})
	}
}

func TestOrderPreservingIndexingOfNumber(t *testing.T) {
	index := map[string]types.IndexAttributeType{
		"a1": types.IndexAttributeType_NUMBER,
	}
	indexJson, err := json.Marshal(index)
	require.NoError(t, err)

	createDBsWithIndex := map[string]*worldstate.DBUpdates{
		worldstate.DatabasesDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   "db1",
					Value: indexJson,
				},
				{
					Key: IndexDB("db1"),
				},
			},
		},
	}
	env := newIndexTestEnv(t)
	require.NoError(t, env.db.Commit(createDBsWithIndex, 1))

	updates := map[string]*worldstate.DBUpdates{
		"db1": {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   "p3",
					Value: []byte(`{"a1":-5}`),
				},
				{
					Key:   "p1",
					Value: []byte(`{"a1":-100}`),
				},
				{
					Key:   "p0",
					Value: []byte(`{"a1":-200}`),
				},
				{
					Key:   "p6",
					Value: []byte(`{"a1":15}`),
				},
				{
					Key:   "p2",
					Value: []byte(`{"a1":-10}`),
				},
				{
					Key:   "p5",
					Value: []byte(`{"a1":10}`),
				},
				{
					Key:   "p4",
					Value: []byte(`{"a1":0}`),
				},
				{
					Key:   "p7",
					Value: []byte(`{"a1":25}`),
				},
				{
					Key:   "p8",
					Value: []byte(`{"a1":-1}`),
				},
			},
		},
	}

	indexEntries, err := ConstructIndexEntries(updates, env.db)
	require.NoError(t, err)
	require.NoError(t, env.db.Commit(indexEntries, 2))

	tests := []struct {
		name        string
		start       *IndexEntry
		end         *IndexEntry
		expectedKVs map[string]int64
	}{
		{
			name: "fetch all positive numbers",
			start: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(0),
				KeyPosition:   Beginning,
			},
			end: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Ending,
			},
			expectedKVs: map[string]int64{
				"p4": 0,
				"p5": 10,
				"p6": 15,
				"p7": 25,
			},
		},
		{
			name: "fetch all positive keys till",
			start: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(0),
				KeyPosition:   Beginning,
			},
			end: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(15),
				KeyPosition:   Ending,
			},
			expectedKVs: map[string]int64{
				"p4": 0,
				"p5": 10,
				"p6": 15,
			},
		},
		{
			name: "fetch all positive keys from",
			start: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(10),
				KeyPosition:   Beginning,
			},
			end: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Ending,
			},
			expectedKVs: map[string]int64{
				"p5": 10,
				"p6": 15,
				"p7": 25,
			},
		},
		{
			name: "fetch all positive keys from and till",
			start: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(10),
				KeyPosition:   Beginning,
			},
			end: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(15),
				KeyPosition:   Ending,
			},
			expectedKVs: map[string]int64{
				"p5": 10,
				"p6": 15,
			},
		},
		{
			name: "fetch all negative numbers",
			start: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Beginning,
			},
			end: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(-1),
				KeyPosition:   Ending,
			},
			expectedKVs: map[string]int64{
				"p0": -200,
				"p1": -100,
				"p2": -10,
				"p3": -5,
				"p8": -1,
			},
		},
		{
			name: "fetch all negative keys till",
			start: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Beginning,
			},
			end: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(-10),
				KeyPosition:   Ending,
			},
			expectedKVs: map[string]int64{
				"p0": -200,
				"p1": -100,
				"p2": -10,
			},
		},
		{
			name: "fetch all negative keys from",
			start: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(-100),
				KeyPosition:   Beginning,
			},
			end: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(-1),
				KeyPosition:   Ending,
			},
			expectedKVs: map[string]int64{
				"p1": -100,
				"p2": -10,
				"p3": -5,
				"p8": -1,
			},
		},
		{
			name: "fetch all negative keys from and till",
			start: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(-100),
				KeyPosition:   Beginning,
			},
			end: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(-10),
				KeyPosition:   Ending,
			},
			expectedKVs: map[string]int64{
				"p1": -100,
				"p2": -10,
			},
		},
		{
			name: "fetch all negative and positive values",
			start: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Beginning,
			},
			end: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Ending,
			},
			expectedKVs: map[string]int64{
				"p0": -200,
				"p1": -100,
				"p2": -10,
				"p3": -5,
				"p8": -1,
				"p4": 0,
				"p5": 10,
				"p6": 15,
				"p7": 25,
			},
		},
		{
			name: "fetch some negative and all positive values",
			start: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(-100),
				KeyPosition:   Beginning,
			},
			end: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Ending,
			},
			expectedKVs: map[string]int64{
				"p1": -100,
				"p2": -10,
				"p3": -5,
				"p8": -1,
				"p4": 0,
				"p5": 10,
				"p6": 15,
				"p7": 25,
			},
		},
		{
			name: "fetch some negative and some positive values",
			start: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(-1),
				KeyPosition:   Beginning,
			},
			end: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(10),
				KeyPosition:   Ending,
			},
			expectedKVs: map[string]int64{
				"p8": -1,
				"p4": 0,
				"p5": 10,
			},
		},
		{
			name: "fetch all negative and some positive values",
			start: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Beginning,
			},
			end: &IndexEntry{
				Attribute:     "a1",
				Type:          types.IndexAttributeType_NUMBER,
				ValuePosition: Existing,
				Value:         EncodeInt64(0),
				KeyPosition:   Ending,
			},
			expectedKVs: map[string]int64{
				"p0": -200,
				"p1": -100,
				"p2": -10,
				"p3": -5,
				"p8": -1,
				"p4": 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			startKey, err := json.Marshal(tt.start)
			require.NoError(t, err)

			endKey, err := json.Marshal(tt.end)
			require.NoError(t, err)

			itr, err := env.db.GetIterator(IndexDB("db1"), string(startKey), string(endKey))
			require.NoError(t, err)

			kvs := make(map[string]int64)
			for itr.Next() {
				ie := &IndexEntry{}
				require.NoError(t, json.Unmarshal(itr.Key(), ie))

				var v int64
				vTemp, et, err := decodeVarUint64(ie.Value.(string))
				require.NoError(t, err)

				if et == normalOrder {
					v = int64(vTemp)
				} else {
					v = -int64(vTemp)
				}
				kvs[ie.Key] = v
			}
			require.Equal(t, tt.expectedKVs, kvs)
		})
	}
}
