// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package stateindex

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate/leveldb"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
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

	dir, err := ioutil.TempDir("", "index")
	require.NoError(t, err)

	dbPath := filepath.Join(dir, "leveldb")
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: dbPath,
			Logger:    logger,
		},
	)
	if err != nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			t.Errorf("error while removing directory %s, %v", dir, rmErr)
		}
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
	indexDB1 := map[string]types.Type{
		"a1": types.Type_NUMBER,
		"a2": types.Type_STRING,
		"a3": types.Type_BOOLEAN,
	}
	indexDB1Json, err := json.Marshal(indexDB1)
	require.NoError(t, err)

	indexDB2 := map[string]types.Type{
		"a2": types.Type_STRING,
	}
	indexDB2Json, err := json.Marshal(indexDB2)
	require.NoError(t, err)

	encoded10 := base64.URLEncoding.EncodeToString(encodeOrderPreservingVarUint64(uint64(10)))
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
							Key: `{"a":"a1","t":0,"m":"` + positiveNumber + `","vp":1,"v":"` + encoded10 + `","kp":1,"k":"person1"}`,
						},
						{
							Key: `{"a":"a2","t":1,"m":"","vp":1,"v":"ten","kp":1,"k":"person1"}`,
						},
						{
							Key: `{"a":"a3","t":2,"m":"","vp":1,"v":true,"kp":1,"k":"person1"}`,
						},
					},
				},
				IndexDB("db2"): {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key: `{"a":"a2","t":1,"m":"","vp":1,"v":"eleven","kp":1,"k":"person2"}`,
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
							Key: `{"a":"a3","t":2,"m":"","vp":1,"v":true,"kp":1,"k":"person1"}`,
						},
						{
							Key: `{"a":"a2","t":1,"m":"","vp":1,"v":"10","kp":1,"k":"person1"}`,
						},
					},
					Deletes: []string{
						`{"a":"a3","t":2,"m":"","vp":1,"v":false,"kp":1,"k":"person1"}`,
						`{"a":"a2","t":1,"m":"","vp":1,"v":"ten","kp":1,"k":"person1"}`,
					},
				},
				IndexDB("db2"): {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key: `{"a":"a2","t":1,"m":"","vp":1,"v":"eleven","kp":1,"k":"person2"}`,
						},
					},
					Deletes: []string{
						`{"a":"a2","t":1,"m":"","vp":1,"v":"ten","kp":1,"k":"person2"}`,
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
						`{"a":"a1","t":0,"m":"` + positiveNumber + `","vp":1,"v":"` + encoded10 + `","kp":1,"k":"person1"}`,
						`{"a":"a2","t":1,"m":"","vp":1,"v":"ten","kp":1,"k":"person1"}`,
						`{"a":"a3","t":2,"m":"","vp":1,"v":true,"kp":1,"k":"person1"}`,
					},
				},
				IndexDB("db2"): {
					Deletes: []string{
						`{"a":"a2","t":1,"m":"","vp":1,"v":"eleven","kp":1,"k":"person2"}`,
					},
				},
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			env := newIndexTestEnv(t)
			tt.setup(env.db)
			indexEntries, err := constructIndexEntries(tt.updates, env.db)
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
	indexDef := map[string]types.Type{
		"age": types.Type_NUMBER,
	}

	encoded25 := encodeOrderPreservingVarUint64(uint64(25))
	encoded26 := encodeOrderPreservingVarUint64(uint64(26))

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
					Type:          types.Type_NUMBER,
					Metadata:      positiveNumber,
					ValuePosition: Existing,
					Value:         encoded25,
					KeyPosition:   Existing,
					Key:           "person1",
				},
				{
					Attribute:     "age",
					Type:          types.Type_NUMBER,
					Metadata:      positiveNumber,
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
	indexDef := map[string]types.Type{
		"age": types.Type_NUMBER,
	}

	encoded25 := encodeOrderPreservingVarUint64(uint64(25))
	encodedNegative26 := encodeReverseOrderVarUint64(uint64(26))
	encoded0 := encodeOrderPreservingVarUint64(uint64(0))
	encodedMax := encodeOrderPreservingVarUint64(math.MaxInt64)

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
					Type:          types.Type_NUMBER,
					Metadata:      positiveNumber,
					ValuePosition: Existing,
					Value:         encoded25,
					KeyPosition:   Existing,
					Key:           "person1",
				},
				{
					Attribute:     "age",
					Type:          types.Type_NUMBER,
					Metadata:      negativeNumber,
					ValuePosition: Existing,
					Value:         encodedNegative26,
					KeyPosition:   Existing,
					Key:           "person2",
				},
				{
					Attribute:     "age",
					Type:          types.Type_NUMBER,
					Metadata:      positiveNumber,
					ValuePosition: Existing,
					Value:         encoded0,
					KeyPosition:   Existing,
					Key:           "person3",
				},
				{
					Attribute:     "age",
					Type:          types.Type_NUMBER,
					Metadata:      positiveNumber,
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
	encoded10 := encodeOrderPreservingVarUint64(uint64(10))
	expectedIndexEntries :=
		[]*IndexEntry{
			{
				Attribute:     "a1",
				Type:          types.Type_NUMBER,
				Metadata:      positiveNumber,
				ValuePosition: Existing,
				Value:         encoded10,
				KeyPosition:   Existing,
			},
			{
				Attribute:     "a2",
				Type:          types.Type_STRING,
				ValuePosition: Existing,
				Value:         "female",
				KeyPosition:   Existing,
			},
			{
				Attribute:     "a3",
				Type:          types.Type_BOOLEAN,
				ValuePosition: Existing,
				Value:         true,
				KeyPosition:   Existing,
			},
		}

	testCases := []struct {
		name  string
		json  []byte
		index map[string]types.Type
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
			index: map[string]types.Type{
				"a1": types.Type_NUMBER,
				"a2": types.Type_STRING,
				"a3": types.Type_BOOLEAN,
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
			index: map[string]types.Type{
				"a1": types.Type_NUMBER,
				"a2": types.Type_STRING,
				"a3": types.Type_BOOLEAN,
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
			index: map[string]types.Type{
				"a1": types.Type_NUMBER,
				"a2": types.Type_STRING,
				"a3": types.Type_BOOLEAN,
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
			index: map[string]types.Type{
				"a1": types.Type_NUMBER,
				"a2": types.Type_STRING,
				"a3": types.Type_BOOLEAN,
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
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person1"}`,
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person2"}`,
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person3"}`,
			},
			indexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person1"}`,
				`{"a":"age","t":0,"vp":1,"v":27,"kp":1,"k":"person2"}`,
				`{"a":"age","t":0,"vp":1,"v":28,"kp":1,"k":"person3"}`,
			},
			expectedIndexOfNewValues: []string{
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person1"}`,
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person2"}`,
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person3"}`,
			},
			expectedIndexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person1"}`,
				`{"a":"age","t":0,"vp":1,"v":27,"kp":1,"k":"person2"}`,
				`{"a":"age","t":0,"vp":1,"v":28,"kp":1,"k":"person3"}`,
			},
		},
		{
			name: "no duplicates as there is no existing value",
			indexOfNewValues: []string{
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person1"}`,
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person2"}`,
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person3"}`,
			},
			indexOfExistingValues: []string{},
			expectedIndexOfNewValues: []string{
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person1"}`,
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person2"}`,
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person3"}`,
			},
			expectedIndexOfExistingValues: []string{},
		},
		{
			name:             "no duplicates as the new value is empty",
			indexOfNewValues: []string{},
			indexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person1"}`,
				`{"a":"age","t":0,"vp":1,"v":27,"kp":1,"k":"person2"}`,
				`{"a":"age","t":0,"vp":1,"v":28,"kp":1,"k":"person3"}`,
			},
			expectedIndexOfNewValues: []string{},
			expectedIndexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person1"}`,
				`{"a":"age","t":0,"vp":1,"v":27,"kp":1,"k":"person2"}`,
				`{"a":"age","t":0,"vp":1,"v":28,"kp":1,"k":"person3"}`,
			},
		},
		{
			name: "two duplicate entries",
			indexOfNewValues: []string{
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person1"}`,
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person2"}`,
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person3"}`,
			},
			indexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person1"}`,
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person2"}`,
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person3"}`,
			},
			expectedIndexOfNewValues: []string{
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person1"}`,
			},
			expectedIndexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person1"}`,
			},
		},
		{
			name: "all are duplicate entries",
			indexOfNewValues: []string{
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person1"}`,
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person2"}`,
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person3"}`,
			},
			indexOfExistingValues: []string{
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person1"}`,
				`{"a":"age","t":0,"vp":1,"v":25,"kp":1,"k":"person2"}`,
				`{"a":"age","t":0,"vp":1,"v":26,"kp":1,"k":"person3"}`,
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
