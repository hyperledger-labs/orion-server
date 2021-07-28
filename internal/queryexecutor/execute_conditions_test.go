package queryexecutor

import (
	"encoding/json"
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/internal/stateindex"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func setupDBForTestingExecutes(t *testing.T, db worldstate.DB, dbName string) {
	indexDBName := stateindex.IndexDB(dbName)

	require.NoError(
		t,
		db.Commit(
			map[string]*worldstate.DBUpdates{
				worldstate.DatabasesDBName: {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key: indexDBName,
						},
					},
				},
			},
			1,
		),
	)

	indexEntries := map[string]map[interface{}][]string{
		"attr1": {
			"a": {"key1", "key2", "key3"},
			"b": {"key4", "key5"},
			"c": {"key6", "key7", "key8", "key9"},
			"d": {"key10", "key0"},
			"f": {"key11", "key13"},
			"i": {"key14"},
			"k": {"key15"},
			"m": {"key31"},
			"p": {"key16"},
			"x": {"key17", "key18", "key19"},
			"z": {"key20", "key21", "key22", "key23"},
		},
		"attr2": {
			true:  {"key1", "key2", "key3"},
			false: {"key4", "key5", "key11", "key21"},
		},
		"attr3": {
			"a1": {"key1", "key2", "key3"},
			"a2": {"key5", "key11", "key21"},
		},
	}

	dbUpdate := &worldstate.DBUpdates{}
	for attr, e := range indexEntries {
		for v, keys := range e {
			var ty types.Type
			switch v.(type) {
			case string:
				ty = types.Type_STRING
			case bool:
				ty = types.Type_BOOLEAN
			}

			for _, k := range keys {
				indexKey, err := json.Marshal(
					&stateindex.IndexEntry{
						Attribute:     attr,
						Type:          ty,
						Metadata:      "",
						ValuePosition: stateindex.Existing,
						Value:         v,
						KeyPosition:   stateindex.Existing,
						Key:           k,
					},
				)
				require.NoError(t, err)

				dbUpdate.Writes = append(
					dbUpdate.Writes,
					&worldstate.KVWithMetadata{
						Key: string(indexKey),
					},
				)
			}
		}
	}

	require.NoError(
		t,
		db.Commit(
			map[string]*worldstate.DBUpdates{
				indexDBName: dbUpdate,
			},
			2,
		),
	)
}

func TestExecuteAND(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	dbName := "testdb"
	setupDBForTestingExecutes(t, env.e.db, dbName)

	tests := []struct {
		name         string
		attrsConds   attributesConditions
		expectedKeys map[string]bool
	}{
		{
			name: "non-empty result",
			attrsConds: attributesConditions{
				"attr1": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "a",
						"$lt":  "f",
					},
				},
				"attr2": {
					valueType: types.Type_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": false,
					},
				},
				"attr3": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "a2",
					},
				},
			},
			expectedKeys: map[string]bool{
				"key5": true,
			},
		},
		{
			name: "more matches: non-empty result",
			attrsConds: attributesConditions{
				"attr1": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "a",
						"$lte": "f",
					},
				},
				"attr2": {
					valueType: types.Type_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": false,
					},
				},
				"attr3": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "a2",
					},
				},
			},
			expectedKeys: map[string]bool{
				"key5":  true,
				"key11": true,
			},
		},
		{
			name: "empty results",
			attrsConds: attributesConditions{
				"attr1": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "a",
						"$lte": "f",
					},
				},
				"attr2": {
					valueType: types.Type_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": true,
					},
				},
				"attr3": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "a2",
					},
				},
			},
			expectedKeys: nil,
		},
		{
			name: "minKeys is 0: empty results",
			attrsConds: attributesConditions{
				"attr1": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "a",
						"$lte": "f",
					},
				},
				"attr2": {
					valueType: types.Type_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": true,
					},
				},
				"attr3": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "a5",
					},
				},
			},
			expectedKeys: nil,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			keys, err := env.e.executeAND(dbName, tt.attrsConds)
			require.NoError(t, err)
			require.Equal(t, tt.expectedKeys, keys)
		})
	}
}

func TestExecuteOR(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	dbName := "testdb"
	setupDBForTestingExecutes(t, env.e.db, dbName)

	tests := []struct {
		name         string
		attrsConds   attributesConditions
		expectedKeys map[string]bool
	}{
		{
			name: "non-empty result",
			attrsConds: attributesConditions{
				"attr1": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "b",
						"$lt":  "c",
					},
				},
				"attr2": {
					valueType: types.Type_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": true,
					},
				},
				"attr3": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "a1",
						"$lt":  "a2",
					},
				},
			},
			expectedKeys: map[string]bool{
				"key4": true,
				"key5": true,
				"key1": true,
				"key2": true,
				"key3": true,
			},
		},
		{
			name: "more matches: non-empty result",
			attrsConds: attributesConditions{
				"attr1": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "a",
						"$lte": "c",
					},
				},
				"attr2": {
					valueType: types.Type_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": false,
					},
				},
				"attr3": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "a2",
					},
				},
			},
			expectedKeys: map[string]bool{
				"key1":  true,
				"key2":  true,
				"key3":  true,
				"key4":  true,
				"key5":  true,
				"key6":  true,
				"key7":  true,
				"key8":  true,
				"key9":  true,
				"key11": true,
				"key21": true,
			},
		},
		{
			name: "empty results",
			attrsConds: attributesConditions{
				"attr1": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "zaa",
					},
				},
				"attr3": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						"$gte": "a3",
					},
				},
			},
			expectedKeys: nil,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			keys, err := env.e.executeOR(dbName, tt.attrsConds)
			require.NoError(t, err)
			require.Equal(t, tt.expectedKeys, keys)
		})
	}
}

func TestExecute(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	dbName := "testdb"
	setupDBForTestingExecutes(t, env.e.db, dbName)

	tests := []struct {
		name         string
		attribute    string
		condition    *attrConditions
		expectedKeys []string
	}{
		{
			name:      "equal to b",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$eq": "b",
				},
			},
			expectedKeys: []string{"key4", "key5"},
		},
		{
			name:      "equal to z",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$eq": "z",
				},
			},
			expectedKeys: []string{"key20", "key21", "key22", "key23"},
		},
		{
			name:      "equal to abc",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$eq": "abc",
				},
			},
			expectedKeys: nil,
		},
		{
			name:      "equal to true",
			attribute: "attr2",
			condition: &attrConditions{
				valueType: types.Type_BOOLEAN,
				conditions: map[string]interface{}{
					"$eq": true,
				},
			},
			expectedKeys: []string{"key1", "key2", "key3"},
		},
		{
			name:      "equal to false",
			attribute: "attr2",
			condition: &attrConditions{
				valueType: types.Type_BOOLEAN,
				conditions: map[string]interface{}{
					"$eq": false,
				},
			},
			expectedKeys: []string{"key11", "key21", "key4", "key5"},
		},
		{
			name:      "greater than n",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$gt": "n",
				},
			},
			expectedKeys: []string{"key16", "key17", "key18", "key19", "key20", "key21", "key22", "key23"},
		},
		{
			name:      "greater than z",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$gt": "z",
				},
			},
			expectedKeys: nil,
		},
		{
			name:      "greater than or eq to z",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$gte": "z",
				},
			},
			expectedKeys: []string{"key20", "key21", "key22", "key23"},
		},
		{
			name:      "lesser than d",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$lt": "d",
				},
			},
			expectedKeys: []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9"},
		},
		{
			name:      "lesser than a",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$lt": "a",
				},
			},
			expectedKeys: nil,
		},
		{
			name:      "lesser than or eq to b",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$lte": "b",
				},
			},
			expectedKeys: []string{"key1", "key2", "key3", "key4", "key5"},
		},
		{
			name:      "greater than c and lesser than m",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$gt": "c",
					"$lt": "m",
				},
			},
			expectedKeys: []string{"key0", "key10", "key11", "key13", "key14", "key15"},
		},
		{
			name:      "greater than c and lesser than or equal to m",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$gt":  "c",
					"$lte": "m",
				},
			},
			expectedKeys: []string{"key0", "key10", "key11", "key13", "key14", "key15", "key31"},
		},
		{
			name:      "greater than or equal to c and lesser than or equal to m",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$gte": "c",
					"$lte": "m",
				},
			},
			expectedKeys: []string{"key6", "key7", "key8", "key9", "key0", "key10", "key11", "key13", "key14", "key15", "key31"},
		},
		{
			name:      "greater than or equal to c and lesser than",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$gte": "c",
					"$lt":  "m",
				},
			},
			expectedKeys: []string{"key6", "key7", "key8", "key9", "key0", "key10", "key11", "key13", "key14", "key15"},
		},
		{
			name:      "all values",
			attribute: "attr1",
			condition: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					"$gt": "",
					"$lt": "zzz",
				},
			},
			expectedKeys: []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key0", "key10", "key11", "key13", "key14", "key15", "key31", "key16", "key17", "key18", "key19", "key20", "key21", "key22", "key23"},
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			keys, err := env.e.execute(dbName, tt.attribute, tt.condition)
			require.NoError(t, err)
			expectedKeys := make(map[string]bool)
			for _, k := range tt.expectedKeys {
				expectedKeys[k] = true
			}
			require.Equal(t, expectedKeys, keys)
		})
	}
}
