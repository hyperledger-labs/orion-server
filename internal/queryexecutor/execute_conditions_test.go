package queryexecutor

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/hyperledger-labs/orion-server/internal/stateindex"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func setupDBForTestingExecutes(t *testing.T, db worldstate.DB, dbName string) {
	indexDef := map[string]types.IndexAttributeType{
		"attr1": types.IndexAttributeType_STRING,
		"attr2": types.IndexAttributeType_BOOLEAN,
		"attr3": types.IndexAttributeType_STRING,
		"attr4": types.IndexAttributeType_NUMBER,
	}
	marshaledIndexDef, err := json.Marshal(indexDef)
	require.NoError(t, err)

	indexDBName := stateindex.IndexDB(dbName)

	require.NoError(
		t,
		db.Commit(
			map[string]*worldstate.DBUpdates{
				worldstate.DatabasesDBName: {
					Writes: []*worldstate.KVWithMetadata{
						{
							Key:   dbName,
							Value: marshaledIndexDef,
						},
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
		"attr4": {
			int64(-210):   {"key3", "key4"},
			int64(-125):   {"key1", "key2"},
			int64(-50):    {"key5"},
			int64(-1):     {"key6", "key7"},
			int64(0):      {"key15", "key16", "key17"},
			int64(5):      {"key10", "key11"},
			int64(1234):   {"key8", "key9"},
			int64(2020):   {"key13", "key12"},
			int64(923421): {"key14"},
		},
	}

	dbUpdate := &worldstate.DBUpdates{}
	for attr, e := range indexEntries {
		for v, keys := range e {
			var ty types.IndexAttributeType
			switch v.(type) {
			case string:
				ty = types.IndexAttributeType_STRING
			case bool:
				ty = types.IndexAttributeType_BOOLEAN
			case int64:
				ty = types.IndexAttributeType_NUMBER
				v = stateindex.EncodeInt64(v.(int64))
			}

			for _, k := range keys {
				indexKey := &stateindex.IndexEntry{
					Attribute:     attr,
					Type:          ty,
					ValuePosition: stateindex.Existing,
					Value:         v,
					KeyPosition:   stateindex.Existing,
					Key:           k,
				}
				idx, err := indexKey.String()
				require.NoError(t, err)

				dbUpdate.Writes = append(
					dbUpdate.Writes,
					&worldstate.KVWithMetadata{
						Key: string(idx),
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
	setupDBForTestingExecutes(t, env.db, dbName)

	tests := []struct {
		name                string
		attrsConds          attributeToConditions
		useCancelledContext bool
		expectedKeys        map[string]bool
	}{
		{
			name: "non-empty result",
			attrsConds: attributeToConditions{
				"attr1": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a",
						"$lt":  "f",
					},
				},
				"attr2": {
					valueType: types.IndexAttributeType_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": false,
					},
				},
				"attr3": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a2",
					},
				},
				"attr4": {
					valueType: types.IndexAttributeType_NUMBER,
					conditions: map[string]interface{}{
						"$lt": stateindex.EncodeInt64(100),
					},
				},
			},
			useCancelledContext: false,
			expectedKeys: map[string]bool{
				"key5": true,
			},
		},
		{
			name: "empty result due to done context",
			attrsConds: attributeToConditions{
				"attr1": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a",
						"$lt":  "f",
					},
				},
				"attr2": {
					valueType: types.IndexAttributeType_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": false,
					},
				},
				"attr3": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a2",
					},
				},
			},
			useCancelledContext: true,
			expectedKeys:        nil,
		},
		{
			name: "more matches: non-empty result",
			attrsConds: attributeToConditions{
				"attr1": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a",
						"$lte": "f",
					},
				},
				"attr2": {
					valueType: types.IndexAttributeType_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": false,
					},
				},
				"attr3": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a2",
					},
				},
				"attr4": {
					valueType: types.IndexAttributeType_NUMBER,
					conditions: map[string]interface{}{
						"$gte": stateindex.EncodeInt64(-10),
						"$lte": stateindex.EncodeInt64(5),
					},
				},
			},
			useCancelledContext: false,
			expectedKeys: map[string]bool{
				"key11": true,
			},
		},
		{
			name: "empty results",
			attrsConds: attributeToConditions{
				"attr1": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a",
						"$lte": "f",
					},
				},
				"attr2": {
					valueType: types.IndexAttributeType_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": true,
					},
				},
				"attr3": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a2",
					},
				},
			},
			useCancelledContext: false,
			expectedKeys:        nil,
		},
		{
			name: "minKeys is 0: empty results",
			attrsConds: attributeToConditions{
				"attr1": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a",
						"$lte": "f",
					},
				},
				"attr2": {
					valueType: types.IndexAttributeType_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": true,
					},
				},
				"attr3": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a5",
					},
				},
			},
			useCancelledContext: false,
			expectedKeys:        nil,
		},
	}

	snapshots, err := env.db.GetDBsSnapshot([]string{worldstate.DatabasesDBName, stateindex.IndexDB(dbName)})
	require.NoError(t, err)
	defer snapshots.Release()

	qExecutor := NewWorldStateJSONQueryExecutor(snapshots, env.l)
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.useCancelledContext {
				cancel()
			}

			keys, err := qExecutor.executeAND(ctx, dbName, tt.attrsConds)
			require.NoError(t, err)
			require.Equal(t, tt.expectedKeys, keys)
		})
	}
}

func TestExecuteOR(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	dbName := "testdb"
	setupDBForTestingExecutes(t, env.db, dbName)

	tests := []struct {
		name                string
		attrsConds          attributeToConditions
		useCancelledContext bool
		expectedKeys        map[string]bool
	}{
		{
			name: "non-empty result",
			attrsConds: attributeToConditions{
				"attr1": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "b",
						"$lt":  "c",
					},
				},
				"attr2": {
					valueType: types.IndexAttributeType_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": true,
					},
				},
				"attr3": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a1",
						"$lt":  "a2",
					},
				},
				"attr4": {
					valueType: types.IndexAttributeType_NUMBER,
					conditions: map[string]interface{}{
						"$lt":  stateindex.EncodeInt64(0),
						"$gte": stateindex.EncodeInt64(-125),
					},
				},
			},
			useCancelledContext: false,
			expectedKeys: map[string]bool{
				"key4": true,
				"key5": true,
				"key1": true,
				"key2": true,
				"key3": true,
				"key6": true,
				"key7": true,
			},
		},
		{
			name: "empty result due to done context",
			attrsConds: attributeToConditions{
				"attr1": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "b",
						"$lt":  "c",
					},
				},
				"attr2": {
					valueType: types.IndexAttributeType_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": true,
					},
				},
				"attr3": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a1",
						"$lt":  "a2",
					},
				},
			},
			useCancelledContext: true,
			expectedKeys:        nil,
		},
		{
			name: "more matches: non-empty result",
			attrsConds: attributeToConditions{
				"attr1": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a",
						"$lte": "c",
					},
				},
				"attr2": {
					valueType: types.IndexAttributeType_BOOLEAN,
					conditions: map[string]interface{}{
						"$eq": false,
					},
				},
				"attr3": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a2",
					},
				},
				"attr4": {
					valueType: types.IndexAttributeType_NUMBER,
					conditions: map[string]interface{}{
						"$gte": stateindex.EncodeInt64(900000),
					},
				},
			},
			useCancelledContext: false,
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
				"key14": true,
			},
		},
		{
			name: "empty results",
			attrsConds: attributeToConditions{
				"attr1": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "zaa",
					},
				},
				"attr3": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						"$gte": "a3",
					},
				},
			},
			useCancelledContext: false,
			expectedKeys:        nil,
		},
	}

	snapshots, err := env.db.GetDBsSnapshot([]string{worldstate.DatabasesDBName, stateindex.IndexDB(dbName)})
	require.NoError(t, err)
	defer snapshots.Release()

	qExecutor := NewWorldStateJSONQueryExecutor(snapshots, env.l)
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.useCancelledContext {
				cancel()
			}

			keys, err := qExecutor.executeOR(ctx, dbName, tt.attrsConds)
			require.NoError(t, err)
			require.Equal(t, tt.expectedKeys, keys)
		})
	}
}

func TestExecuteOnly(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	dbName := "testdb"
	setupDBForTestingExecutes(t, env.db, dbName)

	tests := []struct {
		name                string
		attribute           string
		condition           *attributeTypeAndConditions
		useCancelledContext bool
		expectedKeys        []string
	}{
		{
			name:      "equal to b",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$eq": "b",
				},
			},
			useCancelledContext: false,
			expectedKeys:        []string{"key4", "key5"},
		},
		{
			name:      "equal to b but empty result due to done context",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$eq": "b",
				},
			},
			useCancelledContext: true,
			expectedKeys:        nil,
		},
		{
			name:      "equal to z",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$eq": "z",
				},
			},
			expectedKeys: []string{"key20", "key21", "key22", "key23"},
		},
		{
			name:      "equal to abc",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$eq": "abc",
				},
			},
			useCancelledContext: false,
			expectedKeys:        nil,
		},
		{
			name:      "equal to true",
			attribute: "attr2",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_BOOLEAN,
				conditions: map[string]interface{}{
					"$eq": true,
				},
			},
			expectedKeys: []string{"key1", "key2", "key3"},
		},
		{
			name:      "equal to false",
			attribute: "attr2",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_BOOLEAN,
				conditions: map[string]interface{}{
					"$eq": false,
				},
			},
			expectedKeys: []string{"key11", "key21", "key4", "key5"},
		},
		{
			name:      "equal to true and not equal to true",
			attribute: "attr2",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_BOOLEAN,
				conditions: map[string]interface{}{
					"$eq":  true,
					"$neq": []bool{true},
				},
			},
			expectedKeys: []string{},
		},
		{
			name:      "not eqaul to both true and false",
			attribute: "attr2",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_BOOLEAN,
				conditions: map[string]interface{}{
					"$neq": []bool{false, true},
				},
			},
			expectedKeys: []string{},
		},
		{
			name:      "equal to -125",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$eq": stateindex.EncodeInt64(-125),
				},
			},
			expectedKeys: []string{"key1", "key2"},
		},
		{
			name:      "equal to 923421",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$eq": stateindex.EncodeInt64(923421),
				},
			},
			expectedKeys: []string{"key14"},
		},
		{
			name:      "not eqaul to d",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$neq": []string{"d"},
				},
			},
			expectedKeys: []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key11", "key13", "key14", "key15", "key31", "key16", "key17", "key18", "key19", "key20", "key21", "key22", "key23"},
		},
		{
			name:      "not eqaul to a, b, c, d",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$neq": []string{"a", "b", "c", "d"},
				},
			},
			expectedKeys: []string{"key11", "key13", "key14", "key15", "key31", "key16", "key17", "key18", "key19", "key20", "key21", "key22", "key23"},
		},
		{
			name:      "not eqaul to false",
			attribute: "attr2",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_BOOLEAN,
				conditions: map[string]interface{}{
					"$neq": []bool{false},
				},
			},
			expectedKeys: []string{"key1", "key2", "key3"},
		},
		{
			name:      "not eqaul -50",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$neq": []string{stateindex.EncodeInt64(-50)},
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key6", "key7", "key15", "key16", "key17", "key10", "key11", "key8", "key9", "key13", "key12", "key14"},
		},
		{
			name:      "not eqaul -50, -1, 0, 5",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$neq": []string{stateindex.EncodeInt64(-50), stateindex.EncodeInt64(-1), stateindex.EncodeInt64(0), stateindex.EncodeInt64(5)},
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key8", "key9", "key13", "key12", "key14"},
		},
		{
			name:      "not eqaul 0",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$neq": []string{stateindex.EncodeInt64(0)},
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key5", "key6", "key7", "key10", "key11", "key8", "key9", "key13", "key12", "key14"},
		},
		{
			name:      "greater than n",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gt": "n",
				},
			},
			useCancelledContext: false,
			expectedKeys:        []string{"key16", "key17", "key18", "key19", "key20", "key21", "key22", "key23"},
		},
		{
			name:      "greater than n and not equal to x",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gt":  "n",
					"$neq": []string{"x"},
				},
			},
			expectedKeys: []string{"key16", "key20", "key21", "key22", "key23"},
		},
		{
			name:      "greater than z",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gt": "z",
				},
			},
			useCancelledContext: false,
			expectedKeys:        nil,
		},
		{
			name:      "greater than -125",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt": stateindex.EncodeInt64(-125),
				},
			},
			expectedKeys: []string{"key5", "key6", "key7", "key15", "key16", "key17", "key10", "key11", "key8", "key9", "key13", "key12", "key14"},
		},
		{
			name:      "greater than -125 and not equal to 0",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt":  stateindex.EncodeInt64(-125),
					"$neq": []string{stateindex.EncodeInt64(0)},
				},
			},
			expectedKeys: []string{"key5", "key6", "key7", "key10", "key11", "key8", "key9", "key13", "key12", "key14"},
		},
		{
			name:      "greater than 0",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt": stateindex.EncodeInt64(0),
				},
			},
			expectedKeys: []string{"key10", "key11", "key8", "key9", "key13", "key12", "key14"},
		},
		{
			name:      "greater than 0 and not equal to 2020",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt":  stateindex.EncodeInt64(0),
					"$neq": []string{stateindex.EncodeInt64(2020)},
				},
			},
			expectedKeys: []string{"key10", "key11", "key8", "key9", "key14"},
		},
		{
			name:      "greater than or eq to z",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gte": "z",
				},
			},
			useCancelledContext: false,
			expectedKeys:        []string{"key20", "key21", "key22", "key23"},
		},
		{
			name:      "greater than or equal to -125",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(-125),
				},
			},
			expectedKeys: []string{"key1", "key2", "key5", "key6", "key7", "key15", "key16", "key17", "key10", "key11", "key8", "key9", "key13", "key12", "key14"},
		},
		{
			name:      "greater than or equal to -125 and not eqaul to -125",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(-125),
					"$neq": []string{stateindex.EncodeInt64(-125)},
				},
			},
			expectedKeys: []string{"key5", "key6", "key7", "key15", "key16", "key17", "key10", "key11", "key8", "key9", "key13", "key12", "key14"},
		},
		{
			name:      "greater than or equal to 0",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(0),
				},
			},
			expectedKeys: []string{"key15", "key16", "key17", "key10", "key11", "key8", "key9", "key13", "key12", "key14"},
		},
		{
			name:      "lesser than d",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$lt": "d",
				},
			},
			useCancelledContext: false,
			expectedKeys:        []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9"},
		},
		{
			name:      "lesser than a",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$lt": "a",
				},
			},
			useCancelledContext: false,
			expectedKeys:        nil,
		},
		{
			name:      "lesser than -125",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$lt": stateindex.EncodeInt64(-125),
				},
			},
			expectedKeys: []string{"key3", "key4"},
		},
		{
			name:      "lesser than 2020",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$lt": stateindex.EncodeInt64(2020),
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key5", "key6", "key7", "key15", "key16", "key17", "key10", "key11", "key8", "key9"},
		},
		{
			name:      "lesser than or eq to b",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$lte": "b",
				},
			},
			useCancelledContext: false,
			expectedKeys:        []string{"key1", "key2", "key3", "key4", "key5"},
		},
		{
			name:      "lesser than or equal to -125",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$lte": stateindex.EncodeInt64(-125),
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2"},
		},
		{
			name:      "lesser than or equal to 2020",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$lte": stateindex.EncodeInt64(2020),
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key5", "key6", "key7", "key15", "key16", "key17", "key10", "key11", "key8", "key9", "key13", "key12"},
		},
		{
			name:      "greater than c and lesser than m",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gt": "c",
					"$lt": "m",
				},
			},
			useCancelledContext: false,
			expectedKeys:        []string{"key0", "key10", "key11", "key13", "key14", "key15"},
		},
		{
			name:      "greater than c and lesser than or equal to m",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gt":  "c",
					"$lte": "m",
				},
			},
			useCancelledContext: false,
			expectedKeys:        []string{"key0", "key10", "key11", "key13", "key14", "key15", "key31"},
		},
		{
			name:      "greater than or equal to c and lesser than or equal to m",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gte": "c",
					"$lte": "m",
				},
			},
			useCancelledContext: false,
			expectedKeys:        []string{"key6", "key7", "key8", "key9", "key0", "key10", "key11", "key13", "key14", "key15", "key31"},
		},
		{
			name:      "greater than or equal to c and lesser than",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gte": "c",
					"$lt":  "m",
				},
			},
			useCancelledContext: false,
			expectedKeys:        []string{"key6", "key7", "key8", "key9", "key0", "key10", "key11", "key13", "key14", "key15"},
		},
		{
			name:      "greater than -125 and lesser than 1234",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt": stateindex.EncodeInt64(-125),
					"$lt": stateindex.EncodeInt64(1234),
				},
			},
			expectedKeys: []string{"key5", "key6", "key7", "key15", "key16", "key17", "key10", "key11"},
		},
		{
			name:      "greater than or equal to -125 and lesser than 1234",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(-125),
					"$lt":  stateindex.EncodeInt64(1234),
				},
			},
			expectedKeys: []string{"key1", "key2", "key5", "key6", "key7", "key15", "key16", "key17", "key10", "key11"},
		},
		{
			name:      "greater than -125 and lesser than or equal to 1234",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt":  stateindex.EncodeInt64(-125),
					"$lte": stateindex.EncodeInt64(1234),
				},
			},
			expectedKeys: []string{"key5", "key6", "key7", "key15", "key16", "key17", "key10", "key11", "key8", "key9"},
		},
		{
			name:      "greater than or equal to -125 and lesser than or equal to 1234",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(-125),
					"$lte": stateindex.EncodeInt64(1234),
				},
			},
			expectedKeys: []string{"key1", "key2", "key5", "key6", "key7", "key15", "key16", "key17", "key10", "key11", "key8", "key9"},
		},
		{
			name:      "greater than -500 and lesser than -2",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt": stateindex.EncodeInt64(-500),
					"$lt": stateindex.EncodeInt64(-2),
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key5"},
		},
		{
			name:      "greater than or equal to -210 and lesser than -1",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(-210),
					"$lt":  stateindex.EncodeInt64(-1),
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key5"},
		},
		{
			name:      "greater than -210 and lesser than or equal to -1",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt":  stateindex.EncodeInt64(-210),
					"$lte": stateindex.EncodeInt64(-1),
				},
			},
			expectedKeys: []string{"key1", "key2", "key5", "key6", "key7"},
		},
		{
			name:      "greater than or equal to -210 and lesser than or equal to -1",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(-210),
					"$lte": stateindex.EncodeInt64(-1),
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key5", "key6", "key7"},
		},
		{
			name:      "greater than 0 and lesser than 2020",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt": stateindex.EncodeInt64(0),
					"$lt": stateindex.EncodeInt64(2020),
				},
			},
			expectedKeys: []string{"key10", "key11", "key8", "key9"},
		},
		{
			name:      "greater than or equal to 0 and lesser than 2020",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(0),
					"$lt":  stateindex.EncodeInt64(2020),
				},
			},
			expectedKeys: []string{"key15", "key16", "key17", "key10", "key11", "key8", "key9"},
		},
		{
			name:      "greater than 0 and lesser than or equal to 2020",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt":  stateindex.EncodeInt64(0),
					"$lte": stateindex.EncodeInt64(2020),
				},
			},
			expectedKeys: []string{"key10", "key11", "key8", "key9", "key13", "key12"},
		},
		{
			name:      "greater than or equal to 0 and lesser than or equal to 2020",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(0),
					"$lte": stateindex.EncodeInt64(2020),
				},
			},
			expectedKeys: []string{"key15", "key16", "key17", "key10", "key11", "key8", "key9", "key13", "key12"},
		},
		{
			name:      "all values",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gt": "",
					"$lt": "zzz",
				},
			},
			useCancelledContext: false,
			expectedKeys:        []string{"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9", "key0", "key10", "key11", "key13", "key14", "key15", "key31", "key16", "key17", "key18", "key19", "key20", "key21", "key22", "key23"},
		},
		{
			name:      "greater than or equal to 0 and not eqaul to 0",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(0),
					"$neq": []string{stateindex.EncodeInt64(0)},
				},
			},
			expectedKeys: []string{"key10", "key11", "key8", "key9", "key13", "key12", "key14"},
		},
		{
			name:      "lesser than d and not eqaul to b",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$lt":  "d",
					"$neq": []string{"b"},
				},
			},
			expectedKeys: []string{"key1", "key2", "key3", "key6", "key7", "key8", "key9"},
		},
		{
			name:      "lesser than 2020 and not eqaul to",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$lt":  stateindex.EncodeInt64(2020),
					"$neq": []string{stateindex.EncodeInt64(-50)},
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key6", "key7", "key15", "key16", "key17", "key10", "key11", "key8", "key9"},
		},
		{
			name:      "lesser than or eq to b and not eqaul to a",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$lte": "b",
					"$neq": []string{"a"},
				},
			},
			expectedKeys: []string{"key4", "key5"},
		},
		{
			name:      "lesser than or equal to -125 and not eqaul to -210",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$lte": stateindex.EncodeInt64(-125),
					"$neq": []string{stateindex.EncodeInt64(-210)},
				},
			},
			expectedKeys: []string{"key1", "key2"},
		},
		{
			name:      "lesser than or equal to 2020 and not eqaul to -1",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$lte": stateindex.EncodeInt64(2020),
					"$neq": []string{stateindex.EncodeInt64(-1)},
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key5", "key15", "key16", "key17", "key10", "key11", "key8", "key9", "key13", "key12"},
		},
		{
			name:      "greater than c and lesser than m and not eqaul to i",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gt":  "c",
					"$lt":  "m",
					"$neq": []string{"i"},
				},
			},
			expectedKeys: []string{"key0", "key10", "key11", "key13", "key15"},
		},
		{
			name:      "greater than c and lesser than or equal to m and not eqaul to a",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gt":  "c",
					"$lte": "m",
					"$neq": []string{"a"},
				},
			},
			expectedKeys: []string{"key0", "key10", "key11", "key13", "key14", "key15", "key31"},
		},
		{
			name:      "greater than c and lesser than or equal to m and not eqaul to d, f, i",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gt":  "c",
					"$lte": "m",
					"$neq": []string{"d", "f", "i"},
				},
			},
			expectedKeys: []string{"key15", "key31"},
		},
		{
			name:      "greater than c and lesser than or equal to m and not eqaul to k",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gt":  "c",
					"$lte": "m",
					"$neq": []string{"k"},
				},
			},
			expectedKeys: []string{"key0", "key10", "key11", "key13", "key14", "key31"},
		},
		{
			name:      "greater than or equal to c and lesser than or equal to m and not eqaul to m",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gte": "c",
					"$lte": "m",
					"$neq": []string{"m"},
				},
			},
			expectedKeys: []string{"key6", "key7", "key8", "key9", "key0", "key10", "key11", "key13", "key14", "key15"},
		},
		{
			name:      "greater than -125 and lesser than 1234 and not eqaul to 5",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt":  stateindex.EncodeInt64(-125),
					"$lt":  stateindex.EncodeInt64(1234),
					"$neq": []string{stateindex.EncodeInt64(5)},
				},
			},
			expectedKeys: []string{"key5", "key6", "key7", "key15", "key16", "key17"},
		},
		{
			name:      "greater than or equal to -125 and lesser than 1234 and not eqaul to 0",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(-125),
					"$lt":  stateindex.EncodeInt64(1234),
					"$neq": []string{stateindex.EncodeInt64(0)},
				},
			},
			expectedKeys: []string{"key1", "key2", "key5", "key6", "key7", "key10", "key11"},
		},
		{
			name:      "greater than or equal to -125 and lesser than or equal to 1234 and not eqaul to -50",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(-125),
					"$lte": stateindex.EncodeInt64(1234),
					"$neq": []string{stateindex.EncodeInt64(-50)},
				},
			},
			expectedKeys: []string{"key1", "key2", "key6", "key7", "key15", "key16", "key17", "key10", "key11", "key8", "key9"},
		},
		{
			name:      "greater than -500 and lesser than -2 and not eqaul to -200",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt":  stateindex.EncodeInt64(-500),
					"$lt":  stateindex.EncodeInt64(-2),
					"$neq": []string{stateindex.EncodeInt64(-200)},
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key5"},
		},
		{
			name:      "greater than -500 and lesser than -2 and not eqaul to 50",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt":  stateindex.EncodeInt64(-500),
					"$lt":  stateindex.EncodeInt64(-2),
					"$neq": []string{stateindex.EncodeInt64(50)},
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key5"},
		},
		{
			name:      "greater than or equal to -210 and lesser than or equal to -1 and not equal to -50",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(-210),
					"$lte": stateindex.EncodeInt64(-1),
					"$neq": []string{stateindex.EncodeInt64(-50)},
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key6", "key7"},
		},
		{
			name:      "greater than 0 and lesser than 2020 and not eqaul to -50",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt":  stateindex.EncodeInt64(0),
					"$lt":  stateindex.EncodeInt64(2020),
					"$neq": []string{stateindex.EncodeInt64(-50)},
				},
			},
			expectedKeys: []string{"key10", "key11", "key8", "key9"},
		},
		{
			name:      "empty values",
			attribute: "attr1",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					"$gt": "zzz",
					"$lt": "",
				},
			},
			useCancelledContext: false,
			expectedKeys:        []string{},
		},
		{
			name:      "all values - number",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gte": stateindex.EncodeInt64(-10000),
				},
			},
			expectedKeys: []string{"key3", "key4", "key1", "key2", "key5", "key6", "key7", "key15", "key16", "key17", "key10", "key11", "key8", "key9", "key13", "key12", "key14"},
		},
		{
			name:      "empty values number",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt": stateindex.EncodeInt64(10000000),
				},
			},
			expectedKeys: []string{},
		},
		{
			name:      "empty results for gt 0 and lt -1",
			attribute: "attr4",
			condition: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					"$gt": stateindex.EncodeInt64(0),
					"$lt": stateindex.EncodeInt64(-1),
				},
			},
			expectedKeys: []string{},
		},
	}

	snapshots, err := env.db.GetDBsSnapshot([]string{worldstate.DatabasesDBName, stateindex.IndexDB(dbName)})
	require.NoError(t, err)
	defer snapshots.Release()

	qExecutor := NewWorldStateJSONQueryExecutor(snapshots, env.l)
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.useCancelledContext {
				cancel()
			}

			keys, err := qExecutor.execute(ctx, dbName, tt.attribute, tt.condition)
			require.NoError(t, err)

			if tt.useCancelledContext {
				require.Nil(t, keys)
				require.Nil(t, tt.expectedKeys)
				return
			}

			expectedKeys := make(map[string]bool)
			for _, k := range tt.expectedKeys {
				expectedKeys[k] = true
			}
			require.Equal(t, expectedKeys, keys)
		})
	}
}

func TestIntersectionWithContext(t *testing.T) {
	tests := []struct {
		name                string
		attrToKeys          map[string]map[string]bool
		useCancelledContext bool
		expectedKeys        map[string]bool
	}{
		{
			name: "non-empty result",
			attrToKeys: map[string]map[string]bool{
				"attr1": {
					"key1": true,
					"key2": true,
				},
				"attr2": {
					"key2": true,
				},
			},
			useCancelledContext: false,
			expectedKeys: map[string]bool{
				"key2": true,
			},
		},
		{
			name: "empty result as the context is done",
			attrToKeys: map[string]map[string]bool{
				"attr1": {
					"key1": true,
					"key2": true,
				},
				"attr2": {
					"key2": true,
				},
			},
			useCancelledContext: true,
			expectedKeys:        nil,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.useCancelledContext {
				cancel()
			}

			keys := intersection(ctx, tt.attrToKeys)
			require.Equal(t, tt.expectedKeys, keys)
		})
	}
}

func TestUnionWithContext(t *testing.T) {
	tests := []struct {
		name                string
		attrToKeys          map[string]map[string]bool
		useCancelledContext bool
		expectedKeys        map[string]bool
	}{
		{
			name: "non-empty result",
			attrToKeys: map[string]map[string]bool{
				"attr1": {
					"key1": true,
					"key2": true,
				},
				"attr2": {
					"key2": true,
				},
			},
			useCancelledContext: false,
			expectedKeys: map[string]bool{
				"key1": true,
				"key2": true,
			},
		},
		{
			name: "empty result as the context is done",
			attrToKeys: map[string]map[string]bool{
				"attr1": {
					"key1": true,
					"key2": true,
				},
				"attr2": {
					"key2": true,
				},
			},
			useCancelledContext: true,
			expectedKeys:        nil,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.useCancelledContext {
				cancel()
			}

			keys := union(ctx, tt.attrToKeys)
			require.Equal(t, tt.expectedKeys, keys)
		})
	}
}
