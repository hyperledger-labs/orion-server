package queryexecutor

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/hyperledger-labs/orion-server/internal/stateindex"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/internal/worldstate/leveldb"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

type testEnv struct {
	db      worldstate.DB
	l       *logger.SugarLogger
	cleanup func()
}

func newTestEnv(t *testing.T) *testEnv {
	l, err := logger.New(
		&logger.Config{
			Level:         "debug",
			OutputPath:    []string{os.Stdout.Name()},
			ErrOutputPath: []string{os.Stderr.Name()},
			Encoding:      "console",
			Name:          "queryexecutor",
		},
	)
	require.NoError(t, err)

	tempDir := t.TempDir()
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: tempDir,
			Logger:    l,
		},
	)
	require.NoError(t, err)

	return &testEnv{
		db: db,
		l:  l,
		cleanup: func() {
			if err := db.Close(); err != nil {
				t.Log("error while closing the database: [" + err.Error() + "]")
			}
		},
	}
}

func TestExecuteJSONQuery(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	dbName := "testdb"
	setupDBForTestingExecutes(t, env.db, dbName)

	tests := []struct {
		name                string
		query               []byte
		useCancelledContext bool
		expectedKeys        map[string]bool
	}{
		{
			name: "neither and nor or is set",
			query: []byte(
				`{
					"selector": {
						"attr1": {
							"$gte": "a",
							"$lt": "b"
						},
						"attr2": {
							"$eq": true
						},
						"attr3": {
							"$lt": "a2"
						},
						"attr4": {
							"$lt": -125
						}
					}
				}`,
			),
			useCancelledContext: false,
			expectedKeys: map[string]bool{
				"key3": true,
			},
		},
		{
			name: "and is set",
			query: []byte(
				`{
					"selector": {
						"$and": {
							"attr1": {
								"$gte": "a",
								"$lt": "b"
							},
							"attr2": {
								"$eq": true
							},
							"attr3": {
								"$lt": "a2"
							},
							"attr4": {
								"$lte": -125,
								"$neq": [-210]
							}
						}
					}
				}`,
			),
			useCancelledContext: false,
			expectedKeys: map[string]bool{
				"key1": true,
				"key2": true,
			},
		},
		{
			name: "and is set and the context is done",
			query: []byte(
				`{
					"selector": {
						"$and": {
							"attr1": {
								"$gte": "a",
								"$lt": "b"
							},
							"attr2": {
								"$eq": true
							},
							"attr3": {
								"$lt": "a2"
							}
						}
					}
				}`,
			),
			useCancelledContext: true,
			expectedKeys:        nil,
		},
		{
			name: "or is set",
			query: []byte(
				`{
					"selector": {
						"$or": {
							"attr1": {
								"$gte": "b",
								"$neq": ["c", "d", "e"],
								"$lt": "f"
							},
							"attr2": {
								"$neq": [true]
							},
							"attr3": {
								"$gte": "a2"
							},
							"attr4": {
								"$gte": -5,
								"$neq": [0, 4, 5],
								"$lte": 6
							}
						}
					}
				}`,
			),
			useCancelledContext: false,
			expectedKeys: map[string]bool{
				"key4":  true,
				"key5":  true,
				"key11": true,
				"key21": true,
				"key6":  true,
				"key7":  true,
			},
		},
		{
			name: "or is set and the context is done",
			query: []byte(
				`{
					"selector": {
						"$or": {
							"attr1": {
								"$gte": "b",
								"$lt": "c"
							},
							"attr2": {
								"$eq": false
							},
							"attr3": {
								"$gte": "a2"
							}
						}
					}
				}`,
			),
			useCancelledContext: true,
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

			keys, err := qExecutor.ExecuteQuery(ctx, dbName, tt.query)
			require.NoError(t, err)
			require.Equal(t, tt.expectedKeys, keys)
		})
	}
}

func TestExecuteJSONQueryErrorCases(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	dbName := "testdb"
	setupDBForTestingExecutes(t, env.db, dbName)

	tests := []struct {
		name          string
		query         []byte
		expectedError string
	}{
		{
			name: "both and - or are set",
			query: []byte(
				`{
					"selector": {
						"$and": {
							"attr1": {
								"$gte": "a",
								"$lt": "b"
							},
							"attr2": {
								"$eq": true
							}
						},
						"$or": {
							"attr3": {
								"$lt": "a2"
							},
							"attr2": {
								"$eq": true
							}
						}
					}
				}`,
			),
			expectedError: "there must be a single upper level combination operator",
		},
		{
			name: "extra commas: query syntax error",
			query: []byte(
				`{
					"selector": {
						"$and": {
							"attr1": {
								"$gte": "a",
								"$lt": "b",
							},
						}
					}
				}`,
			),
			expectedError: "error decoding the query",
		},
		{
			name: "and is not with correct syntax",
			query: []byte(
				`{
					"selector": {
						"$and": [
							{
								"attr1": "bc"
							},
							{
								"attr2": "bc"
							}
						]
					}
				}`,
			),
			expectedError: "query syntax error near $and",
		},
		{
			name: "or is not with correct syntax",
			query: []byte(
				`{
					"selector": {
						"$or": [
							{
								"attr1": "bc"
							},
							{
								"attr2": "bc"
							}
						]
					}
				}`,
			),
			expectedError: "query syntax error near $or",
		},
		{
			name: "attribute used in and is not indexed",
			query: []byte(
				`{
					"selector": {
						"$and": {
							"attr5": {
								"$eq": true
							}
						}
					}
				}`,
			),
			expectedError: "attribute [attr5] given in the query condition is not indexed",
		},
		{
			name: "attribute used in or is not indexed",
			query: []byte(
				`{
					"selector": {
						"$or": {
							"attr5": {
								"$eq": true
							}
						}
					}
				}`,
			),
			expectedError: "attribute [attr5] given in the query condition is not indexed",
		},
		{
			name: "attribute used in default combination is not indexed",
			query: []byte(
				`{
					"selector": {
						"attr5": {
							"$eq": true
						}
					}
				}`,
			),
			expectedError: "attribute [attr5] given in the query condition is not indexed",
		},
		{
			name: "selector field is missing",
			query: []byte(
				`{
					"attr5": {
						"$eq": true
					}
				}`,
			),
			expectedError: "selector field is missing in the query",
		},
		{
			name: "selector filed is empty",
			query: []byte(
				`{
					"selector":{
					}
				}`,
			),
			expectedError: "query conditions cannot be empty",
		},
		{
			name: "attribute has no condition",
			query: []byte(
				`{
					"selector": {
						"attr1": {
						}
					}
				}`,
			),
			expectedError: "no condition provided for the attribute [attr1]. All given attributes must have a condition",
		},
		{
			name: "no slice for neq",
			query: []byte(
				`{
					"selector": {
						"$and": {
							"attr1": {
								"$gte": "a",
								"$lt": "b"
							},
							"attr2": {
								"$neq": true
							}
						}
					}
				}`,
			),
			expectedError: "array should be used for $neq condition",
		},
	}

	snapshots, err := env.db.GetDBsSnapshot([]string{worldstate.DatabasesDBName, stateindex.IndexDB(dbName)})
	require.NoError(t, err)
	defer snapshots.Release()

	qExecutor := NewWorldStateJSONQueryExecutor(snapshots, env.l)
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			_, err := qExecutor.ExecuteQuery(context.Background(), dbName, tt.query)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.expectedError)
		})
	}
}

func TestValidateAndDisectConditions(t *testing.T) {
	t.Parallel()

	indexDef := map[string]types.IndexAttributeType{
		"title":      types.IndexAttributeType_STRING,
		"year":       types.IndexAttributeType_NUMBER,
		"bestseller": types.IndexAttributeType_BOOLEAN,
	}
	marshaledIndexDef, err := json.Marshal(indexDef)
	require.NoError(t, err)

	createDbs := map[string]*worldstate.DBUpdates{
		worldstate.DatabasesDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   "db1",
					Value: marshaledIndexDef,
				},
				{
					Key: stateindex.IndexDB("db1"),
				},
			},
		},
	}

	testCases := []struct {
		name                       string
		dbName                     string
		setup                      func(t *testing.T, db worldstate.DB)
		conditions                 string
		expectedDisectedConditions attributeToConditions
	}{
		{
			name:   "single attribute and single equal condition",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"title": {
						"$eq": "book1"
					}
				}
			`,
			expectedDisectedConditions: attributeToConditions{
				"title": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						constants.QueryOpEqual: "book1",
					},
				},
			},
		},
		{
			name:   "single attribute and single not equal condition with string",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"title": {
						"$neq": ["book1"]
					}
				}
			`,
			expectedDisectedConditions: attributeToConditions{
				"title": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						constants.QueryOpNotEqual: []string{"book1"},
					},
				},
			},
		},
		{
			name:   "single attribute and single not equal condition with bool",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"bestseller": {
						"$neq": [false]
					}
				}
			`,
			expectedDisectedConditions: attributeToConditions{
				"bestseller": {
					valueType: types.IndexAttributeType_BOOLEAN,
					conditions: map[string]interface{}{
						constants.QueryOpNotEqual: []bool{false},
					},
				},
			},
		},
		{
			name:   "single attribute and mutiple not equal condition with int64",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"year": {
						"$neq": [2001, 2002, 2003]
					}
				}
			`,
			expectedDisectedConditions: attributeToConditions{
				"year": {
					valueType: types.IndexAttributeType_NUMBER,
					conditions: map[string]interface{}{
						constants.QueryOpNotEqual: []string{stateindex.EncodeInt64(2001), stateindex.EncodeInt64(2002), stateindex.EncodeInt64(2003)},
					},
				},
			},
		},
		{
			name:   "single attribute and multiple not equal conditions",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"title": {
						"$neq": ["book1","book2","book3"]
					}
				}
			`,
			expectedDisectedConditions: attributeToConditions{
				"title": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						constants.QueryOpNotEqual: []string{"book1", "book2", "book3"},
					},
				},
			},
		},
		{
			name:   "two attribute and singe condition per attribute and empty $neq",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"year": {
						"$gt": 2010
					},
					"bestseller": {
						"$eq": true,
						"$neq": []
					}
				}
			`,
			expectedDisectedConditions: attributeToConditions{
				"year": {
					valueType: types.IndexAttributeType_NUMBER,
					conditions: map[string]interface{}{
						constants.QueryOpGreaterThan: stateindex.EncodeInt64(2010),
					},
				},
				"bestseller": {
					valueType: types.IndexAttributeType_BOOLEAN,
					conditions: map[string]interface{}{
						constants.QueryOpEqual: true,
					},
				},
			},
		},
		{
			name:   "single attribute and multiple conditions and empty $neq",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"year": {
						"$gt": 2010,
						"$lt": 2020,
						"$neq": []
					}
				}
			`,
			expectedDisectedConditions: attributeToConditions{
				"year": {
					valueType: types.IndexAttributeType_NUMBER,
					conditions: map[string]interface{}{
						constants.QueryOpGreaterThan: stateindex.EncodeInt64(2010),
						constants.QueryOpLesserThan:  stateindex.EncodeInt64(2020),
					},
				},
			},
		},
		{
			name:   "multiple attributes and multiple conditions per attribute and empty $neq",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"year": {
						"$gt": 2010,
						"$lt": 2020,
						"$neq": []
					},
					"title": {
						"$gt": "book1",
						"$lt": "book100",
						"$neq": []
					}
				}
			`,
			expectedDisectedConditions: attributeToConditions{
				"year": {
					valueType: types.IndexAttributeType_NUMBER,
					conditions: map[string]interface{}{
						constants.QueryOpGreaterThan: stateindex.EncodeInt64(2010),
						constants.QueryOpLesserThan:  stateindex.EncodeInt64(2020),
					},
				},
				"title": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						constants.QueryOpGreaterThan: "book1",
						constants.QueryOpLesserThan:  "book100",
					},
				},
			},
		},
		{
			name:   "multiple attributes and multiple conditions including not equal to per attribute",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"year": {
						"$gt": 2010,
						"$lt": 2020,
						"$neq": [2015, 2017]
					},
					"title": {
						"$gt": "book1",
						"$lt": "book100",
						"$neq": ["book3", "book5"]
					},
					"bestseller": {
						"$neq": [false]
					}
				}
			`,
			expectedDisectedConditions: attributeToConditions{
				"year": {
					valueType: types.IndexAttributeType_NUMBER,
					conditions: map[string]interface{}{
						constants.QueryOpGreaterThan: stateindex.EncodeInt64(2010),
						constants.QueryOpLesserThan:  stateindex.EncodeInt64(2020),
						constants.QueryOpNotEqual:    []string{stateindex.EncodeInt64(2015), stateindex.EncodeInt64(2017)},
					},
				},
				"title": {
					valueType: types.IndexAttributeType_STRING,
					conditions: map[string]interface{}{
						constants.QueryOpGreaterThan: "book1",
						constants.QueryOpLesserThan:  "book100",
						constants.QueryOpNotEqual:    []string{"book3", "book5"},
					},
				},
				"bestseller": {
					valueType: types.IndexAttributeType_BOOLEAN,
					conditions: map[string]interface{}{
						constants.QueryOpNotEqual: []bool{false},
					},
				},
			},
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			defer env.cleanup()

			tt.setup(t, env.db)

			snapshots, err := env.db.GetDBsSnapshot([]string{worldstate.DatabasesDBName, stateindex.IndexDB(tt.dbName)})
			require.NoError(t, err)
			defer snapshots.Release()
			qExecutor := NewWorldStateJSONQueryExecutor(snapshots, env.l)

			conditions := make(map[string]interface{})
			decoder := json.NewDecoder(strings.NewReader(tt.conditions))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&conditions))
			disectedQueryConditions, err := qExecutor.validateAndDisectConditions(tt.dbName, conditions)
			require.NoError(t, err)
			require.Equal(t, tt.expectedDisectedConditions, disectedQueryConditions)
		})
	}
}

func TestValidateAndDisectConditionsErrorCases(t *testing.T) {
	t.Parallel()

	indexDef := map[string]types.IndexAttributeType{
		"title":      types.IndexAttributeType_STRING,
		"year":       types.IndexAttributeType_NUMBER,
		"bestseller": types.IndexAttributeType_BOOLEAN,
	}
	marshaledIndexDef, err := json.Marshal(indexDef)
	require.NoError(t, err)

	createDbs := map[string]*worldstate.DBUpdates{
		worldstate.DatabasesDBName: {
			Writes: []*worldstate.KVWithMetadata{
				{
					Key:   "db1",
					Value: marshaledIndexDef,
				},
				{
					Key: stateindex.IndexDB("db1"),
				},
			},
		},
	}

	testCases := []struct {
		name          string
		dbName        string
		setup         func(t *testing.T, db worldstate.DB)
		conditions    string
		expectedError string
	}{
		{
			name:   "attribute not indexed",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"publishedby": {
						"$eq": "p1"
					}
				}
			`,
			expectedError: "attribute [publishedby] given in the query condition is not indexed",
		},
		{
			name:   "query syntax error - no internal map",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"title": "abc"
				}
			`,
			expectedError: "query syntax error near the attribute [title]",
		},
		{
			name:   "invalid logical operators",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `{
				"title": {
					"$eqne": "p1"
				}
			}`,
			expectedError: "invalid logical operator [$eqne] provided for the attribute [title]",
		},
		{
			name:   "attribute indexed type is string but we pass number",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `{
				"title": {
					"$eq": 10
				}
			}`,
			expectedError: "attribute [title] is indexed but the value type provided in the query does not match the actual indexed type: the actual type [string] does not match the provided type [number]",
		},
		{
			name:   "attribute indexed type is number but we pass string",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `{
				"year": {
					"$eq":"p1"
				}
			}`,
			expectedError: "attribute [year] is indexed but the value type provided in the query does not match the actual indexed type: the actual type [number] does not match the provided type [string]",
		},
		{
			name:   "attribute indexed type is bool but we pass string",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `{
				"title": {
					"$eq": true
				}
			}`,
			expectedError: "attribute [title] is indexed but the value type provided in the query does not match the actual indexed type: the actual type [string] does not match the provided type [bool]",
		},
		{
			name:   "unsupported type",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `{
				"title": {
					"$eq": ["abc", "def"]
				}
			}`,
			expectedError: "attribute [title] is indexed but the value type provided in the query does not match the actual indexed type: the actual type [string] does not match the provided type [slice]",
		},
		{
			name:   "attribute indexed type is string but we pass non-slice string in the $neq",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `{
				"title": {
					"$neq": "book1"
				}
			}`,
			expectedError: "attribute [title] is indexed but incorrect value type provided in the query: query syntex error: array should be used for $neq condition",
		},
		{
			name:   "attribute indexed type is string but we pass both string and bool in the slice to $neq",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `{
				"title": {
					"$neq": [false, "book1"]
				}
			}`,
			expectedError: "attribute [title] is indexed but incorrect value type provided in the query: the actual type [string] does not match the provided type",
		},
		{
			name:   "attribute indexed type is bool but we pass both string and bool in the slice to $neq",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `{
				"bestseller": {
					"$neq": [false, "book1"]
				}
			}`,
			expectedError: "attribute [bestseller] is indexed but incorrect value type provided in the query: the actual type [boolean] does not match the provided type",
		},
		{
			name:   "attribute indexed type is int64 but we pass both int64 and string in the slice to $neq",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `{
				"year": {
					"$neq": [2100, "book1"]
				}
			}`,
			expectedError: "attribute [year] is indexed but incorrect value type provided in the query: the actual type [number] does not match the provided type",
		},
		{
			name:   "query syntax error due to more conditions with $eq",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"year": {
						"$gt": 2010,
						"$lt": 2020,
						"$eq": 2015
					},
					"title": {
						"$gt": "book1",
						"$lt": "book100"
					}
				}
			`,
			expectedError: "query syntax error near attribute [year]: with [$eq] condition, no other condition should be provided",
		},
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			defer env.cleanup()

			tt.setup(t, env.db)

			snapshots, err := env.db.GetDBsSnapshot([]string{worldstate.DatabasesDBName, stateindex.IndexDB(tt.dbName)})
			require.NoError(t, err)
			defer snapshots.Release()

			qExecutor := NewWorldStateJSONQueryExecutor(snapshots, env.l)

			conditions := make(map[string]interface{})
			decoder := json.NewDecoder(strings.NewReader(tt.conditions))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&conditions))
			disectedQueryConditions, err := qExecutor.validateAndDisectConditions(tt.dbName, conditions)
			require.EqualError(t, err, tt.expectedError)
			require.Nil(t, disectedQueryConditions)
		})
	}
}

func TestIsValidLogicalOperator(t *testing.T) {
	t.Parallel()

	for _, opt := range []string{"$eq", "$neq", "$gt", "$lt", "$gte", "$lte"} {
		t.Run(opt, func(t *testing.T) {
			require.True(t, isValidLogicalOperator(opt))
		})
	}

	for _, opt := range []string{"$and", "$or", "eq", "neq"} {
		t.Run(opt, func(t *testing.T) {
			require.False(t, isValidLogicalOperator(opt))
		})
	}
}

func TestValidateAttrConditions(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		conditions    map[string]interface{}
		expectedError string
	}{
		{
			name: "more than one condition with $eq",
			conditions: map[string]interface{}{
				constants.QueryOpEqual:       10,
				constants.QueryOpGreaterThan: 11,
			},
			expectedError: "with [$eq] condition, no other condition should be provided",
		},
		{
			name: "usage of both $gt and $gte",
			conditions: map[string]interface{}{
				constants.QueryOpGreaterThanOrEqual: 10,
				constants.QueryOpGreaterThan:        11,
			},
			expectedError: "use either [$gt] or [$gte] but not both",
		},
		{
			name: "usage of both $lt and $lte",
			conditions: map[string]interface{}{
				constants.QueryOpLesserThanOrEqual: 10,
				constants.QueryOpLesserThan:        11,
			},
			expectedError: "use either [$lt] or [$lte] but not both",
		},
		{
			name: "only one $eq",
			conditions: map[string]interface{}{
				constants.QueryOpEqual: 10,
			},
		},
		{
			name: "only $lt",
			conditions: map[string]interface{}{
				constants.QueryOpLesserThan: 10,
			},
		},
		{
			name: "only $gt",
			conditions: map[string]interface{}{
				constants.QueryOpGreaterThan: 10,
			},
		},
		{
			name: "only $lte",
			conditions: map[string]interface{}{
				constants.QueryOpLesserThanOrEqual: 10,
			},
		},
		{
			name: "only $gte",
			conditions: map[string]interface{}{
				constants.QueryOpLesserThanOrEqual: 10,
			},
		},
		{
			name: "$lt and $gt",
			conditions: map[string]interface{}{
				constants.QueryOpGreaterThan: 5,
				constants.QueryOpLesserThan:  10,
			},
		},
		{
			name: "$gte and $lte",
			conditions: map[string]interface{}{
				constants.QueryOpGreaterThanOrEqual: 5,
				constants.QueryOpLesserThanOrEqual:  10,
			},
		},
		{
			name: "$gt and $lte",
			conditions: map[string]interface{}{
				constants.QueryOpGreaterThan:       5,
				constants.QueryOpLesserThanOrEqual: 10,
			},
		},
		{
			name: "$gte and $lt",
			conditions: map[string]interface{}{
				constants.QueryOpGreaterThanOrEqual: 5,
				constants.QueryOpLesserThan:         10,
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if tt.expectedError != "" {
				require.Equal(t, tt.expectedError, validateAttrConditions(tt.conditions).Error())
			} else {
				require.NoError(t, validateAttrConditions(tt.conditions))
			}
		})
	}
}
