package queryexecutor

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/internal/stateindex"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate/leveldb"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/stretchr/testify/require"
)

type testEnv struct {
	e       *WorldStateJSONQueryExecutor
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

	tempDir, err := ioutil.TempDir("/tmp", "queryexecutor")
	require.NoError(t, err)
	db, err := leveldb.Open(
		&leveldb.Config{
			DBRootDir: tempDir,
			Logger:    l,
		},
	)
	t.Cleanup(
		func() {
			if err := os.RemoveAll(tempDir); err != nil {
				t.Log("error during cleanup: removal of directory [" + tempDir + "] failed with error [" + err.Error() + "]")
				t.Fail()
			}
		},
	)
	require.NoError(t, err)

	return &testEnv{
		e: NewWorldStateJSONQueryExecutor(db, l),
		cleanup: func() {
			if err := db.Close(); err != nil {
				t.Log("error while closing the database: [" + err.Error() + "]")
			}
			if err := os.RemoveAll(tempDir); err != nil {
				t.Log("error during cleanup: removal of directory [" + tempDir + "] failed with error [" + err.Error() + "]")
				t.Fail()
			}
		},
	}
}

func TestExecuteJSONQuery(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	dbName := "testdb"
	setupDBForTestingExecutes(t, env.e.db, dbName)

	tests := []struct {
		name         string
		query        []byte
		expectedKeys map[string]bool
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
						}
					}
				}`,
			),
			expectedKeys: map[string]bool{
				"key1": true,
				"key2": true,
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
							}
						}
					}
				}`,
			),
			expectedKeys: map[string]bool{
				"key1": true,
				"key2": true,
				"key3": true,
			},
		},
		{
			name: "or is set",
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
			expectedKeys: map[string]bool{
				"key4":  true,
				"key5":  true,
				"key11": true,
				"key21": true,
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			keys, err := env.e.ExecuteQuery(dbName, tt.query)
			require.NoError(t, err)
			require.Equal(t, tt.expectedKeys, keys)
		})
	}
}

func TestExecuteJSONQueryErrorCases(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	dbName := "testdb"
	setupDBForTestingExecutes(t, env.e.db, dbName)

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
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			_, err := env.e.ExecuteQuery(dbName, tt.query)
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
			name:   "single attribute and single condition",
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
			name:   "two attribute and singe condition per attribute",
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
						"$eq": true
					}
				}
			`,
			expectedDisectedConditions: attributeToConditions{
				"year": {
					valueType: types.IndexAttributeType_NUMBER,
					conditions: map[string]interface{}{
						constants.QueryOpGreaterThan: int64(2010),
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
			name:   "single attribute and multiple conditions",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"year": {
						"$gt": 2010,
						"$lt": 2020
					}
				}
			`,
			expectedDisectedConditions: attributeToConditions{
				"year": {
					valueType: types.IndexAttributeType_NUMBER,
					conditions: map[string]interface{}{
						constants.QueryOpGreaterThan: int64(2010),
						constants.QueryOpLesserThan:  int64(2020),
					},
				},
			},
		},
		{
			name:   "multiple attributes and multiple conditions per attribute",
			dbName: "db1",
			setup: func(t *testing.T, db worldstate.DB) {
				require.NoError(t, db.Commit(createDbs, 1))
			},
			conditions: `
				{
					"year": {
						"$gt": 2010,
						"$lt": 2020
					},
					"title": {
						"$gt": "book1",
						"$lt": "book100"
					}
				}
			`,
			expectedDisectedConditions: attributeToConditions{
				"year": {
					valueType: types.IndexAttributeType_NUMBER,
					conditions: map[string]interface{}{
						constants.QueryOpGreaterThan: int64(2010),
						constants.QueryOpLesserThan:  int64(2020),
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
	}

	for _, tt := range testCases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newTestEnv(t)
			defer env.cleanup()

			tt.setup(t, env.e.db)

			conditions := make(map[string]interface{})
			decoder := json.NewDecoder(strings.NewReader(tt.conditions))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&conditions))
			disectedQueryConditions, err := env.e.validateAndDisectConditions(tt.dbName, conditions)
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
			name:          "no index error",
			dbName:        "db1",
			setup:         func(t *testing.T, db worldstate.DB) {},
			conditions:    `{}`,
			expectedError: "no index has been defined on the database db1",
		},
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

			tt.setup(t, env.e.db)

			conditions := make(map[string]interface{})
			decoder := json.NewDecoder(strings.NewReader(tt.conditions))
			decoder.UseNumber()
			require.NoError(t, decoder.Decode(&conditions))
			disectedQueryConditions, err := env.e.validateAndDisectConditions(tt.dbName, conditions)
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
			name: "$neq is not supported yet",
			conditions: map[string]interface{}{
				constants.QueryOpNotEqual: 10,
			},
			expectedError: "currently [$neq] condition is not supported",
		},
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
		{
			name: "$lt and $gt and $neq",
			conditions: map[string]interface{}{
				constants.QueryOpGreaterThan: 5,
				constants.QueryOpLesserThan:  10,
				constants.QueryOpNotEqual:    7,
			},
			expectedError: "currently [$neq] condition is not supported",
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
