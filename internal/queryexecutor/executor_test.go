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
	e       *WorldStateQueryExecutor
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
		e: NewWorldStateQueryExecutor(db, l),
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

func TestValidateAndDisectConditions(t *testing.T) {
	t.Parallel()

	indexDef := map[string]types.Type{
		"title":      types.Type_STRING,
		"year":       types.Type_NUMBER,
		"bestseller": types.Type_BOOLEAN,
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
		expectedDisectedConditions attributesConditions
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
			expectedDisectedConditions: attributesConditions{
				"title": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						constants.Equal: "book1",
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
			expectedDisectedConditions: attributesConditions{
				"year": {
					valueType: types.Type_NUMBER,
					conditions: map[string]interface{}{
						constants.GreaterThan: int64(2010),
					},
				},
				"bestseller": {
					valueType: types.Type_BOOLEAN,
					conditions: map[string]interface{}{
						constants.Equal: true,
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
			expectedDisectedConditions: attributesConditions{
				"year": {
					valueType: types.Type_NUMBER,
					conditions: map[string]interface{}{
						constants.GreaterThan: int64(2010),
						constants.LesserThan:  int64(2020),
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
			expectedDisectedConditions: attributesConditions{
				"year": {
					valueType: types.Type_NUMBER,
					conditions: map[string]interface{}{
						constants.GreaterThan: int64(2010),
						constants.LesserThan:  int64(2020),
					},
				},
				"title": {
					valueType: types.Type_STRING,
					conditions: map[string]interface{}{
						constants.GreaterThan: "book1",
						constants.LesserThan:  "book100",
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

	indexDef := map[string]types.Type{
		"title":      types.Type_STRING,
		"year":       types.Type_NUMBER,
		"bestseller": types.Type_BOOLEAN,
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
			name: "more than one condition with $eq",
			conditions: map[string]interface{}{
				constants.Equal:       10,
				constants.GreaterThan: 11,
			},
			expectedError: "with [$eq] condition, no other condition should be provided",
		},
		{
			name: "usage of both $gt and $gte",
			conditions: map[string]interface{}{
				constants.GreaterThanOrEqual: 10,
				constants.GreaterThan:        11,
			},
			expectedError: "use either [$gt] or [$gte] but not both",
		},
		{
			name: "usage of both $lt and $lte",
			conditions: map[string]interface{}{
				constants.LesserThanOrEqual: 10,
				constants.LesserThan:        11,
			},
			expectedError: "use either [$lt] or [$lte] but not both",
		},
		{
			name: "only one $eq",
			conditions: map[string]interface{}{
				constants.Equal: 10,
			},
		},
		{
			name: "only $lt",
			conditions: map[string]interface{}{
				constants.LesserThan: 10,
			},
		},
		{
			name: "only $gt",
			conditions: map[string]interface{}{
				constants.GreaterThan: 10,
			},
		},
		{
			name: "only $lte",
			conditions: map[string]interface{}{
				constants.LesserThanOrEqual: 10,
			},
		},
		{
			name: "only $gte",
			conditions: map[string]interface{}{
				constants.LesserThanOrEqual: 10,
			},
		},
		{
			name: "$lt and $gt",
			conditions: map[string]interface{}{
				constants.GreaterThan: 5,
				constants.LesserThan:  10,
			},
		},
		{
			name: "$gte and $lte",
			conditions: map[string]interface{}{
				constants.GreaterThanOrEqual: 5,
				constants.LesserThanOrEqual:  10,
			},
		},
		{
			name: "$gt and $lte",
			conditions: map[string]interface{}{
				constants.GreaterThan:       5,
				constants.LesserThanOrEqual: 10,
			},
		},
		{
			name: "$gte and $lt",
			conditions: map[string]interface{}{
				constants.GreaterThanOrEqual: 5,
				constants.LesserThan:         10,
			},
		},
		{
			name: "$lt and $neq",
			conditions: map[string]interface{}{
				constants.LesserThan: 10,
				constants.NotEqual:   5,
			},
		},
		{
			name: "$gt and $neq",
			conditions: map[string]interface{}{
				constants.GreaterThan: 10,
				constants.NotEqual:    15,
			},
		},
		{
			name: "$lte and $neq",
			conditions: map[string]interface{}{
				constants.LesserThanOrEqual: 10,
				constants.NotEqual:          5,
			},
		},
		{
			name: "$gte and $neq",
			conditions: map[string]interface{}{
				constants.LesserThanOrEqual: 10,
				constants.NotEqual:          5,
			},
		},
		{
			name: "$lt and $gt and $neq",
			conditions: map[string]interface{}{
				constants.GreaterThan: 5,
				constants.LesserThan:  10,
				constants.NotEqual:    7,
			},
		},
		{
			name: "$gte and $lte and $neq",
			conditions: map[string]interface{}{
				constants.GreaterThanOrEqual: 5,
				constants.LesserThanOrEqual:  10,
				constants.NotEqual:           7,
			},
		},
		{
			name: "$gt and $lte and $neq",
			conditions: map[string]interface{}{
				constants.GreaterThan:       5,
				constants.LesserThanOrEqual: 10,
				constants.NotEqual:          7,
			},
		},
		{
			name: "$gte and $lt and $neq",
			conditions: map[string]interface{}{
				constants.GreaterThanOrEqual: 5,
				constants.LesserThan:         10,
				constants.NotEqual:           7,
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
