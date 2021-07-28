package queryexecutor

import (
	"testing"

	"github.com/IBM-Blockchain/bcdb-server/internal/stateindex"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCreateQueryPlan(t *testing.T) {
	tests := []struct {
		name         string
		attribute    string
		conds        *attrConditions
		expectedPlan *rangeQueryPlan
	}{
		{
			name:      "equal to",
			attribute: "attr1",
			conds: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.Equal: "value-a",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than",
			attribute: "attr1",
			conds: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.GreaterThan: "value-a",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than or equal to",
			attribute: "attr1",
			conds: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.GreaterThanOrEqual: "value-a",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "lesser than",
			attribute: "attr1",
			conds: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.LesserThan: "value-a",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "lesser than or equal to",
			attribute: "attr1",
			conds: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.LesserThanOrEqual: "value-a",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than and lesser than",
			attribute: "attr1",
			conds: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.GreaterThan: "value-a",
					constants.LesserThan:  "value-z",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-z",
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than and lesser than or equal",
			attribute: "attr1",
			conds: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.GreaterThan:       "value-a",
					constants.LesserThanOrEqual: "value-z",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-z",
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than or equal and lesser than",
			attribute: "attr1",
			conds: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.GreaterThanOrEqual: "value-a",
					constants.LesserThan:         "value-z",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-z",
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than or equal and lesser than or equal",
			attribute: "attr1",
			conds: &attrConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.GreaterThanOrEqual: "value-a",
					constants.LesserThanOrEqual:  "value-z",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.Type_STRING,
					Metadata:      "",
					ValuePosition: stateindex.Existing,
					Value:         "value-z",
					KeyPosition:   stateindex.Ending,
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			p, err := createQueryPlan(tt.attribute, tt.conds)
			require.NoError(t, err)
			require.Equal(t, tt.expectedPlan, p)
		})
	}
}
