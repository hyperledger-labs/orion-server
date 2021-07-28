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
		conds        *attributeTypeAndConditions
		expectedPlan *rangeQueryPlan
	}{
		{
			name:      "equal to",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpEqual: "value-a",
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
			conds: &attributeTypeAndConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: "value-a",
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
			conds: &attributeTypeAndConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: "value-a",
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
			conds: &attributeTypeAndConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpLesserThan: "value-a",
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
			conds: &attributeTypeAndConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpLesserThanOrEqual: "value-a",
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
			conds: &attributeTypeAndConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: "value-a",
					constants.QueryOpLesserThan:  "value-z",
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
			conds: &attributeTypeAndConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan:       "value-a",
					constants.QueryOpLesserThanOrEqual: "value-z",
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
			conds: &attributeTypeAndConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: "value-a",
					constants.QueryOpLesserThan:         "value-z",
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
			conds: &attributeTypeAndConditions{
				valueType: types.Type_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: "value-a",
					constants.QueryOpLesserThanOrEqual:  "value-z",
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
