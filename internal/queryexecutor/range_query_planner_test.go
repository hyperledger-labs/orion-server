package queryexecutor

import (
	"testing"

	"github.com/hyperledger-labs/orion-server/internal/stateindex"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCreateQueryPlanForNonNumberType(t *testing.T) {
	tests := []struct {
		name         string
		attribute    string
		conds        *attributeTypeAndConditions
		expectedPlan *rangeQueryPlan
	}{
		{
			name:      "equal to a string",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpEqual: "value-a",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "equal to a bool",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_BOOLEAN,
				conditions: map[string]interface{}{
					constants.QueryOpEqual: true,
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_BOOLEAN,
					ValuePosition: stateindex.Existing,
					Value:         true,
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_BOOLEAN,
					ValuePosition: stateindex.Existing,
					Value:         true,
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "not equal to string",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpNotEqual: []string{"value-c", "value-b"},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Beginning,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					"value-c": {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_STRING,
						ValuePosition: stateindex.Existing,
						Value:         "value-c",
						KeyPosition:   stateindex.Ending,
					},
					"value-b": {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_STRING,
						ValuePosition: stateindex.Existing,
						Value:         "value-b",
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "not equal to a bool",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_BOOLEAN,
				conditions: map[string]interface{}{
					constants.QueryOpNotEqual: []bool{true},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_BOOLEAN,
					ValuePosition: stateindex.Beginning,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					true: {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_BOOLEAN,
						ValuePosition: stateindex.Existing,
						Value:         true,
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_BOOLEAN,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "not equal to both bools",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_BOOLEAN,
				conditions: map[string]interface{}{
					constants.QueryOpNotEqual: []bool{true, false},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_BOOLEAN,
					ValuePosition: stateindex.Beginning,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					true: {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_BOOLEAN,
						ValuePosition: stateindex.Existing,
						Value:         true,
						KeyPosition:   stateindex.Ending,
					},
					false: {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_BOOLEAN,
						ValuePosition: stateindex.Existing,
						Value:         false,
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_BOOLEAN,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: "value-a",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than and not eqaul",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: "value-a",
					constants.QueryOpNotEqual:    []string{"value-c"},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Ending,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					"value-c": {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_STRING,
						ValuePosition: stateindex.Existing,
						Value:         "value-c",
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than or equal to",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: "value-a",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "lesser than or equal to and not equal to",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpLesserThanOrEqual: "value-a",
					constants.QueryOpNotEqual:          []string{"value-s"},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Beginning,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					"value-s": {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_STRING,
						ValuePosition: stateindex.Existing,
						Value:         "value-s",
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "lesser than",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpLesserThan: "value-a",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
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
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpLesserThanOrEqual: "value-a",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
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
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: "value-a",
					constants.QueryOpLesserThan:  "value-z",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
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
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan:       "value-a",
					constants.QueryOpLesserThanOrEqual: "value-z",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
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
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: "value-a",
					constants.QueryOpLesserThan:         "value-z",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
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
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: "value-a",
					constants.QueryOpLesserThanOrEqual:  "value-z",
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Existing,
					Value:         "value-z",
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than and lesser than or equal and not equal to",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_STRING,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan:       "value-a",
					constants.QueryOpLesserThanOrEqual: "value-z",
					constants.QueryOpNotEqual:          []string{"value-c", "value-d", "value-e"},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
					ValuePosition: stateindex.Existing,
					Value:         "value-a",
					KeyPosition:   stateindex.Ending,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					"value-c": {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_STRING,
						ValuePosition: stateindex.Existing,
						Value:         "value-c",
						KeyPosition:   stateindex.Ending,
					},
					"value-d": {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_STRING,
						ValuePosition: stateindex.Existing,
						Value:         "value-d",
						KeyPosition:   stateindex.Ending,
					},
					"value-e": {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_STRING,
						ValuePosition: stateindex.Existing,
						Value:         "value-e",
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_STRING,
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

func TestCreateQueryPlanForNumberType(t *testing.T) {
	tests := []struct {
		name         string
		attribute    string
		conds        *attributeTypeAndConditions
		expectedPlan *rangeQueryPlan
	}{
		{
			name:      "equal to a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpEqual: stateindex.EncodeInt64(100),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(100),
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(100),
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "not equal to a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpNotEqual: []string{stateindex.EncodeInt64(100)},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Beginning,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					stateindex.EncodeInt64(100): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(100),
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "not equal to multiple +ve numbers",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpNotEqual: []string{stateindex.EncodeInt64(100), stateindex.EncodeInt64(321)},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Beginning,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					stateindex.EncodeInt64(100): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(100),
						KeyPosition:   stateindex.Ending,
					},
					stateindex.EncodeInt64(321): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(321),
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "equal to a -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpEqual: stateindex.EncodeInt64(-100),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-100),
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-100),
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "not equal to a -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpNotEqual: []string{stateindex.EncodeInt64(-100)},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Beginning,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					stateindex.EncodeInt64(-100): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(-100),
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "not equal to multiple -ve numbers",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpNotEqual: []string{stateindex.EncodeInt64(-100), stateindex.EncodeInt64(-532)},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Beginning,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					stateindex.EncodeInt64(-100): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(-100),
						KeyPosition:   stateindex.Ending,
					},
					stateindex.EncodeInt64(-532): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(-532),
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: stateindex.EncodeInt64(95),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(95),
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than a +ve number and not equal to a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: stateindex.EncodeInt64(95),
					constants.QueryOpNotEqual:    []string{stateindex.EncodeInt64(100)},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(95),
					KeyPosition:   stateindex.Ending,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					stateindex.EncodeInt64(100): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(100),
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than a -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: stateindex.EncodeInt64(-95),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-95),
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than a -ve number and not equal to a -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: stateindex.EncodeInt64(-95),
					constants.QueryOpNotEqual:    []string{stateindex.EncodeInt64(-200)},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-95),
					KeyPosition:   stateindex.Ending,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					stateindex.EncodeInt64(-200): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(-200),
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than or equal to a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: stateindex.EncodeInt64(195),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(195),
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than or equal to a -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: stateindex.EncodeInt64(-95),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-95),
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Ending,
				},
			},
		},
		{
			name:      "lesser than a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpLesserThan: stateindex.EncodeInt64(1234),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(1234),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "lesser than a +ve number and not equal to a -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpLesserThan: stateindex.EncodeInt64(1234),
					constants.QueryOpNotEqual:   []string{stateindex.EncodeInt64(-100)},
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Beginning,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					stateindex.EncodeInt64(-100): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(-100),
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(1234),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "lesser than a -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpLesserThan: stateindex.EncodeInt64(-3456),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-3456),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "lesser than or equal to a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpLesserThanOrEqual: stateindex.EncodeInt64(1234),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(1234),
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "lesser than or equal to a -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpLesserThanOrEqual: stateindex.EncodeInt64(-3456),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-3456),
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than and lesser than of -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: stateindex.EncodeInt64(-2345),
					constants.QueryOpLesserThan:  stateindex.EncodeInt64(-2),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-2345),
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-2),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than and lesser than of -ve number and not equal to -ve value",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: stateindex.EncodeInt64(-2345),
					constants.QueryOpNotEqual:    []string{stateindex.EncodeInt64(-100)},
					constants.QueryOpLesserThan:  stateindex.EncodeInt64(-2),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-2345),
					KeyPosition:   stateindex.Ending,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					stateindex.EncodeInt64(-100): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(-100),
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-2),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than and lesser than of +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: stateindex.EncodeInt64(100),
					constants.QueryOpLesserThan:  stateindex.EncodeInt64(94224),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(100),
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(94224),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than and lesser than of +ve number and not equal to a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: stateindex.EncodeInt64(100),
					constants.QueryOpNotEqual:    []string{stateindex.EncodeInt64(350)},
					constants.QueryOpLesserThan:  stateindex.EncodeInt64(94224),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(100),
					KeyPosition:   stateindex.Ending,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					stateindex.EncodeInt64(350): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(350),
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(94224),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than and lesser than or equal of -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan:       stateindex.EncodeInt64(-2345),
					constants.QueryOpLesserThanOrEqual: stateindex.EncodeInt64(-2),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-2345),
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-2),
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than or equal and lesser than of -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: stateindex.EncodeInt64(-2345),
					constants.QueryOpLesserThan:         stateindex.EncodeInt64(-2),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-2345),
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-2),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than or equal and lesser than or equal of -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: stateindex.EncodeInt64(-2345),
					constants.QueryOpLesserThanOrEqual:  stateindex.EncodeInt64(-2),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-2345),
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-2),
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than and lesser than or equal of +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan:       stateindex.EncodeInt64(100),
					constants.QueryOpLesserThanOrEqual: stateindex.EncodeInt64(94224),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(100),
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(94224),
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than or equal and lesser than or equal of +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: stateindex.EncodeInt64(100),
					constants.QueryOpLesserThanOrEqual:  stateindex.EncodeInt64(94224),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(100),
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(94224),
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than a -ve number and lesser than a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: stateindex.EncodeInt64(-340),
					constants.QueryOpLesserThan:  stateindex.EncodeInt64(200),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-340),
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(200),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than a -ve number and lesser than a +ve number and not equal to a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: stateindex.EncodeInt64(-340),
					constants.QueryOpNotEqual:    []string{stateindex.EncodeInt64(0)},
					constants.QueryOpLesserThan:  stateindex.EncodeInt64(200),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-340),
					KeyPosition:   stateindex.Ending,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					stateindex.EncodeInt64(0): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(0),
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(200),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than a -ve number and lesser than a +ve number and not equal to multiple +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan: stateindex.EncodeInt64(-340),
					constants.QueryOpNotEqual:    []string{stateindex.EncodeInt64(0), stateindex.EncodeInt64(-100)},
					constants.QueryOpLesserThan:  stateindex.EncodeInt64(200),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-340),
					KeyPosition:   stateindex.Ending,
				},
				excludeKeys: map[interface{}]*stateindex.IndexEntry{
					stateindex.EncodeInt64(-100): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(-100),
						KeyPosition:   stateindex.Ending,
					},
					stateindex.EncodeInt64(0): {
						Attribute:     "attr1",
						Type:          types.IndexAttributeType_NUMBER,
						ValuePosition: stateindex.Existing,
						Value:         stateindex.EncodeInt64(0),
						KeyPosition:   stateindex.Ending,
					},
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(200),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than or equal to a -ve number and lesser than a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: stateindex.EncodeInt64(-340),
					constants.QueryOpLesserThan:         stateindex.EncodeInt64(200),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-340),
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(200),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than to a -ve number and lesser than or equal to a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan:       stateindex.EncodeInt64(-340),
					constants.QueryOpLesserThanOrEqual: stateindex.EncodeInt64(200),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-340),
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(200),
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than or equal to a -ve number and lesser than or equal to a +ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: stateindex.EncodeInt64(-340),
					constants.QueryOpLesserThanOrEqual:  stateindex.EncodeInt64(200),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-340),
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(200),
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than to a +ve number and lesser than to a -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpLesserThan:  stateindex.EncodeInt64(-340),
					constants.QueryOpGreaterThan: stateindex.EncodeInt64(200),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(200),
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-340),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than or equal to a +ve number and lesser than to a -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpLesserThan:         stateindex.EncodeInt64(-340),
					constants.QueryOpGreaterThanOrEqual: stateindex.EncodeInt64(200),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(200),
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-340),
					KeyPosition:   stateindex.Beginning,
				},
			},
		},
		{
			name:      "greater than to a +ve number and lesser than or equal to a -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThan:       stateindex.EncodeInt64(200),
					constants.QueryOpLesserThanOrEqual: stateindex.EncodeInt64(-340),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(200),
					KeyPosition:   stateindex.Ending,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-340),
					KeyPosition:   stateindex.Ending,
				},
			},
		},
		{
			name:      "greater than or equal to a +ve number and lesser than or equal to a -ve number",
			attribute: "attr1",
			conds: &attributeTypeAndConditions{
				valueType: types.IndexAttributeType_NUMBER,
				conditions: map[string]interface{}{
					constants.QueryOpGreaterThanOrEqual: stateindex.EncodeInt64(200),
					constants.QueryOpLesserThanOrEqual:  stateindex.EncodeInt64(-340),
				},
			},
			expectedPlan: &rangeQueryPlan{
				startKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(200),
					KeyPosition:   stateindex.Beginning,
				},
				endKey: &stateindex.IndexEntry{
					Attribute:     "attr1",
					Type:          types.IndexAttributeType_NUMBER,
					ValuePosition: stateindex.Existing,
					Value:         stateindex.EncodeInt64(-340),
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
