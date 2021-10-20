package queryexecutor

import (
	"github.com/hyperledger-labs/orion-server/internal/stateindex"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/types"
)

type rangeQueryPlan struct {
	startKey    *stateindex.IndexEntry
	endKey      *stateindex.IndexEntry
	excludeKeys map[interface{}]*stateindex.IndexEntry
}

type toSeek interface {
	Seek(key []byte) bool
}

func createQueryPlan(attribute string, conds *attributeTypeAndConditions) (*rangeQueryPlan, error) {
	// we assume this function to get only valid conditions
	//   - eq and no other conditions
	//   - lt and lte do not appear together
	//   - gt and gte do not appear together
	//   - neq can appear alone or with lt, lte, gt, and gte
	//   - neq can appear more than once
	//   - correct value type for both slice and other types

	var excludeKeys map[interface{}]*stateindex.IndexEntry
	for c, v := range conds.conditions {
		if c != constants.QueryOpNotEqual {
			continue
		}

		excludeKeys = make(map[interface{}]*stateindex.IndexEntry)

		switch conds.valueType {
		case types.IndexAttributeType_BOOLEAN:
			for _, item := range v.([]bool) {
				excludeKeys[item] = &stateindex.IndexEntry{
					Attribute:     attribute,
					Type:          conds.valueType,
					ValuePosition: stateindex.Existing,
					Value:         item,
					KeyPosition:   stateindex.Ending,
				}
			}
		case types.IndexAttributeType_STRING, types.IndexAttributeType_NUMBER:
			for _, item := range v.([]string) {
				excludeKeys[item] = &stateindex.IndexEntry{
					Attribute:     attribute,
					Type:          conds.valueType,
					ValuePosition: stateindex.Existing,
					Value:         item,
					KeyPosition:   stateindex.Ending,
				}
			}
		}

		delete(conds.conditions, c)
		break
	}

	p := &rangeQueryPlan{
		startKey: &stateindex.IndexEntry{
			Attribute: attribute,
			Type:      conds.valueType,
		},
		endKey: &stateindex.IndexEntry{
			Attribute: attribute,
			Type:      conds.valueType,
		},
	}

	if len(excludeKeys) > 0 && len(conds.conditions) == 0 {
		// full scan with a seek to not equal to
		p.startKey.ValuePosition = stateindex.Beginning
		p.excludeKeys = excludeKeys
		p.endKey.ValuePosition = stateindex.Ending

		return p, nil
	}

	if len(conds.conditions) == 1 {
		for c, v := range conds.conditions {
			setPlanForSingleCondition(c, v, p)
		}
	} else {
		setPlanForMultipleConditions(conds, p)
	}

	p.excludeKeys = excludeKeys
	return p, nil
}

func setPlanForSingleCondition(c string, v interface{}, p *rangeQueryPlan) {
	// For all index entries, we have set ValuePosition to the Existing and
	// KeyPosition to the Existing.

	// While creating range query plan, we use the single byte delimiters Beginning,
	// Existing, and Ending where Beginning < Existing < Ending.

	// we use
	//   - use Beginning to denote before a beginning of a value,
	//   - use Existing to match the value,
	//   - use Ending to denote till after the end.

	// A single condition of types gt/gte/lt/lte means one key (e.g. the start key for
	// "gt value" or "gte value") specifies a value, whereas the other key (e.g. end key
	// in "gt/gte") specifies the end-range of the the attribute that is queried, that is,
	// the transition between one attribute to the next.

	// The key that specified the value has ValuePosition=Existing and uses KeyPosition to
	// capture whether the index entries that include the value are matched or not (e.g.
	// to implement "gte" vs. "gt").

	// The key that specified the end-range uses ValuePosition="Beginning" or "Ending" to
	// specify whether the end-range is below or above the value (for "lt/lte" vs "gt/gte", resp.).
	// The KeyPosition and the value in this case do not matter.
	switch c {
	case constants.QueryOpEqual:
		// By setting startKey.KeyPosition to `Beginning`, we instruct the executor
		// to start scanning from the beginning of the key that matches value `v`
		p.startKey.ValuePosition = stateindex.Existing
		p.startKey.Value = v
		p.startKey.KeyPosition = stateindex.Beginning

		// By setting endKey.KeyPosition to `Ending`, we instruct the executor
		// to scan till the last key that matches `v`
		p.endKey.ValuePosition = stateindex.Existing
		p.endKey.Value = v
		p.endKey.KeyPosition = stateindex.Ending
	case constants.QueryOpGreaterThan:
		// By setting startKey.KeyPosition to `Ending`, we instruct the executor
		// to start scanning from the next greater value of `v`
		p.startKey.ValuePosition = stateindex.Existing
		p.startKey.Value = v
		p.startKey.KeyPosition = stateindex.Ending

		// By setting endKey.ValuePosition to `Ending`, we instruct the executor
		// to scan till the end of all entries that belong to this attribute.
		// Value and KeyPosition do not matter and remain default.
		p.endKey.ValuePosition = stateindex.Ending
	case constants.QueryOpGreaterThanOrEqual:
		// By setting startKey.KeyPosition to `Beginning`, we instruct the executor
		// to start scanning from the beginning of the key that matches value `v`
		p.startKey.ValuePosition = stateindex.Existing
		p.startKey.Value = v
		p.startKey.KeyPosition = stateindex.Beginning

		// By setting endKey.ValuePosition to `Ending`, we instruct the executor
		// to scan till the end of all entries that belong to this attribute.
		// Value and KeyPosition do not matter and remain default.
		p.endKey.ValuePosition = stateindex.Ending
	case constants.QueryOpLesserThan:
		// By setting startKey.ValuePosition to `Beginning`, we instruct the executor
		// to scan from the beginning of entries
		p.startKey.ValuePosition = stateindex.Beginning

		// By setting endKey.KeyPosition to `Beginning`, we instruct the executor
		// to scan till before the begining of the key that matches value `v`
		p.endKey.ValuePosition = stateindex.Existing
		p.endKey.Value = v
		p.endKey.KeyPosition = stateindex.Beginning
	case constants.QueryOpLesserThanOrEqual:
		// By setting startKey.ValuePosition to `Beginning`, we instruct the executor
		// to scan from the beginning of entries that belong to this attribute.
		// Value and KeyPosition do not matterr and remain default
		p.startKey.ValuePosition = stateindex.Beginning

		// By setting endKey.KeyPosition to `Ending`, we instruct the executor
		// to scan till the end of last key that matches the value `v`
		p.endKey.ValuePosition = stateindex.Existing
		p.endKey.Value = v
		p.endKey.KeyPosition = stateindex.Ending
	}
}

func setPlanForMultipleConditions(conds *attributeTypeAndConditions, p *rangeQueryPlan) {
	for c, v := range conds.conditions {
		// assume that
		//   - lt and lte do not appear together
		//   - gt and gte do not appear together
		p.startKey.ValuePosition = stateindex.Existing
		p.endKey.ValuePosition = stateindex.Existing

		switch c {
		case constants.QueryOpGreaterThan:
			p.startKey.Value = v
			p.startKey.KeyPosition = stateindex.Ending
		case constants.QueryOpGreaterThanOrEqual:
			p.startKey.Value = v
			p.startKey.KeyPosition = stateindex.Beginning
		case constants.QueryOpLesserThan:
			p.endKey.Value = v
			p.endKey.KeyPosition = stateindex.Beginning
		case constants.QueryOpLesserThanOrEqual:
			p.endKey.Value = v
			p.endKey.KeyPosition = stateindex.Ending
		}
	}
}
