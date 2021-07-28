package queryexecutor

import (
	"errors"

	"github.com/IBM-Blockchain/bcdb-server/internal/stateindex"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
)

type rangeQueryPlan struct {
	startKey *stateindex.IndexEntry
	endKey   *stateindex.IndexEntry
}

func createQueryPlan(attribute string, conds *attrConditions) (*rangeQueryPlan, error) {
	// we assume this function to get only valid conditions
	//   - eq and no other conditions
	//   - lt and lte do not appear together
	//   - gt and gte do not appear together

	if conds.valueType == types.Type_NUMBER {
		// TODO: support number values
		return nil, errors.New("numbers are not supported currently")
	}

	for c := range conds.conditions {
		if c == constants.NotEqual {
			// TODO: support not equal
			return nil, errors.New(constants.NotEqual + " is not supported currently")
		}
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

	if len(conds.conditions) == 1 {
		for c, v := range conds.conditions {
			setPlanForSingleCondition(c, v, p)
			return p, nil
		}
	}

	setPlanForMultipleConditions(conds, p)

	return p, nil
}

func setPlanForSingleCondition(c string, v interface{}, p *rangeQueryPlan) {
	switch c {
	case constants.Equal:
		p.startKey.ValuePosition = stateindex.Existing
		p.startKey.Value = v
		p.startKey.KeyPosition = stateindex.Beginning

		p.endKey.ValuePosition = stateindex.Existing
		p.endKey.Value = v
		p.endKey.KeyPosition = stateindex.Ending
	case constants.GreaterThan:
		p.startKey.ValuePosition = stateindex.Existing
		p.startKey.Value = v
		p.startKey.KeyPosition = stateindex.Ending

		p.endKey.ValuePosition = stateindex.Ending
	case constants.GreaterThanOrEqual:
		p.startKey.ValuePosition = stateindex.Existing
		p.startKey.Value = v
		p.startKey.KeyPosition = stateindex.Beginning

		p.endKey.ValuePosition = stateindex.Ending
	case constants.LesserThan:
		p.startKey.ValuePosition = stateindex.Beginning

		p.endKey.ValuePosition = stateindex.Existing
		p.endKey.Value = v
		p.endKey.KeyPosition = stateindex.Beginning
	case constants.LesserThanOrEqual:
		p.startKey.ValuePosition = stateindex.Beginning

		p.endKey.ValuePosition = stateindex.Existing
		p.endKey.Value = v
		p.endKey.KeyPosition = stateindex.Ending
	}
}

func setPlanForMultipleConditions(conds *attrConditions, p *rangeQueryPlan) {
	for c, v := range conds.conditions {
		// assume that
		//   - lt and lte do not appear together
		//   - gt and gte do not appear together
		p.startKey.ValuePosition = stateindex.Existing
		p.endKey.ValuePosition = stateindex.Existing

		switch c {
		case constants.GreaterThan:
			p.startKey.Value = v
			p.startKey.KeyPosition = stateindex.Ending
		case constants.GreaterThanOrEqual:
			p.startKey.Value = v
			p.startKey.KeyPosition = stateindex.Beginning
		case constants.LesserThan:
			p.endKey.Value = v
			p.endKey.KeyPosition = stateindex.Beginning
		case constants.LesserThanOrEqual:
			p.endKey.Value = v
			p.endKey.KeyPosition = stateindex.Ending
		}
	}
}
