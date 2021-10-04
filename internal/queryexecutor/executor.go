package queryexecutor

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/hyperledger-labs/orion-server/internal/stateindex"
	"github.com/hyperledger-labs/orion-server/internal/worldstate"
	"github.com/hyperledger-labs/orion-server/pkg/constants"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

// WorldStateQueryExecutor executes a given set of query criterias on the states stored in
// the world state database and returns a set of keys whose values are matching the given
// criterias
type WorldStateJSONQueryExecutor struct {
	db     worldstate.DBsSnapshot
	logger *logger.SugarLogger
}

func NewWorldStateJSONQueryExecutor(db worldstate.DBsSnapshot, l *logger.SugarLogger) *WorldStateJSONQueryExecutor {
	return &WorldStateJSONQueryExecutor{
		db:     db,
		logger: l,
	}
}

func (e *WorldStateJSONQueryExecutor) ExecuteQuery(ctx context.Context, dbName string, selector []byte) (map[string]bool, error) {
	query := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(selector))
	decoder.UseNumber()
	if err := decoder.Decode(&query); err != nil {
		return nil, errors.Wrap(err, "error decoding the query")
	}

	// only the following query semantics are allowed for now
	// "$and: {cond1, cond2, ...} -- all conditions must pass
	// "$or": {cond1, cond2, ...} -- any one condition needs to pass
	// {cond1, cond2, cond3} -- if no combination operator is specified, it defaults to "$and"

	// in the future, we will allow nested "$and", "$or" semantics

	if _, ok := query[constants.QueryFieldSelector]; !ok {
		return nil, errors.New("selector field is missing in the query")
	}
	query, ok := query[constants.QueryFieldSelector].(map[string]interface{})
	if !ok {
		return nil, errors.New("query syntax error near " + constants.QueryFieldSelector)
	}

	if len(query) == 0 {
		return nil, errors.New("query conditions cannot be empty")
	}

	_, and := query[constants.QueryOpAnd]
	_, or := query[constants.QueryOpOr]

	var keys map[string]bool

	switch {
	case !and && !or:
		// default is $and
		disectedConditions, err := e.validateAndDisectConditions(dbName, query)
		if err != nil {
			return nil, err
		}
		if keys, err = e.executeAND(ctx, dbName, disectedConditions); err != nil {
			return nil, err
		}
	case and && or:
		// not supported yet
		return nil, errors.New("there must be a single upper level combination operator")
	case and:
		c, ok := query[constants.QueryOpAnd].(map[string]interface{})
		if !ok {
			return nil, errors.New("query syntax error near $and")
		}

		disectedConditions, err := e.validateAndDisectConditions(dbName, c)
		if err != nil {
			return nil, err
		}
		if keys, err = e.executeAND(ctx, dbName, disectedConditions); err != nil {
			return nil, err
		}
	case or:
		c, ok := query[constants.QueryOpOr].(map[string]interface{})
		if !ok {
			return nil, errors.New("query syntax error near $or")
		}

		disectedConditions, err := e.validateAndDisectConditions(dbName, c)
		if err != nil {
			return nil, err
		}
		if keys, err = e.executeOR(ctx, dbName, disectedConditions); err != nil {
			return nil, err
		}
	}

	return keys, nil
}

type attributeToConditions map[string]*attributeTypeAndConditions

type attributeTypeAndConditions struct {
	valueType  types.IndexAttributeType
	conditions map[string]interface{}
}

func (e *WorldStateJSONQueryExecutor) validateAndDisectConditions(dbName string, conditions map[string]interface{}) (attributeToConditions, error) {
	// when we reach here, we assume that the given dbName exist
	marshledIndexDef, _, err := e.db.GetIndexDefinition(dbName)
	if err != nil {
		return nil, err
	}

	if marshledIndexDef == nil {
		return nil, errors.New("no index has been defined on the database " + dbName)
	}

	indexDef := map[string]types.IndexAttributeType{}
	if err := json.Unmarshal(marshledIndexDef, &indexDef); err != nil {
		return nil, err
	}

	queryConditions := make(attributeToConditions)
	for attr, c := range conditions {
		if _, ok := indexDef[attr]; !ok {
			return nil, errors.New("attribute [" + attr + "] given in the query condition is not indexed")
		}

		cond, ok := c.(map[string]interface{})
		if !ok {
			return nil, errors.New("query syntax error near the attribute [" + attr + "]")
		}

		if len(cond) == 0 {
			return nil, errors.New("no condition provided for the attribute [" + attr + "]. All given attributes must have a condition")
		}

		attrType := indexDef[attr]
		conds := &attributeTypeAndConditions{
			valueType:  attrType,
			conditions: make(map[string]interface{}),
		}

		for opr, v := range cond {
			if !isValidLogicalOperator(opr) {
				return nil, errors.New("invalid logical operator [" + opr + "] provided for the attribute [" + attr + "]")
			}

			var internalVal interface{}
			if opr == constants.QueryOpNotEqual {
				internalVal, err = constructInternalValueForSliceType(v, attrType)
				if err != nil {
					return nil, errors.WithMessage(err, "attribute ["+attr+"] is indexed but incorrect value type provided in the query")
				}
			} else {
				internalVal, err = constructInternalValueForNonSliceType(v, attrType)
				if err != nil {
					return nil, errors.WithMessage(err, "attribute ["+attr+"] is indexed but the value type provided in the query does not match the actual indexed type")
				}
			}

			if internalVal != nil {
				conds.conditions[opr] = internalVal
			}
		}

		if err := validateAttrConditions(conds.conditions); err != nil {
			return nil, errors.WithMessage(err, "query syntax error near attribute ["+attr+"]")
		}
		queryConditions[attr] = conds
	}

	return queryConditions, nil
}

func isValidLogicalOperator(opt string) bool {
	switch opt {
	case constants.QueryOpEqual,
		constants.QueryOpNotEqual,
		constants.QueryOpGreaterThan,
		constants.QueryOpLesserThan,
		constants.QueryOpGreaterThanOrEqual,
		constants.QueryOpLesserThanOrEqual:
		return true
	default:
		return false
	}
}

func constructInternalValueForNonSliceType(v interface{}, t types.IndexAttributeType) (interface{}, error) {
	switch v.(type) {
	case json.Number:
		if t == types.IndexAttributeType_NUMBER {
			n, err := v.(json.Number).Int64()
			if err != nil {
				return nil, err
			}
			return stateindex.EncodeInt64(n), nil
		}
		return nil, errors.New("the actual type [" + strings.ToLower(t.String()) + "]" +
			" does not match the provided type [number]")
	case string:
		if t == types.IndexAttributeType_STRING {
			return v, nil
		}
		return nil, errors.New("the actual type [" + strings.ToLower(t.String()) + "]" +
			" does not match the provided type [string]")

	case bool:
		if t == types.IndexAttributeType_BOOLEAN {
			return v, nil
		}
		return nil, errors.New("the actual type [" + strings.ToLower(t.String()) + "]" +
			" does not match the provided type [bool]")

	default:
		return nil, errors.New("the actual type [" + strings.ToLower(t.String()) + "]" +
			" does not match the provided type [" + reflect.TypeOf(v).Kind().String() + "]")
	}
}
func constructInternalValueForSliceType(v interface{}, t types.IndexAttributeType) (interface{}, error) {
	switch v.(type) {
	case []interface{}:
		var s []string
		var b []bool

		for _, item := range v.([]interface{}) {
			switch t {
			case types.IndexAttributeType_STRING:
				if i, ok := item.(string); ok {
					s = append(s, i)
					continue
				}
			case types.IndexAttributeType_BOOLEAN:
				if i, ok := item.(bool); ok {
					b = append(b, i)
					continue
				}
			case types.IndexAttributeType_NUMBER:
				jNum, ok := item.(json.Number)
				if ok {
					v, err := jNum.Int64()
					if err == nil {
						s = append(s, stateindex.EncodeInt64(v))
						continue
					}
				}

			}

			return nil, errors.New("the actual type [" + strings.ToLower(t.String()) + "]" +
				" does not match the provided type")
		}
		if len(s) > 0 {
			return s, nil
		} else if len(b) > 0 {
			return b, nil
		}

		return nil, nil

	default:
		return nil, errors.New("query syntex error: array should be used for $neq condition")
	}
}

// validateAttrConditions validates whether the conditions provided for an attribute respect
// the following rules:
//   1. when $eq (equal) operator is used, there should be no other logical operators such as $lt, $gt, etc...
//   2. when $gt (greater than) operator is used, there should not be a $gte (greater or equal to) operator
//   3. when $gte (greater than or equal to) operator is used, there should not be a $gt (greater than) operator
//   4. when $lt (lesser than) operator is used, there should not be a $lte (lesser than or equal to) operator
//   5. when $lte (lesser than or equal to) operator is used, there should not be a $lt (lesser than) operator
func validateAttrConditions(conds map[string]interface{}) error {
	if _, ok := conds[constants.QueryOpEqual]; ok {
		if len(conds) > 1 {
			return errors.New("with [" + constants.QueryOpEqual + "] condition, no other condition should be provided")
		}
	}

	_, gt := conds[constants.QueryOpGreaterThan]
	_, gte := conds[constants.QueryOpGreaterThanOrEqual]
	if gt && gte {
		return errors.New("use either [" + constants.QueryOpGreaterThan + "] or [" + constants.QueryOpGreaterThanOrEqual + "] but not both")
	}

	_, lt := conds[constants.QueryOpLesserThan]
	_, lte := conds[constants.QueryOpLesserThanOrEqual]
	if lt && lte {
		return errors.New("use either [" + constants.QueryOpLesserThan + "] or [" + constants.QueryOpLesserThanOrEqual + "] but not both")
	}

	return nil
}
