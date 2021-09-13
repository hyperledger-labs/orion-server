package queryexecutor

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/pkg/errors"
)

// WorldStateQueryExecutor executes a given set of query criterias on the states stored in
// the world state database and returns a set of keys whose values are matching the given
// criterias
type WorldStateQueryExecutor struct {
	db     worldstate.DB
	logger *logger.SugarLogger
}

func NewWorldStateQueryExecutor(db worldstate.DB, l *logger.SugarLogger) *WorldStateQueryExecutor {
	return &WorldStateQueryExecutor{
		db:     db,
		logger: l,
	}
}

type attributeToConditions map[string]*attributeTypeAndConditions

type attributeTypeAndConditions struct {
	valueType  types.Type
	conditions map[string]interface{}
}

func (e *WorldStateQueryExecutor) validateAndDisectConditions(dbName string, conditions map[string]interface{}) (attributeToConditions, error) {
	// when we reach here, we assume that the given dbName exist
	marshledIndexDef, _, err := e.db.GetIndexDefinition(dbName)
	if err != nil {
		return nil, err
	}

	if marshledIndexDef == nil {
		return nil, errors.New("no index has been defined on the database " + dbName)
	}

	indexDef := map[string]types.Type{}
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

		attrType := indexDef[attr]
		conds := &attributeTypeAndConditions{
			valueType:  attrType,
			conditions: make(map[string]interface{}),
		}

		for opr, v := range cond {
			if !isValidLogicalOperator(opr) {
				return nil, errors.New("invalid logical operator [" + opr + "] provided for the attribute [" + attr + "]")
			}

			if err := validateType(v, attrType); err != nil {
				return nil, errors.WithMessage(err, "attribute ["+attr+"] is indexed but the value type provided in the query does not match the actual indexed type")
			}

			if attrType == types.Type_NUMBER {
				v, err = v.(json.Number).Int64()
				if err != nil {
					return nil, err
				}
			}

			conds.conditions[opr] = v
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

func validateType(v interface{}, t types.Type) error {
	kind := reflect.TypeOf(v).Kind()
	switch kind {
	case reflect.String:
		isNumber := reflect.TypeOf(v).Name() == "Number"

		if t == types.Type_STRING && !isNumber {
			return nil
		} else if t == types.Type_NUMBER && isNumber {
			return nil
		} else {
			providedType := kind.String()
			if isNumber {
				providedType = "number"
			}
			return errors.New("the actual type [" + strings.ToLower(t.String()) + "]" +
				" does not match the provided type [" + providedType + "]")
		}

	case reflect.Bool:
		if t == types.Type_BOOLEAN {
			return nil
		}
		return errors.New("the actual type [" + strings.ToLower(t.String()) + "]" +
			" does not match the provided type [" + kind.String() + "]")
	default:
		return errors.New("the actual type [" + strings.ToLower(t.String()) + "]" +
			" does not match the provided type [" + kind.String() + "]")
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
	if _, ok := conds[constants.QueryOpNotEqual]; ok {
		return errors.New("currently [" + constants.QueryOpNotEqual + "] condition is not supported")
	}

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
