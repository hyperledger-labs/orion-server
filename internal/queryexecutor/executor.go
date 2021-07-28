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

type attributesConditions map[string]*attrConditions

type attrConditions struct {
	valueType  types.Type
	conditions map[string]interface{}
}

func (e *WorldStateQueryExecutor) validateAndDisectConditions(dbName string, conditions map[string]interface{}) (attributesConditions, error) {
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

	queryConditions := make(attributesConditions)
	for attr, c := range conditions {
		if _, ok := indexDef[attr]; !ok {
			return nil, errors.New("attribute [" + attr + "] given in the query condition is not indexed")
		}

		cond, ok := c.(map[string]interface{})
		if !ok {
			return nil, errors.New("query syntax error near the attribute [" + attr + "]")
		}

		attrType := indexDef[attr]
		conds := &attrConditions{
			valueType:  attrType,
			conditions: make(map[string]interface{}),
		}

		for opt, v := range cond {
			if !isValidLogicalOperator(opt) {
				return nil, errors.New("invalid logical operator [" + opt + "] provided for the attribute [" + attr + "]")
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

			conds.conditions[opt] = v
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
	case constants.Equal,
		constants.NotEqual,
		constants.GreaterThan,
		constants.LesserThan,
		constants.GreaterThanOrEqual,
		constants.LesserThanOrEqual:
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

func validateAttrConditions(conds map[string]interface{}) error {
	if _, ok := conds[constants.Equal]; ok {
		if len(conds) > 1 {
			return errors.New("with [" + constants.Equal + "] condition, no other condition should be provided")
		}
	}

	_, gt := conds[constants.GreaterThan]
	_, gte := conds[constants.GreaterThanOrEqual]
	if gt && gte {
		return errors.New("use either [" + constants.GreaterThan + "] or [" + constants.GreaterThanOrEqual + "] but not both")
	}

	_, lt := conds[constants.LesserThan]
	_, lte := conds[constants.LesserThanOrEqual]
	if lt && lte {
		return errors.New("use either [" + constants.LesserThan + "] or [" + constants.LesserThanOrEqual + "] but not both")
	}

	return nil
}
