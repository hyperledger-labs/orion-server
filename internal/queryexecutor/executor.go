package queryexecutor

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/IBM-Blockchain/bcdb-server/internal/worldstate"
	"github.com/IBM-Blockchain/bcdb-server/pkg/constants"
	"github.com/IBM-Blockchain/bcdb-server/pkg/logger"
	"github.com/IBM-Blockchain/bcdb-server/pkg/types"
	"github.com/pkg/errors"
)

type WorldStateJSONQueryExecutor struct {
	db     worldstate.DB
	logger *logger.SugarLogger
}

func NewWorldStateJSONQueryExecutor(db worldstate.DB, l *logger.SugarLogger) *WorldStateJSONQueryExecutor {
	return &WorldStateJSONQueryExecutor{
		db:     db,
		logger: l,
	}
}

func (e *WorldStateJSONQueryExecutor) ExecuteQuery(dbName string, selector []byte) (map[string]bool, error) {
	query := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(selector))
	decoder.UseNumber()
	if err := decoder.Decode(&query); err != nil {
		return nil, errors.Wrap(err, "error decoding the query")
	}

	_, and := query[constants.And]
	_, or := query[constants.Or]

	var keys map[string]bool

	switch {
	case !and && !or:
		// default is $and
		disectedConditions, err := e.validateAndDisectConditions(dbName, query)
		if err != nil {
			return nil, err
		}
		if keys, err = e.executeAND(dbName, disectedConditions); err != nil {
			return nil, err
		}
	case and && or:
		// not supported yet
		return nil, errors.New("there must be a single upper level combination operator")
	case and:
		c, ok := query[constants.And].(map[string]interface{})
		if !ok {
			return nil, errors.New("query syntax error near $and")
		}

		disectedConditions, err := e.validateAndDisectConditions(dbName, c)
		if err != nil {
			return nil, err
		}
		if keys, err = e.executeAND(dbName, disectedConditions); err != nil {
			return nil, err
		}
	case or:
		c, ok := query[constants.Or].(map[string]interface{})
		if !ok {
			return nil, errors.New("query syntax error near $or")
		}

		disectedConditions, err := e.validateAndDisectConditions(dbName, c)
		if err != nil {
			return nil, err
		}
		if keys, err = e.executeOR(dbName, disectedConditions); err != nil {
			return nil, err
		}
	}

	return keys, nil
}

type attributesConditions map[string]*attrConditions

type attrConditions struct {
	valueType  types.Type
	conditions map[string]interface{}
}

func (e *WorldStateJSONQueryExecutor) validateAndDisectConditions(dbName string, conditions map[string]interface{}) (attributesConditions, error) {
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
