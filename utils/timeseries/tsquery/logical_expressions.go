package tsquery

import "fmt"

type ConditionOperatorType string

const (
	ConditionOperatorEquals       ConditionOperatorType = "equals"
	ConditionOperatorNotEquals    ConditionOperatorType = "not_equals"
	ConditionOperatorGreaterThan  ConditionOperatorType = "greater_than"
	ConditionOperatorLessThan     ConditionOperatorType = "less_than"
	ConditionOperatorGreaterEqual ConditionOperatorType = "greater_equal"
	ConditionOperatorLessEqual    ConditionOperatorType = "less_equal"
)

// Integer comparison operations
func equalsInt(v1, v2 any) bool {
	return v1.(int64) == v2.(int64)
}

func notEqualsInt(v1, v2 any) bool {
	return v1.(int64) != v2.(int64)
}

func greaterThanInt(v1, v2 any) bool {
	return v1.(int64) > v2.(int64)
}

func lessThanInt(v1, v2 any) bool {
	return v1.(int64) < v2.(int64)
}

func greaterEqualInt(v1, v2 any) bool {
	return v1.(int64) >= v2.(int64)
}

func lessEqualInt(v1, v2 any) bool {
	return v1.(int64) <= v2.(int64)
}

// Float comparison operations
func equalsFloat(v1, v2 any) bool {
	return v1.(float64) == v2.(float64)
}

func notEqualsFloat(v1, v2 any) bool {
	return v1.(float64) != v2.(float64)
}

func greaterThanFloat(v1, v2 any) bool {
	return v1.(float64) > v2.(float64)
}

func lessThanFloat(v1, v2 any) bool {
	return v1.(float64) < v2.(float64)
}

func greaterEqualFloat(v1, v2 any) bool {
	return v1.(float64) >= v2.(float64)
}

func lessEqualFloat(v1, v2 any) bool {
	return v1.(float64) <= v2.(float64)
}

// String comparison operations
func equalsString(v1, v2 any) bool {
	return v1.(string) == v2.(string)
}

func notEqualsString(v1, v2 any) bool {
	return v1.(string) != v2.(string)
}

// Boolean comparison operations
func equalsBoolean(v1, v2 any) bool {
	return v1.(bool) == v2.(bool)
}

func notEqualsBoolean(v1, v2 any) bool {
	return v1.(bool) != v2.(bool)
}

func (co ConditionOperatorType) GetFuncImpl(forDataType DataType) (func(v1, v2 any) bool, error) {
	switch forDataType {
	case DataTypeInteger:
		switch co {
		case ConditionOperatorEquals:
			return equalsInt, nil
		case ConditionOperatorNotEquals:
			return notEqualsInt, nil
		case ConditionOperatorGreaterThan:
			return greaterThanInt, nil
		case ConditionOperatorLessThan:
			return lessThanInt, nil
		case ConditionOperatorGreaterEqual:
			return greaterEqualInt, nil
		case ConditionOperatorLessEqual:
			return lessEqualInt, nil
		default:
			return nil, fmt.Errorf("unsupported condition operator %s for data type %s", co, forDataType)
		}
	case DataTypeDecimal:
		switch co {
		case ConditionOperatorEquals:
			return equalsFloat, nil
		case ConditionOperatorNotEquals:
			return notEqualsFloat, nil
		case ConditionOperatorGreaterThan:
			return greaterThanFloat, nil
		case ConditionOperatorLessThan:
			return lessThanFloat, nil
		case ConditionOperatorGreaterEqual:
			return greaterEqualFloat, nil
		case ConditionOperatorLessEqual:
			return lessEqualFloat, nil
		default:
			return nil, fmt.Errorf("unsupported condition operator %s for data type %s", co, forDataType)
		}
	case DataTypeString:
		switch co {
		case ConditionOperatorEquals:
			return equalsString, nil
		case ConditionOperatorNotEquals:
			return notEqualsString, nil
		default:
			return nil, fmt.Errorf("operator %s is not supported for non-numeric type %s. Only equals and not_equals are supported", co, forDataType)
		}
	case DataTypeBoolean:
		switch co {
		case ConditionOperatorEquals:
			return equalsBoolean, nil
		case ConditionOperatorNotEquals:
			return notEqualsBoolean, nil
		default:
			return nil, fmt.Errorf("operator %s is not supported for non-numeric type %s. Only equals and not_equals are supported", co, forDataType)
		}
	}
	return nil, fmt.Errorf("unsupported data type %s for condition operations", forDataType)
}

func WrapComparisonWithNilChecks(compareFunc func(a, b any) bool) func(a, b any) bool {
	return func(a, b any) bool {
		if a == nil || b == nil {
			return false
		}
		return compareFunc(a, b)
	}
}

type LogicalOperatorType string

func (lot LogicalOperatorType) GetCompareFunc() (func(any, any) any, error) {
	switch lot {
	case LogicalOperatorAnd:
		return func(v1, v2 any) any {
			return v1.(bool) && v2.(bool)
		}, nil
	case LogicalOperatorOr:
		return func(v1, v2 any) any {
			return v1.(bool) || v2.(bool)
		}, nil
	default:
		return nil, fmt.Errorf(
			"unsupported logical operator type %s when executing logical expression field",
			lot,
		)
	}

}

const (
	LogicalOperatorAnd LogicalOperatorType = "and"
	LogicalOperatorOr  LogicalOperatorType = "or"
)
