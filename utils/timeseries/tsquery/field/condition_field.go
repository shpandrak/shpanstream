package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

type ConditionOperatorType string

const (
	ConditionOperatorEquals       ConditionOperatorType = "equals"
	ConditionOperatorNotEquals    ConditionOperatorType = "not_equals"
	ConditionOperatorGreaterThan  ConditionOperatorType = "greater_than"
	ConditionOperatorLessThan     ConditionOperatorType = "less_than"
	ConditionOperatorGreaterEqual ConditionOperatorType = "greater_equal"
	ConditionOperatorLessEqual    ConditionOperatorType = "less_equal"
)

var _ Field = ConditionField{}

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

func (co ConditionOperatorType) getFuncImpl(forDataType tsquery.DataType) (func(v1, v2 any) bool, error) {
	switch forDataType {
	case tsquery.DataTypeInteger:
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
	case tsquery.DataTypeDecimal:
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
	case tsquery.DataTypeString:
		switch co {
		case ConditionOperatorEquals:
			return equalsString, nil
		case ConditionOperatorNotEquals:
			return notEqualsString, nil
		default:
			return nil, fmt.Errorf("operator %s is not supported for non-numeric type %s. Only equals and not_equals are supported", co, forDataType)
		}
	case tsquery.DataTypeBoolean:
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

func wrapWithNilCheck(compareFunc func(a, b any) bool) func(a, b any) bool {
	return func(a, b any) bool {
		if a == nil || b == nil {
			return false
		}
		return compareFunc(a, b)
	}
}

type ConditionField struct {
	urn          string
	operatorType ConditionOperatorType
	operand1     Field
	operand2     Field
}

func NewConditionField(
	urn string,
	operatorType ConditionOperatorType,
	operand1 Field,
	operand2 Field,
) *ConditionField {
	return &ConditionField{urn: urn, operatorType: operatorType, operand1: operand1, operand2: operand2}
}

func (cf ConditionField) Execute(fieldsMeta []tsquery.FieldMeta) (tsquery.FieldMeta, ValueSupplier, error) {
	operand1Meta, operand1Supplier, err := cf.operand1.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, err
	}

	operand2Meta, operand2Supplier, err := cf.operand2.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, err
	}

	dt1 := operand1Meta.DataType()
	dt2 := operand2Meta.DataType()

	// Check that both operands have the same data type
	if dt1 != dt2 {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"operand types do not match: %s vs %s when executing field %s",
			dt1,
			dt2,
			cf.urn,
		)
	}

	// Get the comparison function implementation
	compareFunc, err := cf.operatorType.getFuncImpl(dt1)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed to get comparison function: %w", err)
	}

	// Add nil handling for optional operands
	if !operand1Meta.Required() || !operand2Meta.Required() {
		compareFunc = wrapWithNilCheck(compareFunc)
	}

	fm, err := tsquery.NewFieldMeta(
		cf.urn,
		tsquery.DataTypeBoolean,
		operand1Meta.Required() && operand2Meta.Required(),
	)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, err
	}

	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		val1, err := operand1Supplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed to get value for operand 1 when executing condition field %s: %w", cf.urn, err)
		}
		val2, err := operand2Supplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed to get value for operand 2 when executing condition field %s: %w", cf.urn, err)
		}
		return compareFunc(val1, val2), nil
	}
	return *fm, valueSupplier, nil
}
