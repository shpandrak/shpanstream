package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = ConditionFieldValue{}

type ConditionFieldValue struct {
	operatorType tsquery.ConditionOperatorType
	operand1     Value
	operand2     Value
}

func NewConditionFieldValue(
	operatorType tsquery.ConditionOperatorType,
	operand1 Value,
	operand2 Value,
) ConditionFieldValue {
	return ConditionFieldValue{operatorType: operatorType, operand1: operand1, operand2: operand2}
}

func (cf ConditionFieldValue) Execute(ctx context.Context, fieldMeta tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	operand1Meta, operand1Supplier, err := cf.operand1.Execute(ctx, fieldMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, err
	}

	operand2Meta, operand2Supplier, err := cf.operand2.Execute(ctx, fieldMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, err
	}

	dt1 := operand1Meta.DataType
	dt2 := operand2Meta.DataType

	// Check that both operands have the same data type
	if dt1 != dt2 {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
			"operand types do not match: %s vs %s when executing condition field",
			dt1,
			dt2,
		)
	}

	// Get the comparison function implementation
	compareFunc, err := cf.operatorType.GetFuncImpl(dt1)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed to get comparison function: %w", err)
	}

	// Add nil handling for optional operands
	if !operand1Meta.Required || !operand2Meta.Required {
		compareFunc = tsquery.WrapComparisonWithNilChecks(compareFunc)
	}

	fvm := tsquery.ValueMeta{
		DataType: tsquery.DataTypeBoolean,
		Required: operand1Meta.Required && operand2Meta.Required,
	}

	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[any]) (any, error) {
		val1, err := operand1Supplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed to get value for operand 1 when executing condition field: %w", err)
		}
		val2, err := operand2Supplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed to get value for operand 2 when executing condition field: %w", err)
		}
		return compareFunc(val1, val2), nil
	}
	return fvm, valueSupplier, nil
}
