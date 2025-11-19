package report

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = LogicalExpressionFieldValue{}

type LogicalExpressionFieldValue struct {
	logicalOperatorType tsquery.LogicalOperatorType
	operand1            Value
	operand2            Value
}

func NewLogicalExpressionFieldValue(
	logicalOperatorType tsquery.LogicalOperatorType,
	operand1 Value,
	operand2 Value,
) *LogicalExpressionFieldValue {
	return &LogicalExpressionFieldValue{
		logicalOperatorType: logicalOperatorType,
		operand1:            operand1,
		operand2:            operand2,
	}
}

func (cf LogicalExpressionFieldValue) Execute(fieldsMeta []tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	operand1Meta, operand1Supplier, err := cf.operand1.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, err
	}

	operand2Meta, operand2Supplier, err := cf.operand2.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, err
	}

	if !operand1Meta.Required {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
			"operand 1 must be of required (non-optional) for logical operator %s when executing logical expression field",
			cf.logicalOperatorType,
		)
	}
	if !operand2Meta.Required {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
			"operand 2  must be of required (non-optional) for logical operator %s when executing logical expression field",
			cf.logicalOperatorType,
		)
	}

	if operand1Meta.DataType != tsquery.DataTypeBoolean {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
			"operand 1 must be of boolean type for logical operator %s when executing logical expression field",
			cf.logicalOperatorType,
		)
	}
	if operand2Meta.DataType != tsquery.DataTypeBoolean {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
			"operand 2 must be of boolean type for logical operator %s when executing logical expression field",
			cf.logicalOperatorType,
		)
	}

	fvm := tsquery.ValueMeta{
		DataType: tsquery.DataTypeBoolean,
		Required: operand1Meta.Required && operand2Meta.Required,
	}
	compareFunc, err := cf.logicalOperatorType.GetCompareFunc()
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, err
	}

	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		val1, err := operand1Supplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed to get value for operand 1 when executing logical expression field: %w", err)
		}
		val2, err := operand2Supplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed to get value for operand 2 when executing logical expression field: %w", err)
		}
		return compareFunc(val1, val2), nil
	}
	return fvm, valueSupplier, nil
}
