package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

type LogicalOperatorType string

const (
	LogicalOperatorAnd LogicalOperatorType = "and"
	LogicalOperatorOr  LogicalOperatorType = "or"
)

var _ Field = LogicalExpressionField{}

type LogicalExpressionField struct {
	urn                 string
	logicalOperatorType LogicalOperatorType
	operand1            Field
	operand2            Field
}

func NewLogicalExpressionField(
	urn string,
	logicalOperatorType LogicalOperatorType,
	operand1 Field,
	operand2 Field,
) *LogicalExpressionField {
	return &LogicalExpressionField{
		urn:                 urn,
		logicalOperatorType: logicalOperatorType,
		operand1:            operand1,
		operand2:            operand2,
	}
}

func (cf LogicalExpressionField) Execute(fieldsMeta []tsquery.FieldMeta) (tsquery.FieldMeta, ValueSupplier, error) {
	operand1Meta, operand1Supplier, err := cf.operand1.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, err
	}

	operand2Meta, operand2Supplier, err := cf.operand2.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, err
	}

	if !operand1Meta.Required() {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"operand 1 %s must be of required (non-optional) for logical operator %s when executing field %s",
			operand1Meta.Urn(),
			cf.logicalOperatorType,
			cf.urn,
		)
	}
	if !operand2Meta.Required() {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"operand 2 %s must be of required (non-optional) for logical operator %s when executing field %s",
			operand2Meta.Urn(),
			cf.logicalOperatorType,
			cf.urn,
		)
	}

	if operand1Meta.DataType() != tsquery.DataTypeBoolean {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"operand 1 %s must be of boolean type for logical operator %s when executing field %s",
			operand1Meta.Urn(),
			cf.logicalOperatorType,
			cf.urn,
		)
	}
	if operand2Meta.DataType() != tsquery.DataTypeBoolean {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"operand 2 %s must be of boolean type for logical operator %s when executing field %s",
			operand2Meta.Urn(),
			cf.logicalOperatorType,
			cf.urn,
		)
	}

	fm, err := tsquery.NewFieldMeta(
		cf.urn,
		tsquery.DataTypeBoolean,
		operand1Meta.Required() && operand2Meta.Required(),
	)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, err
	}

	var compareFunc func(any, any) any
	switch cf.logicalOperatorType {
	case LogicalOperatorAnd:
		compareFunc = func(v1, v2 any) any {
			return v1.(bool) && v2.(bool)
		}
	case LogicalOperatorOr:
		compareFunc = func(v1, v2 any) any {
			return v1.(bool) || v2.(bool)
		}
	default:
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"unsupported logical operator type %s when executing field %s",
			cf.logicalOperatorType,
			cf.urn,
		)
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
