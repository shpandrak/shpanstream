package report

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = UnaryNumericOperatorFieldValue{}

type UnaryNumericOperatorFieldValue struct {
	operand Value
	op      tsquery.UnaryNumericOperatorType
}

func NewUnaryNumericOperatorFieldValue(
	operand Value,
	op tsquery.UnaryNumericOperatorType,
) UnaryNumericOperatorFieldValue {
	return UnaryNumericOperatorFieldValue{
		operand: operand,
		op:      op,
	}
}

func (ufv UnaryNumericOperatorFieldValue) Execute(ctx context.Context, fieldsMeta []tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	// Execute operand to get metadata (lazy validation)
	operandMeta, operandValueSupplier, err := ufv.operand.Execute(ctx, fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed executing operand field: %w", err)
	}
	dt := operandMeta.DataType

	// Check that operand is a numeric type
	if !dt.IsNumeric() {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("operand field has non-numeric data type: %s", dt)
	}

	// Get the function implementation
	funcImpl, err := ufv.op.GetFuncImpl(dt)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed to get function implementation: %w", err)
	}

	fvm := tsquery.ValueMeta{
		DataType:   dt,
		Unit:       operandMeta.Unit,
		Required:   operandMeta.Required,
		CustomMeta: operandMeta.CustomMeta,
	}

	// Allow nil in the case of optional fields
	if !fvm.Required {
		originalFunc := funcImpl
		funcImpl = func(v any) any {
			if v == nil {
				return nil
			}
			return originalFunc(v)
		}
	}

	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		value, err := operandValueSupplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed getting value for operand: %w", err)
		}
		return funcImpl(value), nil
	}

	return fvm, valueSupplier, nil
}
