package report

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = NumericExpressionFieldValue{}

type NumericExpressionFieldValue struct {
	op1 Value
	op2 Value
	op  tsquery.BinaryNumericOperatorType
}

func NewNumericExpressionFieldValue(
	op1 Value,
	op tsquery.BinaryNumericOperatorType,
	op2 Value,
) NumericExpressionFieldValue {
	return NumericExpressionFieldValue{
		op1: op1,
		op2: op2,
		op:  op,
	}
}

func (nef NumericExpressionFieldValue) Execute(ctx context.Context, fieldsMeta []tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	// Execute both fields to get metadata (lazy validation)
	op1Meta, op1ValueSupplier, err := nef.op1.Execute(ctx, fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed executing op1 field: %w", err)
	}
	op2Meta, op2ValueSupplier, err := nef.op2.Execute(ctx, fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed executing op2 field: %w", err)
	}

	dt1 := op1Meta.DataType
	dt2 := op2Meta.DataType

	// Check that both operands are numeric types
	if !dt1.IsNumeric() {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("op1 field has non-numeric data type: %s", dt1)
	}
	if !dt2.IsNumeric() {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("op2 field has non-numeric data type: %s", dt2)
	}

	// Auto-promote numeric types (int → decimal)
	promotedType := dt1
	if dt1 != dt2 {
		promoted, ok := tsquery.PromoteNumericTypes(dt1, dt2)
		if !ok {
			return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
				"incompatible datatypes for fields : %s %s %s",
				dt1,
				nef.op,
				dt2,
			)
		}
		promotedType = promoted
		// Wrap the integer operand's supplier to cast int64 → float64
		if dt1 == tsquery.DataTypeInteger {
			orig := op1ValueSupplier
			op1ValueSupplier = func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
				v, err := orig(ctx, currRow)
				if err != nil || v == nil {
					return v, err
				}
				return float64(v.(int64)), nil
			}
		}
		if dt2 == tsquery.DataTypeInteger {
			orig := op2ValueSupplier
			op2ValueSupplier = func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
				v, err := orig(ctx, currRow)
				if err != nil || v == nil {
					return v, err
				}
				return float64(v.(int64)), nil
			}
		}
	}

	// Check modulo operator constraint
	if nef.op == tsquery.BinaryNumericOperatorMod && promotedType != tsquery.DataTypeInteger {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("mod operator is only supported for integer fields. got %s", promotedType)
	}

	// Get the function implementation
	funcImpl, err := nef.op.GetFuncImpl(promotedType)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed to get function implementation: %w", err)
	}

	var updatedUnit string
	if op1Meta.Unit == op2Meta.Unit {
		updatedUnit = op1Meta.Unit
	}

	// Merge CustomMeta from both operands, op1 takes precedence on conflicts
	fvm := tsquery.ValueMeta{
		DataType:   promotedType,
		Unit:       updatedUnit,
		Required:   op1Meta.Required && op2Meta.Required,
		CustomMeta: tsquery.MergeCustomMeta(op2Meta.CustomMeta, op1Meta.CustomMeta),
	}

	// Allow nil in the case of optional fields
	if !fvm.Required {
		originalFunc := funcImpl
		funcImpl = func(v1, v2 any) any {
			if v1 == nil || v2 == nil {
				return nil
			}
			return originalFunc(v1, v2)
		}
	}

	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		v1, err := op1ValueSupplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed getting value for op1: %w", err)
		}
		v2, err := op2ValueSupplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed getting value for op2: %w", err)
		}
		return funcImpl(v1, v2), nil
	}

	return fvm, valueSupplier, nil
}
