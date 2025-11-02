package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"maps"
)

var _ Field = NumericExpressionField{}

type BinaryNumericOperatorType string

// Integer operations
func addInt(v1, v2 any) any {
	return v1.(int64) + v2.(int64)
}

func subInt(v1, v2 any) any {
	return v1.(int64) - v2.(int64)
}

func mulInt(v1, v2 any) any {
	return v1.(int64) * v2.(int64)
}

func divInt(v1, v2 any) any {
	return v1.(int64) / v2.(int64)
}

func modInt(v1, v2 any) any {
	return v1.(int64) % v2.(int64)
}

// Decimal operations
func addDecimal(v1, v2 any) any {
	return v1.(float64) + v2.(float64)
}

func subDecimal(v1, v2 any) any {
	return v1.(float64) - v2.(float64)
}

func mulDecimal(v1, v2 any) any {
	return v1.(float64) * v2.(float64)
}

func divDecimal(v1, v2 any) any {
	return v1.(float64) / v2.(float64)
}

func (bno BinaryNumericOperatorType) getFuncImpl(forDataType tsquery.DataType) (func(v1, v2 any) any, error) {
	switch forDataType {
	case tsquery.DataTypeInteger:
		switch bno {
		case BinaryNumericOperatorAdd:
			return addInt, nil
		case BinaryNumericOperatorSub:
			return subInt, nil
		case BinaryNumericOperatorMul:
			return mulInt, nil
		case BinaryNumericOperatorDiv:
			return divInt, nil
		case BinaryNumericOperatorMod:
			return modInt, nil
		default:
			return nil, fmt.Errorf("unsupported operator %s for data type %s", bno, forDataType)
		}
	case tsquery.DataTypeDecimal:
		switch bno {
		case BinaryNumericOperatorAdd:
			return addDecimal, nil
		case BinaryNumericOperatorSub:
			return subDecimal, nil
		case BinaryNumericOperatorMul:
			return mulDecimal, nil
		case BinaryNumericOperatorDiv:
			return divDecimal, nil
		default:
			return nil, fmt.Errorf("unsupported operator %s for data type %s", bno, forDataType)
		}
	}
	return nil, fmt.Errorf("unsupported data type %s for numeric operations", forDataType)
}

const (
	BinaryNumericOperatorAdd BinaryNumericOperatorType = "+"
	BinaryNumericOperatorSub BinaryNumericOperatorType = "-"
	BinaryNumericOperatorMul BinaryNumericOperatorType = "*"
	BinaryNumericOperatorDiv BinaryNumericOperatorType = "/"
	BinaryNumericOperatorMod BinaryNumericOperatorType = "%"
)

type NumericExpressionField struct {
	op1      Field
	op2      Field
	op       BinaryNumericOperatorType
	fieldUrn string
}

func NewNumericExpressionField(
	fieldUrn string,
	op1 Field,
	op BinaryNumericOperatorType,
	op2 Field,
) NumericExpressionField {
	return NumericExpressionField{
		op1:      op1,
		op2:      op2,
		op:       op,
		fieldUrn: fieldUrn,
	}
}

func (nef NumericExpressionField) Execute(fieldsMeta []tsquery.FieldMeta) (tsquery.FieldMeta, ValueSupplier, error) {
	// Execute both fields to get metadata (lazy validation)
	op1Meta, op1ValueSupplier, err := nef.op1.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed executing op1 field: %w", err)
	}
	op2Meta, op2ValueSupplier, err := nef.op2.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed executing op2 field: %w", err)
	}

	dt1 := op1Meta.DataType()
	dt2 := op2Meta.DataType()

	// Check that both operands are numeric types
	if !dt1.IsNumeric() {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("op1 field %s has non-numeric data type: %s", op1Meta.Urn(), dt1)
	}
	if !dt2.IsNumeric() {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("op2 field %s has non-numeric data type: %s", op2Meta.Urn(), dt2)
	}

	// Check that both operands have the same data type
	if dt1 != dt2 {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"incompatible datatypes for fields %s and %s: %s %s %s",
			op1Meta.Urn(),
			op2Meta.Urn(),
			dt1,
			nef.op,
			dt2,
		)
	}

	// Check modulo operator constraint
	if nef.op == BinaryNumericOperatorMod && dt1 != tsquery.DataTypeInteger {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("mod operator is only supported for integer fields. got fields %s and %s", op1Meta.Urn(), op2Meta.Urn())
	}

	// Get the function implementation
	funcImpl, err := nef.op.getFuncImpl(dt1)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed to get function implementation: %w", err)
	}

	var newFieldCustomMeta map[string]any
	if op1Meta.CustomMeta() != nil {
		newFieldCustomMeta = maps.Clone(op1Meta.CustomMeta())
		if op2Meta.CustomMeta() != nil {
			maps.Copy(newFieldCustomMeta, op2Meta.CustomMeta())
		}
	} else if op2Meta.CustomMeta() != nil {
		newFieldCustomMeta = maps.Clone(op2Meta.CustomMeta())
	}
	var updatedUnit string
	if op1Meta.Unit() == op2Meta.Unit() {
		updatedUnit = op1Meta.Unit()
	}

	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		nef.fieldUrn,
		dt1,
		op1Meta.Required() && op2Meta.Required(),
		updatedUnit,
		newFieldCustomMeta,
	)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed to create field meta for expression field: %w", err)
	}

	// Allow nil in the case of optional fields
	if !fieldMeta.Required() {
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

	return *fieldMeta, valueSupplier, nil
}
