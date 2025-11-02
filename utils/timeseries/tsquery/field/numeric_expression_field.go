package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"maps"
)

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
	op1       Field
	op2       Field
	op        BinaryNumericOperatorType
	funcImpl  func(v1, v2 any) any
	fieldMeta tsquery.FieldMeta
}

func NewNumericExpressionField(
	fieldUrn string,
	op1 Field,
	op BinaryNumericOperatorType,
	op2 Field,
) (*NumericExpressionField, error) {
	dt1 := op1.Meta().DataType()
	dt2 := op2.Meta().DataType()

	// Check that both operands are numeric types
	if !dt1.IsNumeric() {
		return nil, fmt.Errorf("op1 field %s has non-numeric data type: %s", op1.Meta().Urn(), dt1)
	}
	if !dt2.IsNumeric() {
		return nil, fmt.Errorf("op2 field %s has non-numeric data type: %s", op2.Meta().Urn(), dt2)
	}

	// Check that both operands have the same data type
	if dt1 != dt2 {
		return nil, fmt.Errorf(
			"incompatible datatypes for fields %s and %s: %s %s %s",
			op1.Meta().Urn(),
			op2.Meta().Urn(),
			dt1,
			op,
			dt2,
		)
	}

	// Check modulo operator constraint
	if op == BinaryNumericOperatorMod && dt1 != tsquery.DataTypeInteger {
		return nil, fmt.Errorf("mod operator is only supported for integer fields. got fields %s and %s", op1.Meta().Urn(), op2.Meta().Urn())
	}

	// Get the function implementation
	funcImpl, err := op.getFuncImpl(dt1)
	if err != nil {
		return nil, fmt.Errorf("failed to get function implementation: %w", err)
	}

	var newFieldCustomMeta map[string]any
	if op1.Meta().CustomMeta() != nil {
		newFieldCustomMeta = maps.Clone(op1.Meta().CustomMeta())
		if op2.Meta().CustomMeta() != nil {
			maps.Copy(newFieldCustomMeta, op2.Meta().CustomMeta())
		}
	} else if op2.Meta().CustomMeta() != nil {
		newFieldCustomMeta = maps.Clone(op2.Meta().CustomMeta())
	}
	var updatedUnit string
	if op1.Meta().Unit() == op2.Meta().Unit() {
		updatedUnit = op1.Meta().Unit()
	}

	newFieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		fieldUrn,
		dt1,
		op1.Meta().Required() && op2.Meta().Required(),
		updatedUnit,
		newFieldCustomMeta,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create field meta for expression field: %w", err)
	}

	// Allow nil in the case of optional fields
	if !newFieldMeta.Required() {
		originalFunc := funcImpl
		funcImpl = func(v1, v2 any) any {
			if v1 == nil || v2 == nil {
				return nil
			}
			return originalFunc(v1, v2)
		}
	}

	return &NumericExpressionField{
		op1:       op1,
		op2:       op2,
		op:        op,
		funcImpl:  funcImpl,
		fieldMeta: *newFieldMeta,
	}, nil
}

func (nef *NumericExpressionField) Meta() tsquery.FieldMeta {
	return nef.fieldMeta
}

func (nef *NumericExpressionField) GetValue(ctx context.Context) (any, error) {
	v1, err := nef.op1.GetValue(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed getting value for op1: %w", err)
	}
	v2, err := nef.op2.GetValue(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed getting value for op2: %w", err)
	}
	return nef.funcImpl(v1, v2), nil
}
