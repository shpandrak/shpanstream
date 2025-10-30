package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"testing"
)

// errorField is a test helper that returns an error when GetValue is called
type errorField struct {
	meta tsquery.FieldMeta
	err  error
}

func (ef *errorField) Meta() tsquery.FieldMeta {
	return ef.meta
}

func (ef *errorField) GetValue(_ context.Context) (any, error) {
	return nil, ef.err
}

func TestNumericExpressionField_IntegerOperations(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	tests := []struct {
		name     string
		v1       int64
		v2       int64
		operator BinaryNumericOperatorType
		expected int64
	}{
		{"Add", 10, 5, BinaryNumericOperatorAdd, 15},
		{"Subtract", 10, 5, BinaryNumericOperatorSub, 5},
		{"Multiply", 10, 5, BinaryNumericOperatorMul, 50},
		{"Divide", 10, 5, BinaryNumericOperatorDiv, 2},
		{"Modulo", 10, 3, BinaryNumericOperatorMod, 1},
		{"Add negative", -10, 5, BinaryNumericOperatorAdd, -5},
		{"Divide by one", 100, 1, BinaryNumericOperatorDiv, 100},
		{"Zero operations", 0, 0, BinaryNumericOperatorAdd, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field1, err := NewConstantField(*meta, tt.v1)
			require.NoError(t, err)
			field2, err := NewConstantField(*meta, tt.v2)
			require.NoError(t, err)

			expr, err := NewNumericExpressionField("expr_result", field1, tt.operator, field2)
			require.NoError(t, err)
			require.NotNil(t, expr)

			result, err := expr.GetValue(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNumericExpressionField_DecimalOperations(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	tests := []struct {
		name     string
		v1       float64
		v2       float64
		operator BinaryNumericOperatorType
		expected float64
	}{
		{"Add", 10.5, 5.5, BinaryNumericOperatorAdd, 16.0},
		{"Subtract", 10.5, 5.5, BinaryNumericOperatorSub, 5.0},
		{"Multiply", 10.5, 2.0, BinaryNumericOperatorMul, 21.0},
		{"Divide", 10.0, 2.5, BinaryNumericOperatorDiv, 4.0},
		{"Add negative", -10.5, 5.5, BinaryNumericOperatorAdd, -5.0},
		{"Divide by one", 100.5, 1.0, BinaryNumericOperatorDiv, 100.5},
		{"Zero operations", 0.0, 0.0, BinaryNumericOperatorAdd, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field1, err := NewConstantField(*meta, tt.v1)
			require.NoError(t, err)
			field2, err := NewConstantField(*meta, tt.v2)
			require.NoError(t, err)

			expr, err := NewNumericExpressionField("expr_result", field1, tt.operator, field2)
			require.NoError(t, err)
			require.NotNil(t, expr)

			result, err := expr.GetValue(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNumericExpressionField_IncompatibleTypes(t *testing.T) {
	intMeta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	decMeta, err := tsquery.NewFieldMeta("field2", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	field1, err := NewConstantField(*intMeta, int64(10))
	require.NoError(t, err)
	field2, err := NewConstantField(*decMeta, 5.5)
	require.NoError(t, err)

	expr, err := NewNumericExpressionField("expr_result", field1, BinaryNumericOperatorAdd, field2)
	require.Error(t, err)
	require.Nil(t, expr)
	require.Contains(t, err.Error(), "incompatible datatypes")
}

func TestNumericExpressionField_NonNumericTypes(t *testing.T) {
	stringMeta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeString, false)
	require.NoError(t, err)

	field1, err := NewConstantField(*stringMeta, "hello")
	require.NoError(t, err)
	field2, err := NewConstantField(*stringMeta, "world")
	require.NoError(t, err)

	expr, err := NewNumericExpressionField("expr_result", field1, BinaryNumericOperatorAdd, field2)
	require.Error(t, err)
	require.Nil(t, expr)
	require.Contains(t, err.Error(), "non-numeric data type")
}

func TestNumericExpressionField_ModuloOnlyForIntegers(t *testing.T) {
	decMeta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	field1, err := NewConstantField(*decMeta, 10.5)
	require.NoError(t, err)
	field2, err := NewConstantField(*decMeta, 2.5)
	require.NoError(t, err)

	expr, err := NewNumericExpressionField("expr_result", field1, BinaryNumericOperatorMod, field2)
	require.Error(t, err)
	require.Nil(t, expr)
	require.Contains(t, err.Error(), "mod operator is only supported for integer fields")
}

func TestNumericExpressionField_Meta(t *testing.T) {
	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	field1, err := NewConstantField(*meta, int64(10))
	require.NoError(t, err)
	field2, err := NewConstantField(*meta, int64(5))
	require.NoError(t, err)

	expr, err := NewNumericExpressionField("expr_result", field1, BinaryNumericOperatorAdd, field2)
	require.NoError(t, err)

	exprMeta := expr.Meta()
	require.Equal(t, "expr_result", exprMeta.Urn())
	require.Equal(t, meta.DataType(), exprMeta.DataType())
}

func TestNumericExpressionField_ErrorPropagation(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	t.Run("Error from op1", func(t *testing.T) {
		field1 := &errorField{meta: *meta, err: fmt.Errorf("op1 error")}
		field2, err := NewConstantField(*meta, int64(5))
		require.NoError(t, err)

		expr, err := NewNumericExpressionField("expr_result", field1, BinaryNumericOperatorAdd, field2)
		require.NoError(t, err)

		_, err = expr.GetValue(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed getting value for op1")
	})

	t.Run("Error from op2", func(t *testing.T) {
		field1, err := NewConstantField(*meta, int64(10))
		require.NoError(t, err)
		field2 := &errorField{meta: *meta, err: fmt.Errorf("op2 error")}

		expr, err := NewNumericExpressionField("expr_result", field1, BinaryNumericOperatorAdd, field2)
		require.NoError(t, err)

		_, err = expr.GetValue(ctx)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed getting value for op2")
	})
}

func TestNumericExpressionField_ComplexExpressions(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	// Test nested expressions: (10 + 5) * 2 = 30
	field1, err := NewConstantField(*meta, int64(10))
	require.NoError(t, err)
	field2, err := NewConstantField(*meta, int64(5))
	require.NoError(t, err)
	field3, err := NewConstantField(*meta, int64(2))
	require.NoError(t, err)

	// First create 10 + 5
	expr1, err := NewNumericExpressionField("expr1", field1, BinaryNumericOperatorAdd, field2)
	require.NoError(t, err)

	// Then multiply by 2
	expr2, err := NewNumericExpressionField("expr2", expr1, BinaryNumericOperatorMul, field3)
	require.NoError(t, err)

	result, err := expr2.GetValue(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(30), result)
}
