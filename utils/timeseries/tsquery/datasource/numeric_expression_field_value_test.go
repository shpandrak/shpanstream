package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Helper function to execute a field and get its value for testing
func executeAndGetValueForNumericTest(ctx context.Context, f Value) (any, error) {
	_, valueSupplier, err := f.Execute(ctx, tsquery.FieldMeta{})
	if err != nil {
		return nil, err
	}
	dummyRow := timeseries.TsRecord[any]{Timestamp: time.Now(), Value: nil}
	return valueSupplier(ctx, dummyRow)
}

// Helper function to execute a field and get its metadata for testing
func executeAndGetMetaForNumericTest(ctx context.Context, f Value) (tsquery.ValueMeta, error) {
	meta, _, err := f.Execute(ctx, tsquery.FieldMeta{})
	return meta, err
}

// errorFieldForNumeric is a test helper that returns an error when GetValue is called
type errorFieldForNumeric struct {
	meta tsquery.ValueMeta
	err  error
}

func (ef *errorFieldForNumeric) Execute(_ context.Context, _ tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	return ef.meta, func(_ context.Context, _ timeseries.TsRecord[any]) (any, error) {
		return nil, ef.err
	}, nil
}

func TestNumericExpressionField_IntegerOperations(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

	tests := []struct {
		name     string
		v1       int64
		v2       int64
		operator tsquery.BinaryNumericOperatorType
		expected int64
	}{
		{"Add", 10, 5, tsquery.BinaryNumericOperatorAdd, 15},
		{"Subtract", 10, 5, tsquery.BinaryNumericOperatorSub, 5},
		{"Multiply", 10, 5, tsquery.BinaryNumericOperatorMul, 50},
		{"Divide", 10, 5, tsquery.BinaryNumericOperatorDiv, 2},
		{"Modulo", 10, 3, tsquery.BinaryNumericOperatorMod, 1},
		{"Add negative", -10, 5, tsquery.BinaryNumericOperatorAdd, -5},
		{"Divide by one", 100, 1, tsquery.BinaryNumericOperatorDiv, 100},
		{"Zero operations", 0, 0, tsquery.BinaryNumericOperatorAdd, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field1 := NewConstantFieldValue(meta, tt.v1)
			field2 := NewConstantFieldValue(meta, tt.v2)

			expr := NewNumericExpressionFieldValue(field1, tt.operator, field2)
			require.NotNil(t, expr)

			result, err := executeAndGetValueForNumericTest(ctx, expr)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNumericExpressionField_DecimalOperations(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	tests := []struct {
		name     string
		v1       float64
		v2       float64
		operator tsquery.BinaryNumericOperatorType
		expected float64
	}{
		{"Add", 10.5, 5.5, tsquery.BinaryNumericOperatorAdd, 16.0},
		{"Subtract", 10.5, 5.5, tsquery.BinaryNumericOperatorSub, 5.0},
		{"Multiply", 10.5, 2.0, tsquery.BinaryNumericOperatorMul, 21.0},
		{"Divide", 10.0, 2.5, tsquery.BinaryNumericOperatorDiv, 4.0},
		{"Add negative", -10.5, 5.5, tsquery.BinaryNumericOperatorAdd, -5.0},
		{"Divide by one", 100.5, 1.0, tsquery.BinaryNumericOperatorDiv, 100.5},
		{"Zero operations", 0.0, 0.0, tsquery.BinaryNumericOperatorAdd, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field1 := NewConstantFieldValue(meta, tt.v1)
			field2 := NewConstantFieldValue(meta, tt.v2)

			expr := NewNumericExpressionFieldValue(field1, tt.operator, field2)
			require.NotNil(t, expr)

			result, err := executeAndGetValueForNumericTest(ctx, expr)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNumericExpressionField_IntDecimalPromotion(t *testing.T) {
	ctx := context.Background()
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	decMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	tests := []struct {
		name     string
		v1       any
		v1Meta   tsquery.ValueMeta
		v2       any
		v2Meta   tsquery.ValueMeta
		operator tsquery.BinaryNumericOperatorType
		expected float64
	}{
		{"Add", int64(10), intMeta, 5.5, decMeta, tsquery.BinaryNumericOperatorAdd, 15.5},
		{"Sub", int64(10), intMeta, 5.5, decMeta, tsquery.BinaryNumericOperatorSub, 4.5},
		{"Mul", int64(10), intMeta, 2.5, decMeta, tsquery.BinaryNumericOperatorMul, 25.0},
		{"Div", int64(10), intMeta, 4.0, decMeta, tsquery.BinaryNumericOperatorDiv, 2.5},
		{"Pow", int64(2), intMeta, 3.0, decMeta, tsquery.BinaryNumericOperatorPow, 8.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field1 := NewConstantFieldValue(tt.v1Meta, tt.v1)
			field2 := NewConstantFieldValue(tt.v2Meta, tt.v2)

			expr := NewNumericExpressionFieldValue(field1, tt.operator, field2)
			result, err := executeAndGetValueForNumericTest(ctx, expr)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNumericExpressionField_DecimalIntPromotion(t *testing.T) {
	ctx := context.Background()
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	decMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	tests := []struct {
		name     string
		v1       any
		v1Meta   tsquery.ValueMeta
		v2       any
		v2Meta   tsquery.ValueMeta
		operator tsquery.BinaryNumericOperatorType
		expected float64
	}{
		{"Add", 5.5, decMeta, int64(10), intMeta, tsquery.BinaryNumericOperatorAdd, 15.5},
		{"Sub", 10.5, decMeta, int64(5), intMeta, tsquery.BinaryNumericOperatorSub, 5.5},
		{"Mul", 2.5, decMeta, int64(10), intMeta, tsquery.BinaryNumericOperatorMul, 25.0},
		{"Div", 10.0, decMeta, int64(4), intMeta, tsquery.BinaryNumericOperatorDiv, 2.5},
		{"Pow", 2.0, decMeta, int64(3), intMeta, tsquery.BinaryNumericOperatorPow, 8.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field1 := NewConstantFieldValue(tt.v1Meta, tt.v1)
			field2 := NewConstantFieldValue(tt.v2Meta, tt.v2)

			expr := NewNumericExpressionFieldValue(field1, tt.operator, field2)
			result, err := executeAndGetValueForNumericTest(ctx, expr)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNumericExpressionField_IntDecimalPromotion_OutputType(t *testing.T) {
	ctx := context.Background()
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	decMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	field1 := NewConstantFieldValue(intMeta, int64(10))
	field2 := NewConstantFieldValue(decMeta, 5.5)

	expr := NewNumericExpressionFieldValue(field1, tsquery.BinaryNumericOperatorAdd, field2)
	exprMeta, err := executeAndGetMetaForNumericTest(ctx, expr)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, exprMeta.DataType)
}

func TestNumericExpressionField_IntDecimalModulo_Error(t *testing.T) {
	ctx := context.Background()
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	decMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	field1 := NewConstantFieldValue(intMeta, int64(10))
	field2 := NewConstantFieldValue(decMeta, 3.0)

	expr := NewNumericExpressionFieldValue(field1, tsquery.BinaryNumericOperatorMod, field2)
	_, _, err := expr.Execute(ctx, tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "mod operator is only supported for integer fields")
}

func TestNumericExpressionField_DecimalIntModulo_Error(t *testing.T) {
	ctx := context.Background()
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	decMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	field1 := NewConstantFieldValue(decMeta, 10.0)
	field2 := NewConstantFieldValue(intMeta, int64(3))

	expr := NewNumericExpressionFieldValue(field1, tsquery.BinaryNumericOperatorMod, field2)
	_, _, err := expr.Execute(ctx, tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "mod operator is only supported for integer fields")
}

func TestNumericExpressionField_IntDecimalPromotion_Chained(t *testing.T) {
	ctx := context.Background()
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	decMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	// (int + decimal) * int → decimal
	field1 := NewConstantFieldValue(intMeta, int64(10))
	field2 := NewConstantFieldValue(decMeta, 5.5)
	field3 := NewConstantFieldValue(intMeta, int64(2))

	expr1 := NewNumericExpressionFieldValue(field1, tsquery.BinaryNumericOperatorAdd, field2)
	expr2 := NewNumericExpressionFieldValue(expr1, tsquery.BinaryNumericOperatorMul, field3)

	result, err := executeAndGetValueForNumericTest(ctx, expr2)
	require.NoError(t, err)
	require.Equal(t, float64(31.0), result)

	exprMeta, err := executeAndGetMetaForNumericTest(ctx, expr2)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, exprMeta.DataType)
}

func TestNumericExpressionField_IntDecimalPromotion_OptionalNil(t *testing.T) {
	ctx := context.Background()
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	decMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	field1 := NewConstantFieldValue(intMeta, nil)
	field2 := NewConstantFieldValue(decMeta, 5.5)

	expr := NewNumericExpressionFieldValue(field1, tsquery.BinaryNumericOperatorAdd, field2)
	result, err := executeAndGetValueForNumericTest(ctx, expr)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestNumericExpressionField_NonNumericTypes(t *testing.T) {
	ctx := context.Background()
	stringMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeString, Required: false}

	field1 := NewConstantFieldValue(stringMeta, "hello")
	field2 := NewConstantFieldValue(stringMeta, "world")

	expr := NewNumericExpressionFieldValue(field1, tsquery.BinaryNumericOperatorAdd, field2)
	_, _, err := expr.Execute(ctx, tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-numeric data type")
}

func TestNumericExpressionField_ModuloOnlyForIntegers(t *testing.T) {
	ctx := context.Background()
	decMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	field1 := NewConstantFieldValue(decMeta, 10.5)
	field2 := NewConstantFieldValue(decMeta, 2.5)

	expr := NewNumericExpressionFieldValue(field1, tsquery.BinaryNumericOperatorMod, field2)
	_, _, err := expr.Execute(ctx, tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "mod operator is only supported for integer fields")
}

func TestNumericExpressionField_Meta(t *testing.T) {
	ctx := context.Background()
	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

	field1 := NewConstantFieldValue(meta, int64(10))
	field2 := NewConstantFieldValue(meta, int64(5))

	expr := NewNumericExpressionFieldValue(field1, tsquery.BinaryNumericOperatorAdd, field2)

	exprMeta, err := executeAndGetMetaForNumericTest(ctx, expr)
	require.NoError(t, err)
	require.Equal(t, meta.DataType, exprMeta.DataType)
}

func TestNumericExpressionField_ErrorPropagation(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

	t.Run("Error from op1", func(t *testing.T) {
		field1 := &errorFieldForNumeric{meta: meta, err: fmt.Errorf("op1 error")}
		field2 := NewConstantFieldValue(meta, int64(5))

		expr := NewNumericExpressionFieldValue(field1, tsquery.BinaryNumericOperatorAdd, field2)

		_, err := executeAndGetValueForNumericTest(ctx, expr)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed getting value for op1")
	})

	t.Run("Error from op2", func(t *testing.T) {
		field1 := NewConstantFieldValue(meta, int64(10))
		field2 := &errorFieldForNumeric{meta: meta, err: fmt.Errorf("op2 error")}

		expr := NewNumericExpressionFieldValue(field1, tsquery.BinaryNumericOperatorAdd, field2)

		_, err := executeAndGetValueForNumericTest(ctx, expr)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed getting value for op2")
	})
}

func TestNumericExpressionField_ComplexExpressions(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

	// Test nested expressions: (10 + 5) * 2 = 30
	field1 := NewConstantFieldValue(meta, int64(10))
	field2 := NewConstantFieldValue(meta, int64(5))
	field3 := NewConstantFieldValue(meta, int64(2))

	// First create 10 + 5
	expr1 := NewNumericExpressionFieldValue(field1, tsquery.BinaryNumericOperatorAdd, field2)

	// Then multiply by 2
	expr2 := NewNumericExpressionFieldValue(expr1, tsquery.BinaryNumericOperatorMul, field3)

	result, err := executeAndGetValueForNumericTest(ctx, expr2)
	require.NoError(t, err)
	require.Equal(t, int64(30), result)
}
