package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestUnaryNumericOperatorField_IntegerOperations(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	tests := []struct {
		name     string
		value    int64
		operator UnaryNumericOperatorType
		expected int64
	}{
		{"Abs positive", 42, UnaryNumericOperatorAbs, 42},
		{"Abs negative", -42, UnaryNumericOperatorAbs, 42},
		{"Abs zero", 0, UnaryNumericOperatorAbs, 0},
		{"Negate positive", 42, UnaryNumericOperatorNegate, -42},
		{"Negate negative", -42, UnaryNumericOperatorNegate, 42},
		{"Negate zero", 0, UnaryNumericOperatorNegate, 0},
		{"Sqrt 16", 16, UnaryNumericOperatorSqrt, 4},
		{"Sqrt 25", 25, UnaryNumericOperatorSqrt, 5},
		{"Sqrt 0", 0, UnaryNumericOperatorSqrt, 0},
		{"Sqrt 2 (truncated)", 2, UnaryNumericOperatorSqrt, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceField, err := NewConstantField(*meta, tt.value)
			require.NoError(t, err)

			unaryField, err := NewUnaryNumericOperatorField("result", sourceField, tt.operator)
			require.NoError(t, err)
			require.NotNil(t, unaryField)

			result, err := unaryField.GetValue(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestUnaryNumericOperatorField_DecimalOperations(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	tests := []struct {
		name     string
		value    float64
		operator UnaryNumericOperatorType
		expected float64
		epsilon  float64 // for floating point comparison
	}{
		{"Abs positive", 42.5, UnaryNumericOperatorAbs, 42.5, 0},
		{"Abs negative", -42.5, UnaryNumericOperatorAbs, 42.5, 0},
		{"Negate positive", 42.5, UnaryNumericOperatorNegate, -42.5, 0},
		{"Negate negative", -42.5, UnaryNumericOperatorNegate, 42.5, 0},
		{"Sqrt", 16.0, UnaryNumericOperatorSqrt, 4.0, 0},
		{"Ceil positive", 42.3, UnaryNumericOperatorCeil, 43.0, 0},
		{"Ceil negative", -42.3, UnaryNumericOperatorCeil, -42.0, 0},
		{"Floor positive", 42.7, UnaryNumericOperatorFloor, 42.0, 0},
		{"Floor negative", -42.7, UnaryNumericOperatorFloor, -43.0, 0},
		{"Round up", 42.6, UnaryNumericOperatorRound, 43.0, 0},
		{"Round down", 42.4, UnaryNumericOperatorRound, 42.0, 0},
		{"Round half to even", 43.5, UnaryNumericOperatorRound, 44.0, 0}, // Go rounds to even
		{"Log", math.E, UnaryNumericOperatorLog, 1.0, 0.0001},
		{"Log10", 100.0, UnaryNumericOperatorLog10, 2.0, 0.0001},
		{"Exp", 1.0, UnaryNumericOperatorExp, math.E, 0.0001},
		{"Sin 0", 0.0, UnaryNumericOperatorSin, 0.0, 0.0001},
		{"Sin pi/2", math.Pi / 2, UnaryNumericOperatorSin, 1.0, 0.0001},
		{"Cos 0", 0.0, UnaryNumericOperatorCos, 1.0, 0.0001},
		{"Cos pi", math.Pi, UnaryNumericOperatorCos, -1.0, 0.0001},
		{"Tan 0", 0.0, UnaryNumericOperatorTan, 0.0, 0.0001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceField, err := NewConstantField(*meta, tt.value)
			require.NoError(t, err)

			unaryField, err := NewUnaryNumericOperatorField("result", sourceField, tt.operator)
			require.NoError(t, err)
			require.NotNil(t, unaryField)

			result, err := unaryField.GetValue(ctx)
			require.NoError(t, err)

			resultFloat := result.(float64)
			if tt.epsilon > 0 {
				require.InDelta(t, tt.expected, resultFloat, tt.epsilon)
			} else {
				require.Equal(t, tt.expected, resultFloat)
			}
		})
	}
}

func TestUnaryNumericOperatorField_UnsupportedIntegerOperations(t *testing.T) {
	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField, err := NewConstantField(*meta, int64(42))
	require.NoError(t, err)

	unsupportedOps := []UnaryNumericOperatorType{
		UnaryNumericOperatorCeil,
		UnaryNumericOperatorFloor,
		UnaryNumericOperatorRound,
		UnaryNumericOperatorLog,
		UnaryNumericOperatorLog10,
		UnaryNumericOperatorExp,
		UnaryNumericOperatorSin,
		UnaryNumericOperatorCos,
		UnaryNumericOperatorTan,
	}

	for _, op := range unsupportedOps {
		t.Run(string(op), func(t *testing.T) {
			unaryField, err := NewUnaryNumericOperatorField("result", sourceField, op)
			require.Error(t, err)
			require.Nil(t, unaryField)
			require.Contains(t, err.Error(), "unsupported operator")
		})
	}
}

func TestUnaryNumericOperatorField_NonNumericType(t *testing.T) {
	stringMeta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeString, false)
	require.NoError(t, err)

	sourceField, err := NewConstantField(*stringMeta, "hello")
	require.NoError(t, err)

	unaryField, err := NewUnaryNumericOperatorField("result", sourceField, UnaryNumericOperatorAbs)
	require.Error(t, err)
	require.Nil(t, unaryField)
	require.Contains(t, err.Error(), "non-numeric data type")
}

func TestUnaryNumericOperatorField_Meta(t *testing.T) {
	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField, err := NewConstantField(*meta, int64(42))
	require.NoError(t, err)

	unaryField, err := NewUnaryNumericOperatorField("abs_result", sourceField, UnaryNumericOperatorAbs)
	require.NoError(t, err)

	fieldMeta := unaryField.Meta()
	require.Equal(t, "abs_result", fieldMeta.Urn())
	require.Equal(t, tsquery.DataTypeInteger, fieldMeta.DataType())
}

func TestUnaryNumericOperatorField_ErrorPropagation(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	errorField := &errorField{meta: *meta, err: fmt.Errorf("operand error")}

	unaryField, err := NewUnaryNumericOperatorField("result", errorField, UnaryNumericOperatorAbs)
	require.NoError(t, err)

	_, err = unaryField.GetValue(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed getting value for operand")
}

func TestUnaryNumericOperatorField_NilHandling(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField, err := NewConstantField(*meta, nil)
	require.NoError(t, err)

	unaryField, err := NewUnaryNumericOperatorField("result", sourceField, UnaryNumericOperatorAbs)
	require.NoError(t, err)

	result, err := unaryField.GetValue(ctx)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestUnaryNumericOperatorField_ChainedOperations(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	// Start with -16.0, apply abs -> 16.0, then sqrt -> 4.0
	sourceField, err := NewConstantField(*meta, -16.0)
	require.NoError(t, err)

	absField, err := NewUnaryNumericOperatorField("abs_result", sourceField, UnaryNumericOperatorAbs)
	require.NoError(t, err)

	sqrtField, err := NewUnaryNumericOperatorField("sqrt_result", absField, UnaryNumericOperatorSqrt)
	require.NoError(t, err)

	result, err := sqrtField.GetValue(ctx)
	require.NoError(t, err)
	require.Equal(t, 4.0, result)
}

func TestUnaryNumericOperatorField_CombinedWithBinaryOperations(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	// Calculate: abs(-5) + abs(-3) = 5 + 3 = 8
	field1, err := NewConstantField(*meta, int64(-5))
	require.NoError(t, err)

	field2, err := NewConstantField(*meta, int64(-3))
	require.NoError(t, err)

	abs1, err := NewUnaryNumericOperatorField("abs1", field1, UnaryNumericOperatorAbs)
	require.NoError(t, err)

	abs2, err := NewUnaryNumericOperatorField("abs2", field2, UnaryNumericOperatorAbs)
	require.NoError(t, err)

	sumField, err := NewNumericExpressionField("sum", abs1, BinaryNumericOperatorAdd, abs2)
	require.NoError(t, err)

	result, err := sumField.GetValue(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(8), result)
}

func TestUnaryNumericOperatorField_SpecialMathValues(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	tests := []struct {
		name     string
		value    float64
		operator UnaryNumericOperatorType
		check    func(t *testing.T, result float64)
	}{
		{
			name:     "Log of 0 is -Inf",
			value:    0.0,
			operator: UnaryNumericOperatorLog,
			check: func(t *testing.T, result float64) {
				require.True(t, math.IsInf(result, -1))
			},
		},
		{
			name:     "Sqrt of negative is NaN",
			value:    -1.0,
			operator: UnaryNumericOperatorSqrt,
			check: func(t *testing.T, result float64) {
				require.True(t, math.IsNaN(result))
			},
		},
		{
			name:     "Exp of large value",
			value:    10.0,
			operator: UnaryNumericOperatorExp,
			check: func(t *testing.T, result float64) {
				require.InDelta(t, 22026.465794806718, result, 0.001)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceField, err := NewConstantField(*meta, tt.value)
			require.NoError(t, err)

			unaryField, err := NewUnaryNumericOperatorField("result", sourceField, tt.operator)
			require.NoError(t, err)

			result, err := unaryField.GetValue(ctx)
			require.NoError(t, err)

			tt.check(t, result.(float64))
		})
	}
}
