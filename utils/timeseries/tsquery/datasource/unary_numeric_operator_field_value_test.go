package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
	"time"
)

// Helper function to execute a field and get its value for unary tests
func executeUnaryFieldValue(ctx context.Context, f Value) (any, error) {
	_, valueSupplier, err := f.Execute(ctx, tsquery.FieldMeta{})
	if err != nil {
		return nil, err
	}
	dummyRow := timeseries.TsRecord[any]{Timestamp: time.Now(), Value: nil}
	return valueSupplier(ctx, dummyRow)
}

// Helper function to execute a field and get its metadata for unary tests
func executeUnaryFieldValueMeta(ctx context.Context, f Value) (tsquery.ValueMeta, error) {
	meta, _, err := f.Execute(ctx, tsquery.FieldMeta{})
	return meta, err
}

// errorField is a test helper that returns an error when GetValue is called
type errorField struct {
	meta tsquery.FieldMeta
	err  error
}

func (ef *errorField) Execute(_ context.Context, _ tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	valueSupplier := func(_ context.Context, _ timeseries.TsRecord[any]) (any, error) {
		return nil, ef.err
	}
	valueMeta := tsquery.ValueMeta{
		DataType: ef.meta.DataType(),
		Required: ef.meta.Required(),
	}
	return valueMeta, valueSupplier, nil
}

func TestUnaryNumericOperatorField_IntegerOperations(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

	tests := []struct {
		name     string
		value    int64
		operator tsquery.UnaryNumericOperatorType
		expected int64
	}{
		{"Abs positive", 42, tsquery.UnaryNumericOperatorAbs, 42},
		{"Abs negative", -42, tsquery.UnaryNumericOperatorAbs, 42},
		{"Abs zero", 0, tsquery.UnaryNumericOperatorAbs, 0},
		{"Negate positive", 42, tsquery.UnaryNumericOperatorNegate, -42},
		{"Negate negative", -42, tsquery.UnaryNumericOperatorNegate, 42},
		{"Negate zero", 0, tsquery.UnaryNumericOperatorNegate, 0},
		{"Sqrt 16", 16, tsquery.UnaryNumericOperatorSqrt, 4},
		{"Sqrt 25", 25, tsquery.UnaryNumericOperatorSqrt, 5},
		{"Sqrt 0", 0, tsquery.UnaryNumericOperatorSqrt, 0},
		{"Sqrt 2 (truncated)", 2, tsquery.UnaryNumericOperatorSqrt, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceField := NewConstantFieldValue(meta, tt.value)

			unaryField := NewUnaryNumericOperatorFieldValue(sourceField, tt.operator)
			require.NotNil(t, unaryField)

			result, err := executeUnaryFieldValue(ctx, unaryField)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestUnaryNumericOperatorField_DecimalOperations(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	tests := []struct {
		name     string
		value    float64
		operator tsquery.UnaryNumericOperatorType
		expected float64
		epsilon  float64 // for floating point comparison
	}{
		{"Abs positive", 42.5, tsquery.UnaryNumericOperatorAbs, 42.5, 0},
		{"Abs negative", -42.5, tsquery.UnaryNumericOperatorAbs, 42.5, 0},
		{"Negate positive", 42.5, tsquery.UnaryNumericOperatorNegate, -42.5, 0},
		{"Negate negative", -42.5, tsquery.UnaryNumericOperatorNegate, 42.5, 0},
		{"Sqrt", 16.0, tsquery.UnaryNumericOperatorSqrt, 4.0, 0},
		{"Ceil positive", 42.3, tsquery.UnaryNumericOperatorCeil, 43.0, 0},
		{"Ceil negative", -42.3, tsquery.UnaryNumericOperatorCeil, -42.0, 0},
		{"Floor positive", 42.7, tsquery.UnaryNumericOperatorFloor, 42.0, 0},
		{"Floor negative", -42.7, tsquery.UnaryNumericOperatorFloor, -43.0, 0},
		{"Round up", 42.6, tsquery.UnaryNumericOperatorRound, 43.0, 0},
		{"Round down", 42.4, tsquery.UnaryNumericOperatorRound, 42.0, 0},
		{"Round half to even", 43.5, tsquery.UnaryNumericOperatorRound, 44.0, 0}, // Go rounds to even
		{"Log", math.E, tsquery.UnaryNumericOperatorLog, 1.0, 0.0001},
		{"Log10", 100.0, tsquery.UnaryNumericOperatorLog10, 2.0, 0.0001},
		{"Exp", 1.0, tsquery.UnaryNumericOperatorExp, math.E, 0.0001},
		{"Sin 0", 0.0, tsquery.UnaryNumericOperatorSin, 0.0, 0.0001},
		{"Sin pi/2", math.Pi / 2, tsquery.UnaryNumericOperatorSin, 1.0, 0.0001},
		{"Cos 0", 0.0, tsquery.UnaryNumericOperatorCos, 1.0, 0.0001},
		{"Cos pi", math.Pi, tsquery.UnaryNumericOperatorCos, -1.0, 0.0001},
		{"Tan 0", 0.0, tsquery.UnaryNumericOperatorTan, 0.0, 0.0001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceField := NewConstantFieldValue(meta, tt.value)

			unaryField := NewUnaryNumericOperatorFieldValue(sourceField, tt.operator)
			require.NotNil(t, unaryField)

			result, err := executeUnaryFieldValue(ctx, unaryField)
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
	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

	sourceField := NewConstantFieldValue(meta, int64(42))

	unsupportedOps := []tsquery.UnaryNumericOperatorType{
		tsquery.UnaryNumericOperatorCeil,
		tsquery.UnaryNumericOperatorFloor,
		tsquery.UnaryNumericOperatorRound,
		tsquery.UnaryNumericOperatorLog,
		tsquery.UnaryNumericOperatorLog10,
		tsquery.UnaryNumericOperatorExp,
		tsquery.UnaryNumericOperatorSin,
		tsquery.UnaryNumericOperatorCos,
		tsquery.UnaryNumericOperatorTan,
	}

	for _, op := range unsupportedOps {
		t.Run(string(op), func(t *testing.T) {
			ctx := context.Background()
			unaryField := NewUnaryNumericOperatorFieldValue(sourceField, op)
			_, _, err := unaryField.Execute(ctx, tsquery.FieldMeta{})
			require.Error(t, err)
			require.Contains(t, err.Error(), "unsupported operator")
		})
	}
}

func TestUnaryNumericOperatorField_NonNumericType(t *testing.T) {
	ctx := context.Background()
	stringMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeString, Required: false}

	sourceField := NewConstantFieldValue(stringMeta, "hello")

	unaryField := NewUnaryNumericOperatorFieldValue(sourceField, tsquery.UnaryNumericOperatorAbs)
	_, _, err := unaryField.Execute(ctx, tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-numeric data type")
}

func TestUnaryNumericOperatorField_Meta(t *testing.T) {
	ctx := context.Background()
	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

	sourceField := NewConstantFieldValue(meta, int64(42))

	unaryField := NewUnaryNumericOperatorFieldValue(sourceField, tsquery.UnaryNumericOperatorAbs)

	fieldMeta, err := executeUnaryFieldValueMeta(ctx, unaryField)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeInteger, fieldMeta.DataType)
}

func TestUnaryNumericOperatorField_ErrorPropagation(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	errorField := &errorField{meta: *meta, err: fmt.Errorf("operand error")}

	unaryField := NewUnaryNumericOperatorFieldValue(errorField, tsquery.UnaryNumericOperatorAbs)

	_, err = executeUnaryFieldValue(ctx, unaryField)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed getting value for operand")
}

func TestUnaryNumericOperatorField_NilHandling(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

	sourceField := NewConstantFieldValue(meta, nil)

	unaryField := NewUnaryNumericOperatorFieldValue(sourceField, tsquery.UnaryNumericOperatorAbs)

	result, err := executeUnaryFieldValue(ctx, unaryField)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestUnaryNumericOperatorField_ChainedOperations(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	// Start with -16.0, apply abs -> 16.0, then sqrt -> 4.0
	sourceField := NewConstantFieldValue(meta, -16.0)

	absField := NewUnaryNumericOperatorFieldValue(sourceField, tsquery.UnaryNumericOperatorAbs)

	sqrtField := NewUnaryNumericOperatorFieldValue(absField, tsquery.UnaryNumericOperatorSqrt)

	result, err := executeUnaryFieldValue(ctx, sqrtField)
	require.NoError(t, err)
	require.Equal(t, 4.0, result)
}

func TestUnaryNumericOperatorField_CombinedWithBinaryOperations(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

	// Calculate: abs(-5) + abs(-3) = 5 + 3 = 8
	field1 := NewConstantFieldValue(meta, int64(-5))

	field2 := NewConstantFieldValue(meta, int64(-3))

	abs1 := NewUnaryNumericOperatorFieldValue(field1, tsquery.UnaryNumericOperatorAbs)

	abs2 := NewUnaryNumericOperatorFieldValue(field2, tsquery.UnaryNumericOperatorAbs)

	sumField := NewNumericExpressionFieldValue(abs1, tsquery.BinaryNumericOperatorAdd, abs2)

	result, err := executeUnaryFieldValue(ctx, sumField)
	require.NoError(t, err)
	require.Equal(t, int64(8), result)
}

func TestUnaryNumericOperatorField_SpecialMathValues(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	tests := []struct {
		name     string
		value    float64
		operator tsquery.UnaryNumericOperatorType
		check    func(t *testing.T, result float64)
	}{
		{
			name:     "Log of 0 is -Inf",
			value:    0.0,
			operator: tsquery.UnaryNumericOperatorLog,
			check: func(t *testing.T, result float64) {
				require.True(t, math.IsInf(result, -1))
			},
		},
		{
			name:     "Sqrt of negative is NaN",
			value:    -1.0,
			operator: tsquery.UnaryNumericOperatorSqrt,
			check: func(t *testing.T, result float64) {
				require.True(t, math.IsNaN(result))
			},
		},
		{
			name:     "Exp of large value",
			value:    10.0,
			operator: tsquery.UnaryNumericOperatorExp,
			check: func(t *testing.T, result float64) {
				require.InDelta(t, 22026.465794806718, result, 0.001)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceField := NewConstantFieldValue(meta, tt.value)

			unaryField := NewUnaryNumericOperatorFieldValue(sourceField, tt.operator)

			result, err := executeUnaryFieldValue(ctx, unaryField)
			require.NoError(t, err)

			tt.check(t, result.(float64))
		})
	}
}
