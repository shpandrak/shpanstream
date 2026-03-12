package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func executeConditionAndGetValue(ctx context.Context, f Value) (any, error) {
	_, valueSupplier, err := f.Execute(ctx, tsquery.FieldMeta{})
	if err != nil {
		return nil, err
	}
	dummyRow := timeseries.TsRecord[any]{Timestamp: time.Now(), Value: nil}
	return valueSupplier(ctx, dummyRow)
}

func TestConditionField_IntDecimalComparison(t *testing.T) {
	ctx := context.Background()
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}
	decMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: true}

	tests := []struct {
		name     string
		v1       any
		v1Meta   tsquery.ValueMeta
		v2       any
		v2Meta   tsquery.ValueMeta
		op       tsquery.ConditionOperatorType
		expected bool
	}{
		{"Equals_true", int64(10), intMeta, 10.0, decMeta, tsquery.ConditionOperatorEquals, true},
		{"NotEquals_false", int64(10), intMeta, 10.0, decMeta, tsquery.ConditionOperatorNotEquals, false},
		{"GreaterThan_true", int64(10), intMeta, 5.5, decMeta, tsquery.ConditionOperatorGreaterThan, true},
		{"LessThan_true", int64(5), intMeta, 5.5, decMeta, tsquery.ConditionOperatorLessThan, true},
		{"GreaterEqual_true", int64(10), intMeta, 10.0, decMeta, tsquery.ConditionOperatorGreaterEqual, true},
		{"LessEqual_true", int64(5), intMeta, 5.5, decMeta, tsquery.ConditionOperatorLessEqual, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field1 := NewConstantFieldValue(tt.v1Meta, tt.v1)
			field2 := NewConstantFieldValue(tt.v2Meta, tt.v2)

			cond := NewConditionFieldValue(tt.op, field1, field2)
			result, err := executeConditionAndGetValue(ctx, cond)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConditionField_DecimalIntComparison(t *testing.T) {
	ctx := context.Background()
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}
	decMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: true}

	tests := []struct {
		name     string
		v1       any
		v1Meta   tsquery.ValueMeta
		v2       any
		v2Meta   tsquery.ValueMeta
		op       tsquery.ConditionOperatorType
		expected bool
	}{
		{"Equals_true", 10.0, decMeta, int64(10), intMeta, tsquery.ConditionOperatorEquals, true},
		{"NotEquals_true", 10.5, decMeta, int64(10), intMeta, tsquery.ConditionOperatorNotEquals, true},
		{"GreaterThan_true", 10.5, decMeta, int64(10), intMeta, tsquery.ConditionOperatorGreaterThan, true},
		{"LessThan_true", 4.5, decMeta, int64(5), intMeta, tsquery.ConditionOperatorLessThan, true},
		{"GreaterEqual_true", 10.0, decMeta, int64(10), intMeta, tsquery.ConditionOperatorGreaterEqual, true},
		{"LessEqual_true", 5.0, decMeta, int64(5), intMeta, tsquery.ConditionOperatorLessEqual, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field1 := NewConstantFieldValue(tt.v1Meta, tt.v1)
			field2 := NewConstantFieldValue(tt.v2Meta, tt.v2)

			cond := NewConditionFieldValue(tt.op, field1, field2)
			result, err := executeConditionAndGetValue(ctx, cond)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestConditionField_IntDecimalComparison_OutputMeta(t *testing.T) {
	ctx := context.Background()
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}
	decMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: true}

	field1 := NewConstantFieldValue(intMeta, int64(10))
	field2 := NewConstantFieldValue(decMeta, 5.5)

	cond := NewConditionFieldValue(tsquery.ConditionOperatorEquals, field1, field2)
	meta, _, err := cond.Execute(ctx, tsquery.FieldMeta{})
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeBoolean, meta.DataType)
}

func TestConditionField_NonNumericMismatch_StillErrors(t *testing.T) {
	ctx := context.Background()
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}
	strMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeString, Required: true}

	field1 := NewConstantFieldValue(strMeta, "hello")
	field2 := NewConstantFieldValue(intMeta, int64(10))

	cond := NewConditionFieldValue(tsquery.ConditionOperatorEquals, field1, field2)
	_, _, err := cond.Execute(ctx, tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "operand types do not match")
}

func TestConditionField_IntDecimalComparison_WithOptionalOperands(t *testing.T) {
	ctx := context.Background()
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	decMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

	// nil handling: nil compared to any value returns false
	field1 := NewConstantFieldValue(intMeta, nil)
	field2 := NewConstantFieldValue(decMeta, 5.5)

	cond := NewConditionFieldValue(tsquery.ConditionOperatorEquals, field1, field2)
	result, err := executeConditionAndGetValue(ctx, cond)
	require.NoError(t, err)
	require.Equal(t, false, result)
}
