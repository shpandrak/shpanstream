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

// Helper function to execute a field and get its value for nvl tests
func executeFieldValue(ctx context.Context, f Value) (any, error) {
	_, valueSupplier, err := f.Execute(ctx, tsquery.FieldMeta{})
	if err != nil {
		return nil, err
	}
	dummyRow := timeseries.TsRecord[any]{Timestamp: time.Now(), Value: nil}
	return valueSupplier(ctx, dummyRow)
}

// Helper function to execute a field and get its metadata for nvl tests
func executeFieldValueMeta(ctx context.Context, f Value) (tsquery.ValueMeta, error) {
	meta, _, err := f.Execute(ctx, tsquery.FieldMeta{})
	return meta, err
}

func TestNvlField_SourceHasValue(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	requiredMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}

	// Source has value (42), alternative is 100
	sourceField := NewConstantFieldValue(meta, int64(42))

	altField := NewConstantFieldValue(requiredMeta, int64(100))

	nvlField := NewNvlFieldValue(sourceField, altField)
	require.NotNil(t, nvlField)

	// Should return source value
	result, err := executeFieldValue(ctx, nvlField)
	require.NoError(t, err)
	require.Equal(t, int64(42), result)
}

func TestNvlField_SourceIsNull(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	requiredMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}

	// Source is null, alternative is 100
	sourceField := NewConstantFieldValue(meta, nil)

	altField := NewConstantFieldValue(requiredMeta, int64(100))

	nvlField := NewNvlFieldValue(sourceField, altField)

	// Should return alternative value
	result, err := executeFieldValue(ctx, nvlField)
	require.NoError(t, err)
	require.Equal(t, int64(100), result)
}

func TestNvlField_SourceRequired(t *testing.T) {
	ctx := context.Background()

	requiredMeta1 := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}
	requiredMeta2 := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}

	// Both source and alternative are required
	sourceField := NewConstantFieldValue(requiredMeta1, int64(42))

	altField := NewConstantFieldValue(requiredMeta2, int64(100))

	nvlField := NewNvlFieldValue(sourceField, altField)

	// Should take an optimized path and return source value directly
	result, err := executeFieldValue(ctx, nvlField)
	require.NoError(t, err)
	require.Equal(t, int64(42), result)
}

func TestNvlField_DifferentDataTypes(t *testing.T) {
	tests := []struct {
		name       string
		sourceType tsquery.DataType
		altType    tsquery.DataType
	}{
		{"Integer vs Decimal", tsquery.DataTypeInteger, tsquery.DataTypeDecimal},
		{"Integer vs String", tsquery.DataTypeInteger, tsquery.DataTypeString},
		{"Decimal vs String", tsquery.DataTypeDecimal, tsquery.DataTypeString},
		{"String vs Boolean", tsquery.DataTypeString, tsquery.DataTypeBoolean},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceMeta := tsquery.ValueMeta{DataType: tt.sourceType, Required: false}
			altMeta := tsquery.ValueMeta{DataType: tt.altType, Required: true}

			// Create dummy values based on type
			var sourceVal, altVal any
			switch tt.sourceType {
			case tsquery.DataTypeInteger:
				sourceVal = int64(42)
			case tsquery.DataTypeDecimal:
				sourceVal = 42.0
			case tsquery.DataTypeString:
				sourceVal = "test"
			case tsquery.DataTypeBoolean:
				sourceVal = true
			}

			switch tt.altType {
			case tsquery.DataTypeInteger:
				altVal = int64(100)
			case tsquery.DataTypeDecimal:
				altVal = 100.0
			case tsquery.DataTypeString:
				altVal = "alt"
			case tsquery.DataTypeBoolean:
				altVal = false
			}

			ctx := context.Background()
			sourceField := NewConstantFieldValue(sourceMeta, sourceVal)

			altField := NewConstantFieldValue(altMeta, altVal)

			nvlField := NewNvlFieldValue(sourceField, altField)
			_, _, err := nvlField.Execute(ctx, tsquery.FieldMeta{})
			require.Error(t, err)
			require.Contains(t, err.Error(), "incompatible datatypes")
		})
	}
}

func TestNvlField_AlternativeNotRequired(t *testing.T) {
	ctx := context.Background()
	sourceMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	altMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

	sourceField := NewConstantFieldValue(sourceMeta, nil)

	altField := NewConstantFieldValue(altMeta, int64(100))

	nvlField := NewNvlFieldValue(sourceField, altField)
	_, _, err := nvlField.Execute(ctx, tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "alternative field")
	require.Contains(t, err.Error(), "must be required")
}

func TestNvlField_Meta(t *testing.T) {
	ctx := context.Background()
	sourceMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	altMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}

	sourceField := NewConstantFieldValue(sourceMeta, nil)

	altField := NewConstantFieldValue(altMeta, int64(100))

	nvlField := NewNvlFieldValue(sourceField, altField)

	fieldMeta, err := executeFieldValueMeta(ctx, nvlField)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeInteger, fieldMeta.DataType)
	require.True(t, fieldMeta.Required) // NVL field is always required
}

func TestNvlField_SourceErrorPropagation(t *testing.T) {
	ctx := context.Background()

	requiredMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}

	// Source field returns error
	sourceMeta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)
	sourceField := &errorField{meta: *sourceMeta, err: fmt.Errorf("source error")}

	altField := NewConstantFieldValue(requiredMeta, int64(100))

	nvlField := NewNvlFieldValue(sourceField, altField)

	_, err = executeFieldValue(ctx, nvlField)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed getting value from source field")
}

func TestNvlField_AlternativeErrorPropagation(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

	// Source is null, so an alternative will be called
	sourceField := NewConstantFieldValue(meta, nil)

	// Alternative field returns error
	requiredMeta, err := tsquery.NewFieldMeta("field2", tsquery.DataTypeInteger, true)
	require.NoError(t, err)
	altField := &errorField{meta: *requiredMeta, err: fmt.Errorf("alt error")}

	nvlField := NewNvlFieldValue(sourceField, altField)

	_, err = executeFieldValue(ctx, nvlField)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed getting value from alternative field")
}

func TestNvlField_AllDataTypes(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name     string
		dataType tsquery.DataType
		value    any
		altValue any
	}{
		{"Integer", tsquery.DataTypeInteger, int64(42), int64(100)},
		{"Decimal", tsquery.DataTypeDecimal, 42.5, 100.5},
		{"String", tsquery.DataTypeString, "hello", "world"},
		{"Boolean", tsquery.DataTypeBoolean, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceMeta := tsquery.ValueMeta{DataType: tt.dataType, Required: false}
			altMeta := tsquery.ValueMeta{DataType: tt.dataType, Required: true}

			// Test with value
			sourceField := NewConstantFieldValue(sourceMeta, tt.value)

			altField := NewConstantFieldValue(altMeta, tt.altValue)

			nvlField := NewNvlFieldValue(sourceField, altField)

			result, err := executeFieldValue(ctx, nvlField)
			require.NoError(t, err)
			require.Equal(t, tt.value, result)

			// Test with null
			sourceFieldNull := NewConstantFieldValue(sourceMeta, nil)

			nvlFieldNull := NewNvlFieldValue(sourceFieldNull, altField)

			resultNull, err := executeFieldValue(ctx, nvlFieldNull)
			require.NoError(t, err)
			require.Equal(t, tt.altValue, resultNull)
		})
	}
}

func TestNvlField_ChainedNvl(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	requiredMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}

	// NVL(NVL(null, 50), 100)
	// Inner NVL: null -> 50
	// Outer NVL: 50 -> 50
	sourceNull := NewConstantFieldValue(meta, nil)

	alt1 := NewConstantFieldValue(requiredMeta, int64(50))

	innerNvl := NewNvlFieldValue(sourceNull, alt1)

	alt2 := NewConstantFieldValue(requiredMeta, int64(100))

	outerNvl := NewNvlFieldValue(innerNvl, alt2)

	result, err := executeFieldValue(ctx, outerNvl)
	require.NoError(t, err)
	require.Equal(t, int64(50), result)
}

func TestNvlField_CombinedWithOperations(t *testing.T) {
	ctx := context.Background()

	meta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	requiredMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}

	// NVL(null, 10) + 5 = 15
	sourceNull := NewConstantFieldValue(meta, nil)

	alt := NewConstantFieldValue(requiredMeta, int64(10))

	nvlField := NewNvlFieldValue(sourceNull, alt)

	constantFive := NewConstantFieldValue(requiredMeta, int64(5))

	sumField := NewNumericExpressionFieldValue(nvlField, tsquery.BinaryNumericOperatorAdd, constantFive)

	result, err := executeFieldValue(ctx, sumField)
	require.NoError(t, err)
	require.Equal(t, int64(15), result)
}
