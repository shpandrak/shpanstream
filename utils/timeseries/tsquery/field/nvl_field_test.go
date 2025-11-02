package field

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
func executeFieldValue(t *testing.T, f Field, ctx context.Context) (any, error) {
	_, valueSupplier, err := f.Execute([]tsquery.FieldMeta{})
	if err != nil {
		return nil, err
	}
	dummyRow := timeseries.TsRecord[[]any]{Timestamp: time.Now(), Value: []any{}}
	return valueSupplier(ctx, dummyRow)
}

// Helper function to execute a field and get its metadata for nvl tests
func executeFieldMeta(t *testing.T, f Field, ctx context.Context) (tsquery.FieldMeta, error) {
	meta, _, err := f.Execute([]tsquery.FieldMeta{})
	return meta, err
}

func TestNvlField_SourceHasValue(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	requiredMeta, err := tsquery.NewFieldMeta("field2", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	// Source has value (42), alternative is 100
	sourceField := NewConstantField(*meta, int64(42))
	require.NoError(t, err)

	altField := NewConstantField(*requiredMeta, int64(100))
	require.NoError(t, err)

	nvlField := NewNvlField("nvl_result", sourceField, altField)
	require.NoError(t, err)
	require.NotNil(t, nvlField)

	// Should return source value
	result, err := executeFieldValue(t, nvlField, ctx)
	require.NoError(t, err)
	require.Equal(t, int64(42), result)
}

func TestNvlField_SourceIsNull(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	requiredMeta, err := tsquery.NewFieldMeta("field2", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	// Source is null, alternative is 100
	sourceField := NewConstantField(*meta, nil)
	require.NoError(t, err)

	altField := NewConstantField(*requiredMeta, int64(100))
	require.NoError(t, err)

	nvlField := NewNvlField("nvl_result", sourceField, altField)
	require.NoError(t, err)

	// Should return alternative value
	result, err := executeFieldValue(t, nvlField, ctx)
	require.NoError(t, err)
	require.Equal(t, int64(100), result)
}

func TestNvlField_SourceRequired(t *testing.T) {
	ctx := context.Background()

	requiredMeta1, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	requiredMeta2, err := tsquery.NewFieldMeta("field2", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	// Both source and alternative are required
	sourceField := NewConstantField(*requiredMeta1, int64(42))
	require.NoError(t, err)

	altField := NewConstantField(*requiredMeta2, int64(100))
	require.NoError(t, err)

	nvlField := NewNvlField("nvl_result", sourceField, altField)
	require.NoError(t, err)

	// Should take optimized path and return source value directly
	result, err := executeFieldValue(t, nvlField, ctx)
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
			sourceMeta, err := tsquery.NewFieldMeta("source", tt.sourceType, false)
			require.NoError(t, err)

			altMeta, err := tsquery.NewFieldMeta("alt", tt.altType, true)
			require.NoError(t, err)

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

			sourceField := NewConstantField(*sourceMeta, sourceVal)

			altField := NewConstantField(*altMeta, altVal)

			nvlField := NewNvlField("nvl_result", sourceField, altField)
			_, _, err = nvlField.Execute([]tsquery.FieldMeta{})
			require.Error(t, err)
			require.Contains(t, err.Error(), "incompatible datatypes")
		})
	}
}

func TestNvlField_AlternativeNotRequired(t *testing.T) {
	sourceMeta, err := tsquery.NewFieldMeta("source", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	altMeta, err := tsquery.NewFieldMeta("alt", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField := NewConstantField(*sourceMeta, nil)

	altField := NewConstantField(*altMeta, int64(100))

	nvlField := NewNvlField("nvl_result", sourceField, altField)
	_, _, err = nvlField.Execute([]tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "alternative field")
	require.Contains(t, err.Error(), "must be required")
}

func TestNvlField_Meta(t *testing.T) {
	ctx := context.Background()
	sourceMeta, err := tsquery.NewFieldMeta("source", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	altMeta, err := tsquery.NewFieldMeta("alt", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	sourceField := NewConstantField(*sourceMeta, nil)
	require.NoError(t, err)

	altField := NewConstantField(*altMeta, int64(100))
	require.NoError(t, err)

	nvlField := NewNvlField("nvl_result", sourceField, altField)
	require.NoError(t, err)

	fieldMeta, err := executeFieldMeta(t, nvlField, ctx)
	require.NoError(t, err)
	require.Equal(t, "nvl_result", fieldMeta.Urn())
	require.Equal(t, tsquery.DataTypeInteger, fieldMeta.DataType())
	require.True(t, fieldMeta.Required()) // NVL field is always required
}

func TestNvlField_SourceErrorPropagation(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	requiredMeta, err := tsquery.NewFieldMeta("field2", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	// Source field returns error
	sourceField := &errorField{meta: *meta, err: fmt.Errorf("source error")}

	altField := NewConstantField(*requiredMeta, int64(100))
	require.NoError(t, err)

	nvlField := NewNvlField("nvl_result", sourceField, altField)
	require.NoError(t, err)

	_, err = executeFieldValue(t, nvlField, ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed getting value from source field")
}

func TestNvlField_AlternativeErrorPropagation(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	requiredMeta, err := tsquery.NewFieldMeta("field2", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	// Source is null, so alternative will be called
	sourceField := NewConstantField(*meta, nil)
	require.NoError(t, err)

	// Alternative field returns error
	altField := &errorField{meta: *requiredMeta, err: fmt.Errorf("alt error")}

	nvlField := NewNvlField("nvl_result", sourceField, altField)
	require.NoError(t, err)

	_, err = executeFieldValue(t, nvlField, ctx)
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
			sourceMeta, err := tsquery.NewFieldMeta("source", tt.dataType, false)
			require.NoError(t, err)

			altMeta, err := tsquery.NewFieldMeta("alt", tt.dataType, true)
			require.NoError(t, err)

			// Test with value
			sourceField := NewConstantField(*sourceMeta, tt.value)
			require.NoError(t, err)

			altField := NewConstantField(*altMeta, tt.altValue)
			require.NoError(t, err)

			nvlField := NewNvlField("nvl_result", sourceField, altField)
			require.NoError(t, err)

			result, err := executeFieldValue(t, nvlField, ctx)
			require.NoError(t, err)
			require.Equal(t, tt.value, result)

			// Test with null
			sourceFieldNull := NewConstantField(*sourceMeta, nil)
			require.NoError(t, err)

			nvlFieldNull := NewNvlField("nvl_result_null", sourceFieldNull, altField)
			require.NoError(t, err)

			resultNull, err := executeFieldValue(t, nvlFieldNull, ctx)
			require.NoError(t, err)
			require.Equal(t, tt.altValue, resultNull)
		})
	}
}

func TestNvlField_ChainedNvl(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	requiredMeta, err := tsquery.NewFieldMeta("required_field", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	// NVL(NVL(null, 50), 100)
	// Inner NVL: null -> 50
	// Outer NVL: 50 -> 50
	sourceNull := NewConstantField(*meta, nil)
	require.NoError(t, err)

	alt1 := NewConstantField(*requiredMeta, int64(50))
	require.NoError(t, err)

	innerNvl := NewNvlField("inner_nvl", sourceNull, alt1)
	require.NoError(t, err)

	alt2 := NewConstantField(*requiredMeta, int64(100))
	require.NoError(t, err)

	outerNvl := NewNvlField("outer_nvl", innerNvl, alt2)
	require.NoError(t, err)

	result, err := executeFieldValue(t, outerNvl, ctx)
	require.NoError(t, err)
	require.Equal(t, int64(50), result)
}

func TestNvlField_CombinedWithOperations(t *testing.T) {
	ctx := context.Background()

	meta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	requiredMeta, err := tsquery.NewFieldMeta("required_field", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	// NVL(null, 10) + 5 = 15
	sourceNull := NewConstantField(*meta, nil)
	require.NoError(t, err)

	alt := NewConstantField(*requiredMeta, int64(10))
	require.NoError(t, err)

	nvlField := NewNvlField("nvl_result", sourceNull, alt)
	require.NoError(t, err)

	constantFive := NewConstantField(*requiredMeta, int64(5))
	require.NoError(t, err)

	sumField := NewNumericExpressionField("sum", nvlField, BinaryNumericOperatorAdd, constantFive)
	require.NoError(t, err)

	result, err := executeFieldValue(t, sumField, ctx)
	require.NoError(t, err)
	require.Equal(t, int64(15), result)
}
