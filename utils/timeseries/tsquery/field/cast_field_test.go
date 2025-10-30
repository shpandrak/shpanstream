package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCastField_IntegerToDecimal(t *testing.T) {
	ctx := context.Background()

	intMeta, err := tsquery.NewFieldMeta("source_int", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField, err := NewConstantField(*intMeta, int64(42))
	require.NoError(t, err)

	castField, err := NewCastField("casted_decimal", sourceField, tsquery.DataTypeDecimal)
	require.NoError(t, err)
	require.NotNil(t, castField)

	result, err := castField.GetValue(ctx)
	require.NoError(t, err)
	require.Equal(t, float64(42), result)

	// Check metadata
	require.Equal(t, "casted_decimal", castField.Meta().Urn())
	require.Equal(t, tsquery.DataTypeDecimal, castField.Meta().DataType())
}

func TestCastField_DecimalToInteger(t *testing.T) {
	ctx := context.Background()

	decMeta, err := tsquery.NewFieldMeta("source_dec", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	tests := []struct {
		name     string
		input    float64
		expected int64
	}{
		{"Positive with fraction", 42.7, 42},
		{"Negative with fraction", -42.7, -42},
		{"Whole number", 100.0, 100},
		{"Zero", 0.0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceField, err := NewConstantField(*decMeta, tt.input)
			require.NoError(t, err)

			castField, err := NewCastField("casted_int", sourceField, tsquery.DataTypeInteger)
			require.NoError(t, err)

			result, err := castField.GetValue(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCastField_IntegerToString(t *testing.T) {
	ctx := context.Background()

	intMeta, err := tsquery.NewFieldMeta("source_int", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	tests := []struct {
		name     string
		input    int64
		expected string
	}{
		{"Positive", 42, "42"},
		{"Negative", -42, "-42"},
		{"Zero", 0, "0"},
		{"Large number", 123456789, "123456789"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceField, err := NewConstantField(*intMeta, tt.input)
			require.NoError(t, err)

			castField, err := NewCastField("casted_string", sourceField, tsquery.DataTypeString)
			require.NoError(t, err)

			result, err := castField.GetValue(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCastField_DecimalToString(t *testing.T) {
	ctx := context.Background()

	decMeta, err := tsquery.NewFieldMeta("source_dec", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	tests := []struct {
		name     string
		input    float64
		expected string
	}{
		{"Positive with fraction", 42.5, "42.5"},
		{"Negative with fraction", -42.5, "-42.5"},
		{"Whole number", 100.0, "100"},
		{"Zero", 0.0, "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceField, err := NewConstantField(*decMeta, tt.input)
			require.NoError(t, err)

			castField, err := NewCastField("casted_string", sourceField, tsquery.DataTypeString)
			require.NoError(t, err)

			result, err := castField.GetValue(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCastField_StringToInteger(t *testing.T) {
	ctx := context.Background()

	stringMeta, err := tsquery.NewFieldMeta("source_string", tsquery.DataTypeString, false)
	require.NoError(t, err)

	tests := []struct {
		name      string
		input     string
		expected  int64
		shouldErr bool
	}{
		{"Valid positive", "42", 42, false},
		{"Valid negative", "-42", -42, false},
		{"Valid zero", "0", 0, false},
		{"Invalid - with decimals", "42.5", 0, true},
		{"Invalid - not a number", "abc", 0, true},
		{"Invalid - empty string", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceField, err := NewConstantField(*stringMeta, tt.input)
			require.NoError(t, err)

			castField, err := NewCastField("casted_int", sourceField, tsquery.DataTypeInteger)
			require.NoError(t, err)

			result, err := castField.GetValue(ctx)
			if tt.shouldErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestCastField_StringToDecimal(t *testing.T) {
	ctx := context.Background()

	stringMeta, err := tsquery.NewFieldMeta("source_string", tsquery.DataTypeString, false)
	require.NoError(t, err)

	tests := []struct {
		name      string
		input     string
		expected  float64
		shouldErr bool
	}{
		{"Valid positive integer", "42", 42.0, false},
		{"Valid positive decimal", "42.5", 42.5, false},
		{"Valid negative", "-42.5", -42.5, false},
		{"Valid zero", "0", 0.0, false},
		{"Invalid - not a number", "abc", 0, true},
		{"Invalid - empty string", "", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceField, err := NewConstantField(*stringMeta, tt.input)
			require.NoError(t, err)

			castField, err := NewCastField("casted_decimal", sourceField, tsquery.DataTypeDecimal)
			require.NoError(t, err)

			result, err := castField.GetValue(ctx)
			if tt.shouldErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestCastField_SameType(t *testing.T) {
	ctx := context.Background()

	intMeta, err := tsquery.NewFieldMeta("source_int", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField, err := NewConstantField(*intMeta, int64(42))
	require.NoError(t, err)

	// Cast to same type should work (identity function)
	castField, err := NewCastField("casted_int", sourceField, tsquery.DataTypeInteger)
	require.NoError(t, err)

	result, err := castField.GetValue(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(42), result)
}

func TestCastField_BooleanNotSupported(t *testing.T) {
	boolMeta, err := tsquery.NewFieldMeta("source_bool", tsquery.DataTypeBoolean, false)
	require.NoError(t, err)

	sourceField, err := NewConstantField(*boolMeta, true)
	require.NoError(t, err)

	// Cast from boolean should fail
	castField, err := NewCastField("casted_int", sourceField, tsquery.DataTypeInteger)
	require.Error(t, err)
	require.Nil(t, castField)
	require.Contains(t, err.Error(), "boolean")

	// Cast to boolean should also fail
	intMeta, err := tsquery.NewFieldMeta("source_int", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField2, err := NewConstantField(*intMeta, int64(42))
	require.NoError(t, err)

	castField2, err := NewCastField("casted_bool", sourceField2, tsquery.DataTypeBoolean)
	require.Error(t, err)
	require.Nil(t, castField2)
	require.Contains(t, err.Error(), "boolean")
}

func TestCastField_TimestampNotSupported(t *testing.T) {
	// We can't create a ConstantField with timestamp since ValidateData doesn't support it
	// So we'll just test that creating a cast field with timestamp fails at the getCastFunc level
	intMeta, err := tsquery.NewFieldMeta("source_int", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField, err := NewConstantField(*intMeta, int64(42))
	require.NoError(t, err)

	// Cast to timestamp should fail
	castField, err := NewCastField("casted_timestamp", sourceField, tsquery.DataTypeTimestamp)
	require.Error(t, err)
	require.Nil(t, castField)
	require.Contains(t, err.Error(), "timestamp")
}

func TestCastField_ErrorPropagation(t *testing.T) {
	ctx := context.Background()

	intMeta, err := tsquery.NewFieldMeta("source_int", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	// Create an error field
	errorField := &errorField{meta: *intMeta, err: fmt.Errorf("source error")}

	castField, err := NewCastField("casted_decimal", errorField, tsquery.DataTypeDecimal)
	require.NoError(t, err)

	_, err = castField.GetValue(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed getting value from source field")
}

func TestCastField_NilHandling(t *testing.T) {
	ctx := context.Background()

	// Create optional field
	intMeta, err := tsquery.NewFieldMeta("source_int", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField, err := NewConstantField(*intMeta, nil)
	require.NoError(t, err)

	castField, err := NewCastField("casted_decimal", sourceField, tsquery.DataTypeDecimal)
	require.NoError(t, err)

	result, err := castField.GetValue(ctx)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestCastField_ChainedCasts(t *testing.T) {
	ctx := context.Background()

	// Start with string -> int -> decimal
	stringMeta, err := tsquery.NewFieldMeta("source_string", tsquery.DataTypeString, false)
	require.NoError(t, err)

	sourceField, err := NewConstantField(*stringMeta, "42")
	require.NoError(t, err)

	// First cast: string -> int
	castToInt, err := NewCastField("cast_to_int", sourceField, tsquery.DataTypeInteger)
	require.NoError(t, err)

	// Second cast: int -> decimal
	castToDecimal, err := NewCastField("cast_to_decimal", castToInt, tsquery.DataTypeDecimal)
	require.NoError(t, err)

	result, err := castToDecimal.GetValue(ctx)
	require.NoError(t, err)
	require.Equal(t, float64(42), result)
}
