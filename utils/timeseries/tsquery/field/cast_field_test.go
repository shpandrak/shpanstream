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

// Helper function to execute a field and get its value for testing
func executeAndGetValue(t *testing.T, f Field, ctx context.Context) (any, error) {
	_, valueSupplier, err := f.Execute([]tsquery.FieldMeta{})
	if err != nil {
		return nil, err
	}
	dummyRow := timeseries.TsRecord[[]any]{Timestamp: time.Now(), Value: []any{}}
	return valueSupplier(ctx, dummyRow)
}

// Helper function to execute a field and get its metadata for testing
func executeAndGetMeta(t *testing.T, f Field, ctx context.Context) (tsquery.FieldMeta, error) {
	meta, _, err := f.Execute([]tsquery.FieldMeta{})
	return meta, err
}

func TestCastField_IntegerToDecimal(t *testing.T) {
	ctx := context.Background()

	intMeta, err := tsquery.NewFieldMeta("source_int", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField := NewConstantField(*intMeta, int64(42))

	castField := NewCastField("casted_decimal", sourceField, tsquery.DataTypeDecimal)
	require.NotNil(t, castField)

	result, err := executeAndGetValue(t, castField, ctx)
	require.NoError(t, err)
	require.Equal(t, float64(42), result)

	// Check metadata
	meta, err := executeAndGetMeta(t, castField, ctx)
	require.NoError(t, err)
	require.Equal(t, "casted_decimal", meta.Urn())
	require.Equal(t, tsquery.DataTypeDecimal, meta.DataType())
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
			sourceField := NewConstantField(*decMeta, tt.input)

			castField := NewCastField("casted_int", sourceField, tsquery.DataTypeInteger)

			result, err := executeAndGetValue(t, castField, ctx)
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
			sourceField := NewConstantField(*intMeta, tt.input)

			castField := NewCastField("casted_string", sourceField, tsquery.DataTypeString)

			result, err := executeAndGetValue(t, castField, ctx)
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
			sourceField := NewConstantField(*decMeta, tt.input)

			castField := NewCastField("casted_string", sourceField, tsquery.DataTypeString)

			result, err := executeAndGetValue(t, castField, ctx)
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
			sourceField := NewConstantField(*stringMeta, tt.input)

			castField := NewCastField("casted_int", sourceField, tsquery.DataTypeInteger)

			result, err := executeAndGetValue(t, castField, ctx)
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
			sourceField := NewConstantField(*stringMeta, tt.input)

			castField := NewCastField("casted_decimal", sourceField, tsquery.DataTypeDecimal)

			result, err := executeAndGetValue(t, castField, ctx)
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

	sourceField := NewConstantField(*intMeta, int64(42))

	// Cast to same type should work (identity function)
	castField := NewCastField("casted_int", sourceField, tsquery.DataTypeInteger)

	result, err := executeAndGetValue(t, castField, ctx)
	require.NoError(t, err)
	require.Equal(t, int64(42), result)
}

func TestCastField_BooleanNotSupported(t *testing.T) {
	boolMeta, err := tsquery.NewFieldMeta("source_bool", tsquery.DataTypeBoolean, false)
	require.NoError(t, err)

	sourceField := NewConstantField(*boolMeta, true)

	// Cast from boolean should fail on Execute
	castField := NewCastField("casted_int", sourceField, tsquery.DataTypeInteger)
	_, _, err = castField.Execute([]tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "boolean")

	// Cast to boolean should also fail
	intMeta, err := tsquery.NewFieldMeta("source_int", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField2 := NewConstantField(*intMeta, int64(42))

	castField2 := NewCastField("casted_bool", sourceField2, tsquery.DataTypeBoolean)
	_, _, err = castField2.Execute([]tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "boolean")
}

func TestCastField_TimestampNotSupported(t *testing.T) {
	// We can't create a ConstantField with timestamp since ValidateData doesn't support it
	// So we'll just test that creating a cast field with timestamp fails at the getCastFunc level
	intMeta, err := tsquery.NewFieldMeta("source_int", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField := NewConstantField(*intMeta, int64(42))

	// Cast to timestamp should fail on Execute
	castField := NewCastField("casted_timestamp", sourceField, tsquery.DataTypeTimestamp)
	_, _, err = castField.Execute([]tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "timestamp")
}

func TestCastField_ErrorPropagation(t *testing.T) {
	ctx := context.Background()

	intMeta, err := tsquery.NewFieldMeta("source_int", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	// Create an error field
	errorField := &testErrorField{meta: *intMeta, err: fmt.Errorf("source error")}

	castField := NewCastField("casted_decimal", errorField, tsquery.DataTypeDecimal)

	_, err = executeAndGetValue(t, castField, ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed getting value from source field")
}

func TestCastField_NilHandling(t *testing.T) {
	ctx := context.Background()

	// Create optional field
	intMeta, err := tsquery.NewFieldMeta("source_int", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	sourceField := NewConstantField(*intMeta, nil)

	castField := NewCastField("casted_decimal", sourceField, tsquery.DataTypeDecimal)

	result, err := executeAndGetValue(t, castField, ctx)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestCastField_ChainedCasts(t *testing.T) {
	ctx := context.Background()

	// Start with string -> int -> decimal
	stringMeta, err := tsquery.NewFieldMeta("source_string", tsquery.DataTypeString, false)
	require.NoError(t, err)

	sourceField := NewConstantField(*stringMeta, "42")

	// First cast: string -> int
	castToInt := NewCastField("cast_to_int", sourceField, tsquery.DataTypeInteger)

	// Second cast: int -> decimal
	castToDecimal := NewCastField("cast_to_decimal", castToInt, tsquery.DataTypeDecimal)

	result, err := executeAndGetValue(t, castToDecimal, ctx)
	require.NoError(t, err)
	require.Equal(t, float64(42), result)
}

// testErrorField is a helper field that always returns an error
type testErrorField struct {
	meta tsquery.FieldMeta
	err  error
}

func (ef *testErrorField) Execute(fieldsMeta []tsquery.FieldMeta) (tsquery.FieldMeta, ValueSupplier, error) {
	valueSupplier := func(_ context.Context, _ timeseries.TsRecord[[]any]) (any, error) {
		return nil, ef.err
	}
	return ef.meta, valueSupplier, nil
}
