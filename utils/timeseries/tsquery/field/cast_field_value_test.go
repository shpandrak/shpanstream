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
func executeAndGetValue(t *testing.T, f Value, ctx context.Context) (any, error) {
	_, valueSupplier, err := f.Execute([]tsquery.FieldMeta{})
	if err != nil {
		return nil, err
	}
	dummyRow := timeseries.TsRecord[[]any]{Timestamp: time.Now(), Value: []any{}}
	return valueSupplier(ctx, dummyRow)
}

// Helper function to execute a field and get its metadata for testing
func executeAndGetMeta(t *testing.T, f Value, ctx context.Context) (ValueMeta, error) {
	meta, _, err := f.Execute([]tsquery.FieldMeta{})
	return meta, err
}

func TestCastField_IntegerToDecimal(t *testing.T) {
	ctx := context.Background()

	intMeta := ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	sourceField := NewConstantFieldValue(intMeta, int64(42))

	castField := NewCastFieldValue(sourceField, tsquery.DataTypeDecimal)
	require.NotNil(t, castField)

	result, err := executeAndGetValue(t, castField, ctx)
	require.NoError(t, err)
	require.Equal(t, float64(42), result)

	// Check metadata
	meta, err := executeAndGetMeta(t, castField, ctx)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, meta.DataType)
}

func TestCastField_DecimalToInteger(t *testing.T) {
	ctx := context.Background()

	decMeta := ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

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
			sourceField := NewConstantFieldValue(decMeta, tt.input)

			castField := NewCastFieldValue(sourceField, tsquery.DataTypeInteger)

			result, err := executeAndGetValue(t, castField, ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCastField_IntegerToString(t *testing.T) {
	ctx := context.Background()

	intMeta := ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

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
			sourceField := NewConstantFieldValue(intMeta, tt.input)

			castField := NewCastFieldValue(sourceField, tsquery.DataTypeString)

			result, err := executeAndGetValue(t, castField, ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCastField_DecimalToString(t *testing.T) {
	ctx := context.Background()

	decMeta := ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}

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
			sourceField := NewConstantFieldValue(decMeta, tt.input)

			castField := NewCastFieldValue(sourceField, tsquery.DataTypeString)

			result, err := executeAndGetValue(t, castField, ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestCastField_StringToInteger(t *testing.T) {
	ctx := context.Background()

	stringMeta := ValueMeta{DataType: tsquery.DataTypeString, Required: false}

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
			sourceField := NewConstantFieldValue(stringMeta, tt.input)

			castField := NewCastFieldValue(sourceField, tsquery.DataTypeInteger)

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

	stringMeta := ValueMeta{DataType: tsquery.DataTypeString, Required: false}

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
			sourceField := NewConstantFieldValue(stringMeta, tt.input)

			castField := NewCastFieldValue(sourceField, tsquery.DataTypeDecimal)

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

	intMeta := ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	sourceField := NewConstantFieldValue(intMeta, int64(42))

	// Cast to same type should work (identity function)
	castField := NewCastFieldValue(sourceField, tsquery.DataTypeInteger)

	result, err := executeAndGetValue(t, castField, ctx)
	require.NoError(t, err)
	require.Equal(t, int64(42), result)
}

func TestCastField_BooleanNotSupported(t *testing.T) {
	boolMeta := ValueMeta{DataType: tsquery.DataTypeBoolean, Required: false}
	sourceField := NewConstantFieldValue(boolMeta, true)

	// Cast from boolean should fail on Execute
	castField := NewCastFieldValue(sourceField, tsquery.DataTypeInteger)
	_, _, err := castField.Execute([]tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "boolean")

	// Cast to boolean should also fail
	intMeta := ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	sourceField2 := NewConstantFieldValue(intMeta, int64(42))

	castField2 := NewCastFieldValue(sourceField2, tsquery.DataTypeBoolean)
	_, _, err = castField2.Execute([]tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "boolean")
}

func TestCastField_TimestampNotSupported(t *testing.T) {
	// We can't create a ConstantField with timestamp since ValidateData doesn't support it
	// So we'll just test that creating a cast field with timestamp fails at the getCastFunc level
	intMeta := ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	sourceField := NewConstantFieldValue(intMeta, int64(42))

	// Cast to timestamp should fail on Execute
	castField := NewCastFieldValue(sourceField, tsquery.DataTypeTimestamp)
	_, _, err := castField.Execute([]tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "timestamp")
}

func TestCastField_ErrorPropagation(t *testing.T) {
	ctx := context.Background()

	intMeta := ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}

	// Create an error field
	errorField := &testErrorField{meta: intMeta, err: fmt.Errorf("source error")}

	castField := NewCastFieldValue(errorField, tsquery.DataTypeDecimal)

	_, err := executeAndGetValue(t, castField, ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed getting value from source field")
}

func TestCastField_NilHandling(t *testing.T) {
	ctx := context.Background()

	// Create optional field
	intMeta := ValueMeta{DataType: tsquery.DataTypeInteger, Required: false}
	sourceField := NewConstantFieldValue(intMeta, nil)

	castField := NewCastFieldValue(sourceField, tsquery.DataTypeDecimal)

	result, err := executeAndGetValue(t, castField, ctx)
	require.NoError(t, err)
	require.Nil(t, result)
}

func TestCastField_ChainedCasts(t *testing.T) {
	ctx := context.Background()

	// Start with string -> int -> decimal
	stringMeta := ValueMeta{DataType: tsquery.DataTypeString, Required: false}
	sourceField := NewConstantFieldValue(stringMeta, "42")

	// First cast: string -> int
	castToInt := NewCastFieldValue(sourceField, tsquery.DataTypeInteger)

	// Second cast: int -> decimal
	castToDecimal := NewCastFieldValue(castToInt, tsquery.DataTypeDecimal)

	result, err := executeAndGetValue(t, castToDecimal, ctx)
	require.NoError(t, err)
	require.Equal(t, float64(42), result)
}

// testErrorField is a helper field that always returns an error
type testErrorField struct {
	meta ValueMeta
	err  error
}

func (ef *testErrorField) Execute(fieldsMeta []tsquery.FieldMeta) (ValueMeta, ValueSupplier, error) {
	valueSupplier := func(_ context.Context, _ timeseries.TsRecord[[]any]) (any, error) {
		return nil, ef.err
	}
	return ef.meta, valueSupplier, nil
}
