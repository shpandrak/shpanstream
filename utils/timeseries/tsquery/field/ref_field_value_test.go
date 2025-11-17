package field

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRefField_SuccessfulReference(t *testing.T) {
	// Create metadata for a result set with multiple fields
	field1Meta, err := tsquery.NewFieldMeta("temperature", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	field2Meta, err := tsquery.NewFieldMeta("humidity", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	field3Meta, err := tsquery.NewFieldMeta("location", tsquery.DataTypeString, true)
	require.NoError(t, err)

	fieldsMeta := []tsquery.FieldMeta{*field1Meta, *field2Meta, *field3Meta}

	// Create a RefField that references the second field (humidity)
	refField := NewRefFieldValue("humidity")

	// Execute the field
	meta, valueSupplier, err := refField.Execute(fieldsMeta)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeInteger, meta.DataType)
	require.True(t, meta.Required)

	// Test the value supplier with a sample row
	ctx := context.Background()
	row := timeseries.TsRecord[[]any]{
		Timestamp: time.Now(),
		Value:     []any{23.5, int64(65), "room1"},
	}

	value, err := valueSupplier(ctx, row)
	require.NoError(t, err)
	require.Equal(t, int64(65), value)
}

func TestRefField_NotFound(t *testing.T) {
	// Create metadata for a result set
	field1Meta, err := tsquery.NewFieldMeta("temperature", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	field2Meta, err := tsquery.NewFieldMeta("humidity", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	fieldsMeta := []tsquery.FieldMeta{*field1Meta, *field2Meta}

	// Create a RefField that references a non-existent field
	refField := NewRefFieldValue("pressure")

	// Execute should fail
	_, _, err = refField.Execute(fieldsMeta)
	require.Error(t, err)
	require.Contains(t, err.Error(), " not found time series")
	require.Contains(t, err.Error(), "pressure")
}

func TestRefField_FirstField(t *testing.T) {
	// Test referencing the first field
	field1Meta, err := tsquery.NewFieldMeta("first", tsquery.DataTypeString, true)
	require.NoError(t, err)

	field2Meta, err := tsquery.NewFieldMeta("second", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	fieldsMeta := []tsquery.FieldMeta{*field1Meta, *field2Meta}

	refField := NewRefFieldValue("first")
	meta, valueSupplier, err := refField.Execute(fieldsMeta)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeString, meta.DataType)

	ctx := context.Background()
	row := timeseries.TsRecord[[]any]{
		Timestamp: time.Now(),
		Value:     []any{"value1", int64(42)},
	}

	value, err := valueSupplier(ctx, row)
	require.NoError(t, err)
	require.Equal(t, "value1", value)
}

func TestRefField_LastField(t *testing.T) {
	// Test referencing the last field
	field1Meta, err := tsquery.NewFieldMeta("first", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	field2Meta, err := tsquery.NewFieldMeta("second", tsquery.DataTypeInteger, true)
	require.NoError(t, err)

	field3Meta, err := tsquery.NewFieldMeta("last", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	fieldsMeta := []tsquery.FieldMeta{*field1Meta, *field2Meta, *field3Meta}

	refField := NewRefFieldValue("last")
	meta, valueSupplier, err := refField.Execute(fieldsMeta)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, meta.DataType)

	ctx := context.Background()
	row := timeseries.TsRecord[[]any]{
		Timestamp: time.Now(),
		Value:     []any{int64(1), int64(2), 99.9},
	}

	value, err := valueSupplier(ctx, row)
	require.NoError(t, err)
	require.Equal(t, 99.9, value)
}

func TestRefField_EmptyFieldsMeta(t *testing.T) {
	// Test with empty fieldsMeta
	fieldsMeta := []tsquery.FieldMeta{}

	refField := NewRefFieldValue("any_field")
	_, _, err := refField.Execute(fieldsMeta)
	require.Error(t, err)
	require.Contains(t, err.Error(), " not found time series")
}

func TestRefField_OptionalField(t *testing.T) {
	// Test that RefField works with optional fields
	optionalMeta, err := tsquery.NewFieldMeta("optional_field", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	fieldsMeta := []tsquery.FieldMeta{*optionalMeta}

	refField := NewRefFieldValue("optional_field")
	meta, valueSupplier, err := refField.Execute(fieldsMeta)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeInteger, meta.DataType)
	require.False(t, meta.Required)

	// Test with nil value
	ctx := context.Background()
	row := timeseries.TsRecord[[]any]{
		Timestamp: time.Now(),
		Value:     []any{nil},
	}

	value, err := valueSupplier(ctx, row)
	require.NoError(t, err)
	require.Nil(t, value)
}

func TestRefField_MultipleFieldsWithSamePrefix(t *testing.T) {
	// Test that RefField matches exact URN, not prefix
	field1Meta, err := tsquery.NewFieldMeta("temp", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	field2Meta, err := tsquery.NewFieldMeta("temperature", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	fieldsMeta := []tsquery.FieldMeta{*field1Meta, *field2Meta}

	refField := NewRefFieldValue("temp")
	meta, valueSupplier, err := refField.Execute(fieldsMeta)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, meta.DataType)

	ctx := context.Background()
	row := timeseries.TsRecord[[]any]{
		Timestamp: time.Now(),
		Value:     []any{20.0, 25.0},
	}

	value, err := valueSupplier(ctx, row)
	require.NoError(t, err)
	require.Equal(t, 20.0, value)
}
