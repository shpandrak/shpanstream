package report

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSingleFieldFilter_WithRefField(t *testing.T) {
	ctx := context.Background()
	// Test using a RefField to extract a single field from a multi-field result
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b", "field_c"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0, 3.0}, Timestamp: time.Unix(0, 0)},
		{Value: []any{4.0, 5.0, 6.0}, Timestamp: time.Unix(60, 0)},
	}

	// Extract field_b using RefField
	singleFieldFilter := NewSingleFieldFilter(NewRefFieldValue("field_b"), tsquery.AddFieldMeta{Urn: "field_b"})
	result, err := singleFieldFilter.Filter(ctx, NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	// Verify field metadata - should have only one field
	require.Len(t, result.FieldsMeta(), 1)
	require.Equal(t, "field_b", result.FieldsMeta()[0].Urn())

	// Verify values
	collectedRecords := result.Stream().MustCollect()
	require.Len(t, collectedRecords, 2)
	require.Equal(t, []any{2.0}, collectedRecords[0].Value)
	require.Equal(t, []any{5.0}, collectedRecords[1].Value)
}

func TestSingleFieldFilter_WithConstantField(t *testing.T) {
	ctx := context.Background()
	// Test using a ConstantField to create a single constant field
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0}, Timestamp: time.Unix(0, 0)},
		{Value: []any{3.0, 4.0}, Timestamp: time.Unix(60, 0)},
		{Value: []any{5.0, 6.0}, Timestamp: time.Unix(120, 0)},
	}

	// Create a constant field
	constantValueMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false}
	constantField := NewConstantFieldValue(constantValueMeta, 42.0)

	singleFieldFilter := NewSingleFieldFilter(constantField, tsquery.AddFieldMeta{Urn: "constant"})
	result, err := singleFieldFilter.Filter(ctx, NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	// Verify field metadata
	require.Len(t, result.FieldsMeta(), 1)
	require.Equal(t, "constant", result.FieldsMeta()[0].Urn())

	// Verify all records have the constant value
	collectedRecords := result.Stream().MustCollect()
	require.Len(t, collectedRecords, 3)
	require.Equal(t, []any{42.0}, collectedRecords[0].Value)
	require.Equal(t, []any{42.0}, collectedRecords[1].Value)
	require.Equal(t, []any{42.0}, collectedRecords[2].Value)
}

func TestSingleFieldFilter_FirstField(t *testing.T) {
	ctx := context.Background()
	// Test extracting the first field
	fieldsMeta := createFieldsMeta(t, []string{"first", "second", "third"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{10.0, 20.0, 30.0}, Timestamp: time.Unix(0, 0)},
	}

	singleFieldFilter := NewSingleFieldFilter(NewRefFieldValue("first"), tsquery.AddFieldMeta{Urn: "first"})
	result, err := singleFieldFilter.Filter(ctx, NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	require.Len(t, result.FieldsMeta(), 1)
	require.Equal(t, "first", result.FieldsMeta()[0].Urn())

	collectedRecords := result.Stream().MustCollect()
	require.Equal(t, []any{10.0}, collectedRecords[0].Value)
}

func TestSingleFieldFilter_LastField(t *testing.T) {
	ctx := context.Background()
	// Test extracting the last field
	fieldsMeta := createFieldsMeta(t, []string{"first", "second", "third"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{10.0, 20.0, 30.0}, Timestamp: time.Unix(0, 0)},
	}

	singleFieldFilter := NewSingleFieldFilter(NewRefFieldValue("third"), tsquery.AddFieldMeta{Urn: "third"})
	result, err := singleFieldFilter.Filter(ctx, NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	require.Len(t, result.FieldsMeta(), 1)
	require.Equal(t, "third", result.FieldsMeta()[0].Urn())

	collectedRecords := result.Stream().MustCollect()
	require.Equal(t, []any{30.0}, collectedRecords[0].Value)
}

func TestSingleFieldFilter_PreservesTimestamps(t *testing.T) {
	ctx := context.Background()
	// Test that timestamps are preserved
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b"})
	timestamps := []time.Time{
		time.Unix(100, 0),
		time.Unix(200, 0),
		time.Unix(300, 0),
	}
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0}, Timestamp: timestamps[0]},
		{Value: []any{3.0, 4.0}, Timestamp: timestamps[1]},
		{Value: []any{5.0, 6.0}, Timestamp: timestamps[2]},
	}

	singleFieldFilter := NewSingleFieldFilter(NewRefFieldValue("field_a"), tsquery.AddFieldMeta{Urn: "field_a"})
	result, err := singleFieldFilter.Filter(ctx, NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	collectedRecords := result.Stream().MustCollect()
	require.Len(t, collectedRecords, 3)
	require.Equal(t, timestamps[0], collectedRecords[0].Timestamp)
	require.Equal(t, timestamps[1], collectedRecords[1].Timestamp)
	require.Equal(t, timestamps[2], collectedRecords[2].Timestamp)
}

func TestSingleFieldFilter_EmptyStream(t *testing.T) {
	ctx := context.Background()
	// Test with an empty stream
	singleFieldFilter := NewSingleFieldFilter(NewRefFieldValue("field_a"), tsquery.AddFieldMeta{Urn: "field_a"})
	result, err := singleFieldFilter.Filter(ctx, NewResult(createFieldsMeta(
		t,
		[]string{"field_a", "field_b"},
	), stream.Empty[timeseries.TsRecord[[]any]]()))
	require.NoError(t, err)

	require.Len(t, result.FieldsMeta(), 1)
	collectedRecords := result.Stream().MustCollect()
	require.Len(t, collectedRecords, 0)
}

func TestSingleFieldFilter_ErrorOnNonExistentField(t *testing.T) {
	ctx := context.Background()
	// Test that referencing a non-existent field returns an error
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0}, Timestamp: time.Unix(0, 0)},
	}

	singleFieldFilter := NewSingleFieldFilter(NewRefFieldValue("field_x"), tsquery.AddFieldMeta{Urn: "field_x"})
	_, err := singleFieldFilter.Filter(ctx, NewResult(fieldsMeta, stream.Just(records...)))
	require.Error(t, err)
	require.Contains(t, err.Error(), " not found time series")
}

func TestSingleFieldFilter_ManyRecords(t *testing.T) {
	ctx := context.Background()
	// Test with many records to ensure efficiency
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b", "field_c"})

	// Generate 1000 records
	numRecords := 1000
	records := make([]timeseries.TsRecord[[]any], numRecords)
	for i := 0; i < numRecords; i++ {
		records[i] = timeseries.TsRecord[[]any]{
			Value:     []any{float64(i), float64(i * 2), float64(i * 3)},
			Timestamp: time.Unix(int64(i), 0),
		}
	}

	singleFieldFilter := NewSingleFieldFilter(NewRefFieldValue("field_b"), tsquery.AddFieldMeta{Urn: "field_b"})
	result, err := singleFieldFilter.Filter(ctx, NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	require.Len(t, result.FieldsMeta(), 1)

	collectedRecords := result.Stream().MustCollect()
	require.Len(t, collectedRecords, numRecords)

	// Spot check values
	require.Equal(t, []any{0.0}, collectedRecords[0].Value)
	require.Equal(t, []any{200.0}, collectedRecords[100].Value)
	require.Equal(t, []any{1998.0}, collectedRecords[999].Value)
}

func TestSingleFieldFilter_WithCustomField(t *testing.T) {
	ctx := context.Background()
	// Test with a custom field implementation
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0}, Timestamp: time.Unix(0, 0)},
		{Value: []any{3.0, 4.0}, Timestamp: time.Unix(60, 0)},
	}

	// Create a custom field that doubles the first field
	customField := &mockField{
		valueMeta: tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false},
		valueFunc: func(ctx context.Context, record timeseries.TsRecord[[]any]) (any, error) {
			firstValue := record.Value[0].(float64)
			return firstValue * 2.0, nil
		},
	}

	singleFieldFilter := NewSingleFieldFilter(customField, tsquery.AddFieldMeta{Urn: "doubled"})
	result, err := singleFieldFilter.Filter(ctx, NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	require.Len(t, result.FieldsMeta(), 1)
	require.Equal(t, "doubled", result.FieldsMeta()[0].Urn())

	collectedRecords := result.Stream().MustCollect()
	require.Len(t, collectedRecords, 2)
	require.Equal(t, []any{2.0}, collectedRecords[0].Value) // 1.0 * 2
	require.Equal(t, []any{6.0}, collectedRecords[1].Value) // 3.0 * 2
}

func TestSingleFieldFilter_ErrorInValueSupplier(t *testing.T) {
	ctx := context.Background()
	// Test that errors in value supplier are propagated
	fieldsMeta := createFieldsMeta(t, []string{"field_a"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0}, Timestamp: time.Unix(0, 0)},
	}

	// Create a field that returns an error
	errorField := &mockField{
		valueMeta: tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: false},
		valueFunc: func(ctx context.Context, record timeseries.TsRecord[[]any]) (any, error) {
			return nil, fmt.Errorf("intentional error")
		},
	}

	singleFieldFilter := NewSingleFieldFilter(errorField, tsquery.AddFieldMeta{Urn: "error_field"})
	result, err := singleFieldFilter.Filter(ctx, NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err) // Filter itself doesn't error

	// Error occurs during stream consumption
	_, err = result.Stream().Collect(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "intentional error")
}

func TestSingleFieldFilter_WithDifferentDataTypes(t *testing.T) {
	ctx := context.Background()
	// Test that different data types are preserved
	testCases := []struct {
		name     string
		dataType tsquery.DataType
		value    any
	}{
		{"integer", tsquery.DataTypeInteger, int64(42)},
		{"decimal", tsquery.DataTypeDecimal, 3.14},
		{"string", tsquery.DataTypeString, "hello"},
		{"boolean", tsquery.DataTypeBoolean, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			valueMeta := tsquery.ValueMeta{DataType: tc.dataType, Required: false}
			constantField := NewConstantFieldValue(valueMeta, tc.value)

			// Create a dummy input result (the constant field ignores it)
			dummyMeta := createFieldsMeta(t, []string{"dummy"})
			records := []timeseries.TsRecord[[]any]{
				{Value: []any{1.0}, Timestamp: time.Unix(0, 0)},
			}

			singleFieldFilter := NewSingleFieldFilter(constantField, tsquery.AddFieldMeta{Urn: "field"})
			result, err := singleFieldFilter.Filter(ctx, NewResult(dummyMeta, stream.Just(records...)))
			require.NoError(t, err)

			require.Len(t, result.FieldsMeta(), 1)
			require.Equal(t, tc.dataType, result.FieldsMeta()[0].DataType())

			collectedRecords := result.Stream().MustCollect()
			require.Len(t, collectedRecords, 1)
			require.Equal(t, []any{tc.value}, collectedRecords[0].Value)
		})
	}
}

// --- Helper Types and Functions ---

// mockField is a simple mock implementation of field.Value for testing
type mockField struct {
	valueMeta tsquery.ValueMeta
	valueFunc func(ctx context.Context, record timeseries.TsRecord[[]any]) (any, error)
}

func (m *mockField) Execute(ctx context.Context, fieldsMeta []tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	return m.valueMeta, m.valueFunc, nil
}
