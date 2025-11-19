package report

import (
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDropFieldsFilter_DropSingleField(t *testing.T) {
	// Test dropping a single field from a 3-field result
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b", "field_c"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0, 3.0}, Timestamp: time.Unix(0, 0)},
		{Value: []any{4.0, 5.0, 6.0}, Timestamp: time.Unix(60, 0)},
	}

	filter := NewDropFieldsFilter("field_b")
	result, err := filter.Filter(NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	// Verify field metadata
	require.Len(t, result.FieldsMeta(), 2)
	require.Equal(t, "field_a", result.FieldsMeta()[0].Urn())
	require.Equal(t, "field_c", result.FieldsMeta()[1].Urn())

	// Verify values
	collectedRecords := result.Stream().MustCollect()
	require.Len(t, collectedRecords, 2)
	require.Equal(t, []any{1.0, 3.0}, collectedRecords[0].Value)
	require.Equal(t, []any{4.0, 6.0}, collectedRecords[1].Value)
}

func TestDropFieldsFilter_DropMultipleFields(t *testing.T) {
	// Test dropping multiple fields
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b", "field_c", "field_d", "field_e"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0, 3.0, 4.0, 5.0}, Timestamp: time.Unix(0, 0)},
		{Value: []any{10.0, 20.0, 30.0, 40.0, 50.0}, Timestamp: time.Unix(60, 0)},
	}

	filter := NewDropFieldsFilter("field_b", "field_d")
	result, err := filter.Filter(NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	// Verify field metadata
	require.Len(t, result.FieldsMeta(), 3)
	require.Equal(t, "field_a", result.FieldsMeta()[0].Urn())
	require.Equal(t, "field_c", result.FieldsMeta()[1].Urn())
	require.Equal(t, "field_e", result.FieldsMeta()[2].Urn())

	// Verify values
	collectedRecords := result.Stream().MustCollect()
	require.Len(t, collectedRecords, 2)
	require.Equal(t, []any{1.0, 3.0, 5.0}, collectedRecords[0].Value)
	require.Equal(t, []any{10.0, 30.0, 50.0}, collectedRecords[1].Value)
}

func TestDropFieldsFilter_DropFirstField(t *testing.T) {
	// Test dropping the first field
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b", "field_c"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0, 3.0}, Timestamp: time.Unix(0, 0)},
	}

	filter := NewDropFieldsFilter("field_a")
	result, err := filter.Filter(NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	// Verify field metadata
	require.Len(t, result.FieldsMeta(), 2)
	require.Equal(t, "field_b", result.FieldsMeta()[0].Urn())
	require.Equal(t, "field_c", result.FieldsMeta()[1].Urn())

	// Verify values
	collectedRecords := result.Stream().MustCollect()
	require.Equal(t, []any{2.0, 3.0}, collectedRecords[0].Value)
}

func TestDropFieldsFilter_DropLastField(t *testing.T) {
	// Test dropping the last field
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b", "field_c"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0, 3.0}, Timestamp: time.Unix(0, 0)},
	}

	filter := NewDropFieldsFilter("field_c")
	result, err := filter.Filter(NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	// Verify field metadata
	require.Len(t, result.FieldsMeta(), 2)
	require.Equal(t, "field_a", result.FieldsMeta()[0].Urn())
	require.Equal(t, "field_b", result.FieldsMeta()[1].Urn())

	// Verify values
	collectedRecords := result.Stream().MustCollect()
	require.Equal(t, []any{1.0, 2.0}, collectedRecords[0].Value)
}

func TestDropFieldsFilter_ErrorOnDropAllFields(t *testing.T) {
	// Test that dropping all fields returns an error
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0}, Timestamp: time.Unix(0, 0)},
	}

	filter := NewDropFieldsFilter("field_a", "field_b")
	_, err := filter.Filter(NewResult(fieldsMeta, stream.Just(records...)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot drop all fields")
}

func TestDropFieldsFilter_ErrorOnNonExistentField(t *testing.T) {
	// Test that dropping a non-existent field returns an error
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b", "field_c"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0, 3.0}, Timestamp: time.Unix(0, 0)},
	}

	filter := NewDropFieldsFilter("field_x")
	_, err := filter.Filter(NewResult(fieldsMeta, stream.Just(records...)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot drop fields, since they do not exist")
	require.Contains(t, err.Error(), "field_x")
}

func TestDropFieldsFilter_ErrorOnMultipleNonExistentFields(t *testing.T) {
	// Test that dropping multiple non-existent fields returns an error listing all missing fields
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b", "field_c"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0, 3.0}, Timestamp: time.Unix(0, 0)},
	}

	filter := NewDropFieldsFilter("field_x", "field_y", "field_b")
	_, err := filter.Filter(NewResult(fieldsMeta, stream.Just(records...)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot drop fields, since they do not exist")
	// Both missing fields should be mentioned
	require.Contains(t, err.Error(), "field_x")
	require.Contains(t, err.Error(), "field_y")
}

func TestDropFieldsFilter_EmptyStream(t *testing.T) {
	// Test that dropping fields from an empty stream works
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b", "field_c"})
	records := []timeseries.TsRecord[[]any]{}

	filter := NewDropFieldsFilter("field_b")
	result, err := filter.Filter(NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	// Verify field metadata
	require.Len(t, result.FieldsMeta(), 2)
	require.Equal(t, "field_a", result.FieldsMeta()[0].Urn())
	require.Equal(t, "field_c", result.FieldsMeta()[1].Urn())

	// Verify stream is empty
	collectedRecords := result.Stream().MustCollect()
	require.Len(t, collectedRecords, 0)
}

func TestDropFieldsFilter_PreservesTimestamps(t *testing.T) {
	// Test that timestamps are preserved correctly
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

	filter := NewDropFieldsFilter("field_b")
	result, err := filter.Filter(NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	// Verify timestamps are preserved
	collectedRecords := result.Stream().MustCollect()
	require.Len(t, collectedRecords, 3)
	require.Equal(t, timestamps[0], collectedRecords[0].Timestamp)
	require.Equal(t, timestamps[1], collectedRecords[1].Timestamp)
	require.Equal(t, timestamps[2], collectedRecords[2].Timestamp)
}

func TestDropFieldsFilter_ManyRecords(t *testing.T) {
	// Test efficiency with many records to ensure pre-allocation is working
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b", "field_c", "field_d"})

	// Generate 1000 records
	numRecords := 1000
	records := make([]timeseries.TsRecord[[]any], numRecords)
	for i := 0; i < numRecords; i++ {
		records[i] = timeseries.TsRecord[[]any]{
			Value:     []any{float64(i), float64(i * 2), float64(i * 3), float64(i * 4)},
			Timestamp: time.Unix(int64(i), 0),
		}
	}

	filter := NewDropFieldsFilter("field_b", "field_d")
	result, err := filter.Filter(NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	// Verify metadata
	require.Len(t, result.FieldsMeta(), 2)

	// Verify all records are processed correctly
	collectedRecords := result.Stream().MustCollect()
	require.Len(t, collectedRecords, numRecords)

	// Spot check a few records
	require.Equal(t, []any{0.0, 0.0}, collectedRecords[0].Value)
	require.Equal(t, []any{100.0, 300.0}, collectedRecords[100].Value)
	require.Equal(t, []any{999.0, 2997.0}, collectedRecords[999].Value)
}

func TestDropFieldsFilter_DropAllButOne(t *testing.T) {
	// Test dropping all fields except one
	fieldsMeta := createFieldsMeta(t, []string{"field_a", "field_b", "field_c", "field_d"})
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, 2.0, 3.0, 4.0}, Timestamp: time.Unix(0, 0)},
		{Value: []any{5.0, 6.0, 7.0, 8.0}, Timestamp: time.Unix(60, 0)},
	}

	filter := NewDropFieldsFilter("field_a", "field_b", "field_d")
	result, err := filter.Filter(NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)

	// Verify only one field remains
	require.Len(t, result.FieldsMeta(), 1)
	require.Equal(t, "field_c", result.FieldsMeta()[0].Urn())

	// Verify values
	collectedRecords := result.Stream().MustCollect()
	require.Len(t, collectedRecords, 2)
	require.Equal(t, []any{3.0}, collectedRecords[0].Value)
	require.Equal(t, []any{7.0}, collectedRecords[1].Value)
}

// --- Helper Functions ---

// createFieldsMeta creates field metadata for testing
func createFieldsMeta(t *testing.T, urns []string) []tsquery.FieldMeta {
	t.Helper()
	fieldsMeta := make([]tsquery.FieldMeta, len(urns))
	for i, urn := range urns {
		fm, err := tsquery.NewFieldMeta(urn, tsquery.DataTypeDecimal, false)
		require.NoError(t, err)
		fieldsMeta[i] = *fm
	}
	return fieldsMeta
}
