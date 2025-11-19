package report

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNewStaticDatasource_ValidInput(t *testing.T) {
	// Create test data
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create field metadata
	fieldsMeta := []tsquery.FieldMeta{
		*mustCreateFieldMeta("temperature", tsquery.DataTypeDecimal),
		*mustCreateFieldMeta("location", tsquery.DataTypeString),
		*mustCreateFieldMeta("count", tsquery.DataTypeInteger),
		*mustCreateFieldMeta("is_active", tsquery.DataTypeBoolean),
	}

	// Create test records
	testRecords := []timeseries.TsRecord[[]any]{
		{
			Timestamp: baseTime,
			Value:     []any{20.5, "Room1", int64(100), true},
		},
		{
			Timestamp: baseTime.Add(1 * time.Hour),
			Value:     []any{21.3, "Room2", int64(150), false},
		},
		{
			Timestamp: baseTime.Add(2 * time.Hour),
			Value:     []any{19.8, "Room1", int64(200), true},
		},
	}

	// Create stream
	testStream := stream.Just(testRecords...)

	// Create datasource
	ds, err := NewStaticDatasource(fieldsMeta, testStream)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Verify field metadata
	resultMeta := ds.result.FieldsMeta()
	require.Len(t, resultMeta, 4)
	assert.Equal(t, "temperature", resultMeta[0].Urn())
	assert.Equal(t, "location", resultMeta[1].Urn())
	assert.Equal(t, "count", resultMeta[2].Urn())
	assert.Equal(t, "is_active", resultMeta[3].Urn())

	// Verify stream data
	records := ds.result.Stream().MustCollect()
	require.Len(t, records, 3)

	// Check first record
	assert.Equal(t, baseTime, records[0].Timestamp)
	assert.Equal(t, 20.5, records[0].Value[0])
	assert.Equal(t, "Room1", records[0].Value[1])
	assert.Equal(t, int64(100), records[0].Value[2])
	assert.Equal(t, true, records[0].Value[3])
}

func TestNewStaticDatasource_EmptyFieldsMeta(t *testing.T) {
	testRecords := []timeseries.TsRecord[[]any]{
		{
			Timestamp: time.Now(),
			Value:     []any{20.5},
		},
	}

	testStream := stream.Just(testRecords...)

	ds, err := NewStaticDatasource([]tsquery.FieldMeta{}, testStream)
	assert.Error(t, err)
	assert.Nil(t, ds)
	assert.Contains(t, err.Error(), "fieldsMeta cannot be empty")
}

func TestNewStaticDatasource_DuplicateURNs(t *testing.T) {
	// Create field metadata with duplicate URNs
	fieldsMeta := []tsquery.FieldMeta{
		*mustCreateFieldMeta("temperature", tsquery.DataTypeDecimal),
		*mustCreateFieldMeta("temperature", tsquery.DataTypeDecimal), // Duplicate!
		*mustCreateFieldMeta("location", tsquery.DataTypeString),
	}

	testRecords := []timeseries.TsRecord[[]any]{
		{
			Timestamp: time.Now(),
			Value:     []any{20.5, 21.0, "Room1"},
		},
	}

	testStream := stream.Just(testRecords...)

	ds, err := NewStaticDatasource(fieldsMeta, testStream)
	assert.Error(t, err)
	assert.Nil(t, ds)
	assert.Contains(t, err.Error(), "duplicate field URN found")
	assert.Contains(t, err.Error(), "temperature")
}

func TestStaticDatasource_Execute_TimeFiltering(t *testing.T) {
	// Create test data spanning 5 hours
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	fieldsMeta := []tsquery.FieldMeta{
		*mustCreateFieldMeta("temperature", tsquery.DataTypeDecimal),
	}

	testRecords := []timeseries.TsRecord[[]any]{
		{Timestamp: baseTime, Value: []any{20.0}},
		{Timestamp: baseTime.Add(1 * time.Hour), Value: []any{21.0}},
		{Timestamp: baseTime.Add(2 * time.Hour), Value: []any{22.0}},
		{Timestamp: baseTime.Add(3 * time.Hour), Value: []any{23.0}},
		{Timestamp: baseTime.Add(4 * time.Hour), Value: []any{24.0}},
	}

	testStream := stream.Just(testRecords...)
	ds, err := NewStaticDatasource(fieldsMeta, testStream)
	require.NoError(t, err)

	ctx := context.Background()

	// Test: Get records from hour 1 to hour 3 (exclusive end)
	result, err := ds.Execute(ctx, baseTime.Add(1*time.Hour), baseTime.Add(3*time.Hour))
	require.NoError(t, err)

	records := result.Stream().MustCollect()
	require.Len(t, records, 2)

	assert.Equal(t, baseTime.Add(1*time.Hour), records[0].Timestamp)
	assert.Equal(t, 21.0, records[0].Value[0])

	assert.Equal(t, baseTime.Add(2*time.Hour), records[1].Timestamp)
	assert.Equal(t, 22.0, records[1].Value[0])
}

func TestStaticDatasource_Execute_EmptyResult(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	fieldsMeta := []tsquery.FieldMeta{
		*mustCreateFieldMeta("temperature", tsquery.DataTypeDecimal),
	}

	testRecords := []timeseries.TsRecord[[]any]{
		{Timestamp: baseTime, Value: []any{20.0}},
	}

	testStream := stream.Just(testRecords...)
	ds, err := NewStaticDatasource(fieldsMeta, testStream)
	require.NoError(t, err)

	ctx := context.Background()

	// Query for a time range that doesn't contain any records
	result, err := ds.Execute(ctx, baseTime.Add(1*time.Hour), baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	records := result.Stream().MustCollect()
	assert.Len(t, records, 0)
}

func TestStaticDatasource_Execute_BoundaryConditions(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	fieldsMeta := []tsquery.FieldMeta{
		*mustCreateFieldMeta("temperature", tsquery.DataTypeDecimal),
	}

	testRecords := []timeseries.TsRecord[[]any]{
		{Timestamp: baseTime, Value: []any{20.0}},
		{Timestamp: baseTime.Add(1 * time.Hour), Value: []any{21.0}},
		{Timestamp: baseTime.Add(2 * time.Hour), Value: []any{22.0}},
	}

	testStream := stream.Just(testRecords...)
	ds, err := NewStaticDatasource(fieldsMeta, testStream)
	require.NoError(t, err)

	ctx := context.Background()

	// Test: from is inclusive
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
	require.NoError(t, err)
	records := result.Stream().MustCollect()
	require.Len(t, records, 1)
	assert.Equal(t, baseTime, records[0].Timestamp)

	// Test: to is exclusive
	testStream2 := stream.Just(testRecords...)
	ds2, err := NewStaticDatasource(fieldsMeta, testStream2)
	require.NoError(t, err)

	result2, err := ds2.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)
	records2 := result2.Stream().MustCollect()
	require.Len(t, records2, 2)
	assert.Equal(t, baseTime, records2[0].Timestamp)
	assert.Equal(t, baseTime.Add(1*time.Hour), records2[1].Timestamp)
}

// Helper function to create field metadata
func mustCreateFieldMeta(urn string, dataType tsquery.DataType) *tsquery.FieldMeta {
	meta, err := tsquery.NewFieldMeta(urn, dataType, false)
	if err != nil {
		panic(err)
	}
	return meta
}
