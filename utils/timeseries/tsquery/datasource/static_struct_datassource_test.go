package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Test structs

type ValidTestStruct struct {
	Timestamp   time.Time
	Temperature float64
	Location    string
	Count       int64
	IsActive    bool
}

type NoTimestampStruct struct {
	Temperature float64
	Location    string
}

type MultipleTimestampsStruct struct {
	Timestamp1 time.Time
	Timestamp2 time.Time
	Value      int64
}

type InvalidFieldTypeStruct struct {
	Timestamp time.Time
	Value     int32 // Invalid: not int64
}

type UnexportedFieldStruct struct {
	Timestamp   time.Time
	temperature float64 // unexported, should be ignored
	Location    string
}

func TestNewStaticStructDatasource_ValidStruct(t *testing.T) {
	// Create test data
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ValidTestStruct{
		{
			Timestamp:   baseTime,
			Temperature: 20.5,
			Location:    "Room1",
			Count:       100,
			IsActive:    true,
		},
		{
			Timestamp:   baseTime.Add(1 * time.Hour),
			Temperature: 21.3,
			Location:    "Room2",
			Count:       150,
			IsActive:    false,
		},
		{
			Timestamp:   baseTime.Add(2 * time.Hour),
			Temperature: 19.8,
			Location:    "Room1",
			Count:       200,
			IsActive:    true,
		},
	}

	// Create stream
	testStream := stream.Just(testData...)

	// Create datasource
	ds, err := NewStaticStructDatasource(testStream)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Verify field metadata
	fieldsMeta := ds.result.FieldsMeta()
	require.Len(t, fieldsMeta, 4)

	// Check field metadata details
	assert.Equal(t, "ValidTestStruct:Temperature", fieldsMeta[0].Urn())
	assert.Equal(t, tsquery.DataTypeDecimal, fieldsMeta[0].DataType())

	assert.Equal(t, "ValidTestStruct:Location", fieldsMeta[1].Urn())
	assert.Equal(t, tsquery.DataTypeString, fieldsMeta[1].DataType())

	assert.Equal(t, "ValidTestStruct:Count", fieldsMeta[2].Urn())
	assert.Equal(t, tsquery.DataTypeInteger, fieldsMeta[2].DataType())

	assert.Equal(t, "ValidTestStruct:IsActive", fieldsMeta[3].Urn())
	assert.Equal(t, tsquery.DataTypeBoolean, fieldsMeta[3].DataType())

	// Verify stream data
	records := ds.result.Stream().MustCollect()
	require.Len(t, records, 3)

	// Check first record
	assert.Equal(t, baseTime, records[0].Timestamp)
	assert.Equal(t, 20.5, records[0].Value[0])
	assert.Equal(t, "Room1", records[0].Value[1])
	assert.Equal(t, int64(100), records[0].Value[2])
	assert.Equal(t, true, records[0].Value[3])

	// Check second record
	assert.Equal(t, baseTime.Add(1*time.Hour), records[1].Timestamp)
	assert.Equal(t, 21.3, records[1].Value[0])
	assert.Equal(t, "Room2", records[1].Value[1])
	assert.Equal(t, int64(150), records[1].Value[2])
	assert.Equal(t, false, records[1].Value[3])
}

func TestNewStaticStructDatasource_NoTimestampField(t *testing.T) {
	testData := []NoTimestampStruct{
		{Temperature: 20.5, Location: "Room1"},
	}

	testStream := stream.Just(testData...)

	ds, err := NewStaticStructDatasource(testStream)
	assert.Error(t, err)
	assert.Nil(t, ds)
	assert.Contains(t, err.Error(), "must have exactly one public time.Time field")
}

func TestNewStaticStructDatasource_MultipleTimestamps(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []MultipleTimestampsStruct{
		{
			Timestamp1: baseTime,
			Timestamp2: baseTime.Add(1 * time.Hour),
			Value:      100,
		},
	}

	testStream := stream.Just(testData...)

	ds, err := NewStaticStructDatasource(testStream)
	assert.Error(t, err)
	assert.Nil(t, ds)
	assert.Contains(t, err.Error(), "must have exactly one time.Time field")
}

func TestNewStaticStructDatasource_InvalidFieldType(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []InvalidFieldTypeStruct{
		{Timestamp: baseTime, Value: 100},
	}

	testStream := stream.Just(testData...)

	ds, err := NewStaticStructDatasource(testStream)
	assert.Error(t, err)
	assert.Nil(t, ds)
	assert.Contains(t, err.Error(), "has invalid type")
}

func TestNewStaticStructDatasource_UnexportedFieldsIgnored(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []UnexportedFieldStruct{
		{
			Timestamp:   baseTime,
			temperature: 20.5, // unexported, should be ignored
			Location:    "Room1",
		},
	}

	testStream := stream.Just(testData...)

	ds, err := NewStaticStructDatasource(testStream)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Should only have Location field (temperature is unexported)
	fieldsMeta := ds.result.FieldsMeta()
	require.Len(t, fieldsMeta, 1)
	assert.Equal(t, "UnexportedFieldStruct:Location", fieldsMeta[0].Urn())
}

func TestStaticStructDatasource_Execute_TimeFiltering(t *testing.T) {
	// Create test data spanning 5 hours
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ValidTestStruct{
		{Timestamp: baseTime, Temperature: 20.0, Location: "R1", Count: 1, IsActive: true},
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 21.0, Location: "R2", Count: 2, IsActive: true},
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 22.0, Location: "R3", Count: 3, IsActive: true},
		{Timestamp: baseTime.Add(3 * time.Hour), Temperature: 23.0, Location: "R4", Count: 4, IsActive: true},
		{Timestamp: baseTime.Add(4 * time.Hour), Temperature: 24.0, Location: "R5", Count: 5, IsActive: true},
	}

	testStream := stream.Just(testData...)
	ds, err := NewStaticStructDatasource(testStream)
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

func TestStaticStructDatasource_Execute_EmptyResult(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ValidTestStruct{
		{Timestamp: baseTime, Temperature: 20.0, Location: "R1", Count: 1, IsActive: true},
	}

	testStream := stream.Just(testData...)
	ds, err := NewStaticStructDatasource(testStream)
	require.NoError(t, err)

	ctx := context.Background()

	// Query for a time range that doesn't contain any records
	result, err := ds.Execute(ctx, baseTime.Add(1*time.Hour), baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	records := result.Stream().MustCollect()
	assert.Len(t, records, 0)
}

func TestStaticStructDatasource_Execute_BoundaryConditions(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ValidTestStruct{
		{Timestamp: baseTime, Temperature: 20.0, Location: "R1", Count: 1, IsActive: true},
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 21.0, Location: "R2", Count: 2, IsActive: true},
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 22.0, Location: "R3", Count: 3, IsActive: true},
	}

	testStream := stream.Just(testData...)
	ds, err := NewStaticStructDatasource(testStream)
	require.NoError(t, err)

	ctx := context.Background()

	// Test: from is inclusive
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
	require.NoError(t, err)
	records := result.Stream().MustCollect()
	require.Len(t, records, 1)
	assert.Equal(t, baseTime, records[0].Timestamp)

	// Test: to is exclusive
	testStream2 := stream.Just(testData...)
	ds2, err := NewStaticStructDatasource(testStream2)
	require.NoError(t, err)

	result2, err := ds2.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)
	records2 := result2.Stream().MustCollect()
	require.Len(t, records2, 2)
	assert.Equal(t, baseTime, records2[0].Timestamp)
	assert.Equal(t, baseTime.Add(1*time.Hour), records2[1].Timestamp)
}
