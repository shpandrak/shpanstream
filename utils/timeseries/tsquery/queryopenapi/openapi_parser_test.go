package queryopenapi

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestParseStaticDatasource_Valid(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	apiDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldsMeta: []ApiQueryFieldMeta{
			{
				Uri:      "temperature",
				DataType: tsquery.DataTypeDecimal,
				Required: false,
			},
			{
				Uri:      "location",
				DataType: tsquery.DataTypeString,
				Required: false,
			},
		},
		Data: []ApiStaticDataRow{
			{
				Timestamp: baseTime,
				Values:    []any{20.5, "Room1"},
			},
			{
				Timestamp: baseTime.Add(1 * time.Hour),
				Values:    []any{21.3, "Room2"},
			},
		},
	}

	ds, err := parseStaticDatasource(apiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Execute the datasource to get results
	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	// Verify field metadata
	fieldsMeta := result.FieldsMeta()
	require.Len(t, fieldsMeta, 2)
	assert.Equal(t, "temperature", fieldsMeta[0].Urn())
	assert.Equal(t, tsquery.DataTypeDecimal, fieldsMeta[0].DataType())
	assert.Equal(t, "location", fieldsMeta[1].Urn())
	assert.Equal(t, tsquery.DataTypeString, fieldsMeta[1].DataType())

	// Verify data
	records := result.Stream().MustCollect()
	require.Len(t, records, 2)

	assert.Equal(t, baseTime, records[0].Timestamp)
	assert.Equal(t, 20.5, records[0].Value[0])
	assert.Equal(t, "Room1", records[0].Value[1])

	assert.Equal(t, baseTime.Add(1*time.Hour), records[1].Timestamp)
	assert.Equal(t, 21.3, records[1].Value[0])
	assert.Equal(t, "Room2", records[1].Value[1])
}

func TestParseStaticDatasource_WithCustomMetadata(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	customMeta := map[string]interface{}{
		"source": "sensor1",
		"floor":  2,
	}

	apiDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldsMeta: []ApiQueryFieldMeta{
			{
				Uri:            "temperature",
				DataType:       tsquery.DataTypeDecimal,
				Required:       true,
				Unit:           "celsius",
				CustomMetadata: customMeta,
			},
		},
		Data: []ApiStaticDataRow{
			{
				Timestamp: baseTime,
				Values:    []any{20.5},
			},
		},
	}

	ds, err := parseStaticDatasource(apiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Execute the datasource to get results
	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
	require.NoError(t, err)

	// Verify field metadata with custom data
	fieldsMeta := result.FieldsMeta()
	require.Len(t, fieldsMeta, 1)
	assert.Equal(t, "temperature", fieldsMeta[0].Urn())
	assert.Equal(t, tsquery.DataTypeDecimal, fieldsMeta[0].DataType())
	assert.True(t, fieldsMeta[0].Required())
	assert.Equal(t, "celsius", fieldsMeta[0].Unit())

	// Verify custom metadata
	meta := fieldsMeta[0].CustomMeta()
	assert.Equal(t, "sensor1", meta["source"])
	assert.Equal(t, 2, meta["floor"])
}

func TestParseStaticDatasource_EmptyFieldsMeta(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	apiDs := ApiStaticQueryDatasource{
		Type:       "static",
		FieldsMeta: []ApiQueryFieldMeta{},
		Data: []ApiStaticDataRow{
			{
				Timestamp: baseTime,
				Values:    []any{20.5},
			},
		},
	}

	ds, err := parseStaticDatasource(apiDs)
	assert.Error(t, err)
	assert.Nil(t, ds)
	assert.Contains(t, err.Error(), "fieldsMeta cannot be empty")
}

func TestParseStaticDatasource_DuplicateURNs(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	apiDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldsMeta: []ApiQueryFieldMeta{
			{
				Uri:      "temperature",
				DataType: tsquery.DataTypeDecimal,
				Required: false,
			},
			{
				Uri:      "temperature", // Duplicate!
				DataType: tsquery.DataTypeDecimal,
				Required: false,
			},
		},
		Data: []ApiStaticDataRow{
			{
				Timestamp: baseTime,
				Values:    []any{20.5, 21.0},
			},
		},
	}

	ds, err := parseStaticDatasource(apiDs)
	assert.Error(t, err)
	assert.Nil(t, ds)
	assert.Contains(t, err.Error(), "duplicate field URN")
}

func TestParseDatasource_Static(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a static datasource
	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldsMeta: []ApiQueryFieldMeta{
			{
				Uri:      "value",
				DataType: tsquery.DataTypeDecimal,
				Required: false,
			},
		},
		Data: []ApiStaticDataRow{
			{
				Timestamp: baseTime,
				Values:    []any{100.0},
			},
		},
	}

	// Wrap in ApiQueryDatasource
	var apiDs ApiQueryDatasource
	err := apiDs.FromApiStaticQueryDatasource(staticDs)
	require.NoError(t, err)

	// Parse using the main parseDatasource function
	ds, err := ParseDatasource(NewParsingContext(context.Background(), nil), apiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Verify it works
	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
	require.NoError(t, err)

	records := result.Stream().MustCollect()
	require.Len(t, records, 1)
	assert.Equal(t, 100.0, records[0].Value[0])
}

func TestParseStaticDatasource_AllDataTypes(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	apiDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldsMeta: []ApiQueryFieldMeta{
			{Uri: "intField", DataType: tsquery.DataTypeInteger, Required: false},
			{Uri: "decimalField", DataType: tsquery.DataTypeDecimal, Required: false},
			{Uri: "stringField", DataType: tsquery.DataTypeString, Required: false},
			{Uri: "boolField", DataType: tsquery.DataTypeBoolean, Required: false},
		},
		Data: []ApiStaticDataRow{
			{
				Timestamp: baseTime,
				Values:    []any{int64(42), 3.14, "hello", true},
			},
			{
				Timestamp: baseTime.Add(1 * time.Hour),
				Values:    []any{int64(100), 2.71, "world", false},
			},
		},
	}

	ds, err := parseStaticDatasource(apiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Execute and verify
	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	records := result.Stream().MustCollect()
	require.Len(t, records, 2)

	// Verify the first record
	assert.Equal(t, int64(42), records[0].Value[0])
	assert.Equal(t, 3.14, records[0].Value[1])
	assert.Equal(t, "hello", records[0].Value[2])
	assert.Equal(t, true, records[0].Value[3])

	// Verify the second record
	assert.Equal(t, int64(100), records[1].Value[0])
	assert.Equal(t, 2.71, records[1].Value[1])
	assert.Equal(t, "world", records[1].Value[2])
	assert.Equal(t, false, records[1].Value[3])
}

// Helper function to create a test context
func testContext() context.Context {
	return context.Background()
}
