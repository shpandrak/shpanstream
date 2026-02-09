package queryopenapi

import (
	"context"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseFilter_AlignerFilter_WithFillMode(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a static datasource with 2 points 1 hour apart
	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "temperature",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 100.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: 200.0},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Create aligner filter with custom 15-minute alignment period
	var alignmentPeriod ApiAlignmentPeriod
	require.NoError(t, alignmentPeriod.FromApiCustomAlignmentPeriod(ApiCustomAlignmentPeriod{
		ZoneId:           "UTC",
		DurationInMillis: 15 * 60 * 1000, // 15 minutes
	}))

	// Apply fillMode=linear to densify the stream
	fillMode := timeseries.FillModeLinear
	var alignerFilter ApiQueryFilter
	require.NoError(t, alignerFilter.FromApiAlignerFilter(ApiAlignerFilter{
		AlignerPeriod:     alignmentPeriod,
		FillMode: &fillMode,
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{alignerFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Execute and verify: with 15-min alignment over a 1-hour span,
	// we expect 5 dense points: 0min, 15min, 30min, 45min, 60min
	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 5, "Linear fill with 15-min alignment over 1 hour should produce 5 points")

	// Verify timestamps and linearly interpolated values
	assert.Equal(t, baseTime, records[0].Timestamp)
	assert.InDelta(t, 100.0, records[0].Value, 0.01) // t=0min -> 100.0

	assert.Equal(t, baseTime.Add(15*time.Minute), records[1].Timestamp)
	assert.InDelta(t, 125.0, records[1].Value, 0.01) // t=15min -> 125.0

	assert.Equal(t, baseTime.Add(30*time.Minute), records[2].Timestamp)
	assert.InDelta(t, 150.0, records[2].Value, 0.01) // t=30min -> 150.0

	assert.Equal(t, baseTime.Add(45*time.Minute), records[3].Timestamp)
	assert.InDelta(t, 175.0, records[3].Value, 0.01) // t=45min -> 175.0

	assert.Equal(t, baseTime.Add(60*time.Minute), records[4].Timestamp)
	assert.InDelta(t, 200.0, records[4].Value, 0.01) // t=60min -> 200.0
}

func TestParseFilter_AlignerFilter_WithoutFillMode(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a static datasource with 2 points 1 hour apart
	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "temperature",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 100.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: 200.0},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Create aligner filter with same 15-minute alignment but NO fillMode (nil)
	var alignmentPeriod ApiAlignmentPeriod
	require.NoError(t, alignmentPeriod.FromApiCustomAlignmentPeriod(ApiCustomAlignmentPeriod{
		ZoneId:           "UTC",
		DurationInMillis: 15 * 60 * 1000, // 15 minutes
	}))

	var alignerFilter ApiQueryFilter
	require.NoError(t, alignerFilter.FromApiAlignerFilter(ApiAlignerFilter{
		AlignerPeriod:     alignmentPeriod,
		// FillMode is nil - no gap filling, backward compatible sparse behavior
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{alignerFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Execute and verify: without fillMode, only the 2 original data points
	// should be emitted (sparse output), one per occupied alignment bucket
	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2, "Without fillMode, aligner should produce only sparse aligned points")

	// First point aligned to its 15-min bucket start
	assert.Equal(t, baseTime, records[0].Timestamp)
	assert.InDelta(t, 100.0, records[0].Value, 0.01)

	// Second point aligned to its 15-min bucket start
	assert.Equal(t, baseTime.Add(1*time.Hour), records[1].Timestamp)
	assert.InDelta(t, 200.0, records[1].Value, 0.01)
}

func TestParseFilter_AlignerFilter_ForwardFill(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a static datasource with 2 points 1 hour apart
	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "temperature",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 100.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: 200.0},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Create aligner filter with custom 15-minute alignment and forwardFill mode
	var alignmentPeriod ApiAlignmentPeriod
	require.NoError(t, alignmentPeriod.FromApiCustomAlignmentPeriod(ApiCustomAlignmentPeriod{
		ZoneId:           "UTC",
		DurationInMillis: 15 * 60 * 1000, // 15 minutes
	}))

	fillMode := timeseries.FillModeForwardFill
	var alignerFilter ApiQueryFilter
	require.NoError(t, alignerFilter.FromApiAlignerFilter(ApiAlignerFilter{
		AlignerPeriod:     alignmentPeriod,
		FillMode: &fillMode,
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{alignerFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Execute and verify: with forwardFill and 15-min alignment over 1 hour,
	// we expect 5 dense points: 0min, 15min, 30min, 45min, 60min
	// Forward fill carries the previous value forward until a new data point is reached
	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 5, "Forward fill with 15-min alignment over 1 hour should produce 5 points")

	// Verify timestamps and forward-filled values
	// First 4 points should carry the value 100.0 (from the first data point)
	// The last point at t=60min has the actual data point value 200.0
	assert.Equal(t, baseTime, records[0].Timestamp)
	assert.InDelta(t, 100.0, records[0].Value, 0.01) // t=0min -> 100.0 (actual)

	assert.Equal(t, baseTime.Add(15*time.Minute), records[1].Timestamp)
	assert.InDelta(t, 100.0, records[1].Value, 0.01) // t=15min -> 100.0 (forward fill)

	assert.Equal(t, baseTime.Add(30*time.Minute), records[2].Timestamp)
	assert.InDelta(t, 100.0, records[2].Value, 0.01) // t=30min -> 100.0 (forward fill)

	assert.Equal(t, baseTime.Add(45*time.Minute), records[3].Timestamp)
	assert.InDelta(t, 100.0, records[3].Value, 0.01) // t=45min -> 100.0 (forward fill)

	assert.Equal(t, baseTime.Add(60*time.Minute), records[4].Timestamp)
	assert.InDelta(t, 200.0, records[4].Value, 0.01) // t=60min -> 200.0 (actual)
}
