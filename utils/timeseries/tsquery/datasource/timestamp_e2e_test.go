package datasource

import (
	"context"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
)

func TestE2E_FilterBusinessHours(t *testing.T) {
	ctx := context.Background()

	// Create hourly data for 3 days (72 data points)
	// Monday 2025-06-16 00:00 UTC through Wednesday 2025-06-18 23:00 UTC
	startTime := time.Date(2025, 6, 16, 0, 0, 0, 0, time.UTC)
	var records []timeseries.TsRecord[any]
	for i := 0; i < 72; i++ {
		ts := startTime.Add(time.Duration(i) * time.Hour)
		records = append(records, timeseries.TsRecord[any]{
			Timestamp: ts,
			Value:     int64(i),
		})
	}

	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, startTime, startTime.Add(72*time.Hour))
	require.NoError(t, err)

	// Build filter: hour >= 9 AND hour < 17 (business hours 9am-5pm)
	rowTs := NewRowTimestampFieldValue()
	hourExtract := NewTimestampExtractFieldValue(rowTs, tsquery.TimestampExtractHour, time.UTC)

	hourGte9 := NewConditionFieldValue(
		tsquery.ConditionOperatorGreaterEqual,
		hourExtract,
		NewConstantFieldValue(tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}, int64(9)),
	)
	hourLt17 := NewConditionFieldValue(
		tsquery.ConditionOperatorLessThan,
		hourExtract,
		NewConstantFieldValue(tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}, int64(17)),
	)
	businessHours := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, hourGte9, hourLt17)

	conditionFilter := NewConditionFilter(businessHours)
	filteredResult, err := conditionFilter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := filteredResult.Data().Collect(ctx)
	require.NoError(t, err)

	// 3 days * 8 business hours (9,10,11,12,13,14,15,16) = 24 records
	require.Len(t, resultData, 24)

	// Verify all records are within business hours
	for _, rec := range resultData {
		hour := rec.Timestamp.Hour()
		require.True(t, hour >= 9 && hour < 17, "hour %d should be in business hours", hour)
	}
}

func TestE2E_FilterWeekdaysOnly(t *testing.T) {
	ctx := context.Background()

	// Create daily data for a full week
	// Monday 2025-06-16 through Sunday 2025-06-22
	startTime := time.Date(2025, 6, 16, 12, 0, 0, 0, time.UTC)
	var records []timeseries.TsRecord[any]
	for i := 0; i < 7; i++ {
		ts := startTime.Add(time.Duration(i) * 24 * time.Hour)
		records = append(records, timeseries.TsRecord[any]{
			Timestamp: ts,
			Value:     int64(i + 1),
		})
	}

	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, startTime, startTime.Add(8*24*time.Hour))
	require.NoError(t, err)

	// Build filter: dayOfWeek <= 5 (Monday=1..Friday=5, excluding Saturday=6, Sunday=7)
	rowTs := NewRowTimestampFieldValue()
	dayExtract := NewTimestampExtractFieldValue(rowTs, tsquery.TimestampExtractDayOfWeek, time.UTC)

	weekdayFilter := NewConditionFieldValue(
		tsquery.ConditionOperatorLessEqual,
		dayExtract,
		NewConstantFieldValue(tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}, int64(5)),
	)

	conditionFilter := NewConditionFilter(weekdayFilter)
	filteredResult, err := conditionFilter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := filteredResult.Data().Collect(ctx)
	require.NoError(t, err)

	// 5 weekdays (Monday through Friday)
	require.Len(t, resultData, 5)

	// Verify no weekends
	for _, rec := range resultData {
		day := rec.Timestamp.Weekday()
		require.NotEqual(t, time.Saturday, day)
		require.NotEqual(t, time.Sunday, day)
	}
}

func TestE2E_TimestampComparisonFilter(t *testing.T) {
	ctx := context.Background()

	// Create data spanning a time range
	startTime := time.Date(2025, 6, 16, 0, 0, 0, 0, time.UTC)
	cutoff := time.Date(2025, 6, 18, 0, 0, 0, 0, time.UTC)
	var records []timeseries.TsRecord[any]
	for i := 0; i < 5; i++ {
		ts := startTime.Add(time.Duration(i) * 24 * time.Hour)
		records = append(records, timeseries.TsRecord[any]{
			Timestamp: ts,
			Value:     int64(i),
		})
	}

	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, startTime, startTime.Add(6*24*time.Hour))
	require.NoError(t, err)

	// Filter: rowTimestamp < cutoff (timestamp comparison)
	rowTs := NewRowTimestampFieldValue()
	cutoffConst := NewConstantFieldValue(tsquery.ValueMeta{DataType: tsquery.DataTypeTimestamp, Required: true}, cutoff)
	beforeCutoff := NewConditionFieldValue(tsquery.ConditionOperatorLessThan, rowTs, cutoffConst)

	conditionFilter := NewConditionFilter(beforeCutoff)
	filteredResult, err := conditionFilter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := filteredResult.Data().Collect(ctx)
	require.NoError(t, err)

	// June 16 and June 17 are before cutoff
	require.Len(t, resultData, 2)
	require.True(t, resultData[0].Timestamp.Before(cutoff))
	require.True(t, resultData[1].Timestamp.Before(cutoff))
}
