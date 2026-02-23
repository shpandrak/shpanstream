package datasource

import (
	"context"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
)

func TestTimestampExtract_AllComponents(t *testing.T) {
	ctx := context.Background()
	// Wednesday, 2025-06-18 14:35:00 UTC
	ts := time.Date(2025, 6, 18, 14, 35, 0, 0, time.UTC)
	fieldMeta, _ := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)

	tests := []struct {
		name      string
		component tsquery.TimestampExtractComponent
		expected  int64
	}{
		{"year", tsquery.TimestampExtractYear, 2025},
		{"quarter", tsquery.TimestampExtractQuarter, 2},
		{"month", tsquery.TimestampExtractMonth, 6},
		{"dayOfYear", tsquery.TimestampExtractDayOfYear, 169},
		{"dayOfMonth", tsquery.TimestampExtractDayOfMonth, 18},
		{"dayOfWeek", tsquery.TimestampExtractDayOfWeek, 3}, // Wednesday = 3
		{"hour", tsquery.TimestampExtractHour, 14},
		{"minute", tsquery.TimestampExtractMinute, 35},
		{"epochMillis", tsquery.TimestampExtractEpochMilli, ts.UnixMilli()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := NewRowTimestampFieldValue()
			field := NewTimestampExtractFieldValue(source, tt.component, time.UTC)

			meta, supplier, err := field.Execute(ctx, *fieldMeta)
			require.NoError(t, err)
			require.Equal(t, tsquery.DataTypeInteger, meta.DataType)
			require.True(t, meta.Required)

			row := timeseries.TsRecord[any]{Timestamp: ts, Value: int64(0)}
			val, err := supplier(ctx, row)
			require.NoError(t, err)
			require.Equal(t, tt.expected, val)
		})
	}
}

func TestTimestampExtract_DayOfWeek_Sunday(t *testing.T) {
	ctx := context.Background()
	// Sunday, 2025-06-22
	ts := time.Date(2025, 6, 22, 0, 0, 0, 0, time.UTC)
	fieldMeta, _ := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)

	source := NewRowTimestampFieldValue()
	field := NewTimestampExtractFieldValue(source, tsquery.TimestampExtractDayOfWeek, time.UTC)

	_, supplier, err := field.Execute(ctx, *fieldMeta)
	require.NoError(t, err)

	row := timeseries.TsRecord[any]{Timestamp: ts, Value: int64(0)}
	val, err := supplier(ctx, row)
	require.NoError(t, err)
	require.Equal(t, int64(7), val) // ISO Sunday = 7
}

func TestTimestampExtract_DayOfWeek_Monday(t *testing.T) {
	ctx := context.Background()
	// Monday, 2025-06-16
	ts := time.Date(2025, 6, 16, 0, 0, 0, 0, time.UTC)
	fieldMeta, _ := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)

	source := NewRowTimestampFieldValue()
	field := NewTimestampExtractFieldValue(source, tsquery.TimestampExtractDayOfWeek, time.UTC)

	_, supplier, err := field.Execute(ctx, *fieldMeta)
	require.NoError(t, err)

	row := timeseries.TsRecord[any]{Timestamp: ts, Value: int64(0)}
	val, err := supplier(ctx, row)
	require.NoError(t, err)
	require.Equal(t, int64(1), val) // ISO Monday = 1
}

func TestTimestampExtract_TimezoneAwareness(t *testing.T) {
	ctx := context.Background()
	// 2025-06-18 23:30:00 UTC => 2025-06-19 01:30:00 in Europe/Berlin (UTC+2 in summer)
	ts := time.Date(2025, 6, 18, 23, 30, 0, 0, time.UTC)
	fieldMeta, _ := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)

	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)

	source := NewRowTimestampFieldValue()

	// Hour in UTC should be 23
	fieldUTC := NewTimestampExtractFieldValue(source, tsquery.TimestampExtractHour, time.UTC)
	_, supplierUTC, err := fieldUTC.Execute(ctx, *fieldMeta)
	require.NoError(t, err)

	row := timeseries.TsRecord[any]{Timestamp: ts, Value: int64(0)}
	hourUTC, err := supplierUTC(ctx, row)
	require.NoError(t, err)
	require.Equal(t, int64(23), hourUTC)

	// Hour in Berlin should be 1 (next day)
	fieldBerlin := NewTimestampExtractFieldValue(source, tsquery.TimestampExtractHour, berlin)
	_, supplierBerlin, err := fieldBerlin.Execute(ctx, *fieldMeta)
	require.NoError(t, err)

	hourBerlin, err := supplierBerlin(ctx, row)
	require.NoError(t, err)
	require.Equal(t, int64(1), hourBerlin)

	// DayOfMonth should differ: 18 in UTC, 19 in Berlin
	fieldDayUTC := NewTimestampExtractFieldValue(source, tsquery.TimestampExtractDayOfMonth, time.UTC)
	_, supplierDayUTC, err := fieldDayUTC.Execute(ctx, *fieldMeta)
	require.NoError(t, err)
	dayUTC, err := supplierDayUTC(ctx, row)
	require.NoError(t, err)
	require.Equal(t, int64(18), dayUTC)

	fieldDayBerlin := NewTimestampExtractFieldValue(source, tsquery.TimestampExtractDayOfMonth, berlin)
	_, supplierDayBerlin, err := fieldDayBerlin.Execute(ctx, *fieldMeta)
	require.NoError(t, err)
	dayBerlin, err := supplierDayBerlin(ctx, row)
	require.NoError(t, err)
	require.Equal(t, int64(19), dayBerlin)
}

func TestTimestampExtract_Quarters(t *testing.T) {
	ctx := context.Background()
	fieldMeta, _ := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)

	tests := []struct {
		month    time.Month
		expected int64
	}{
		{time.January, 1}, {time.February, 1}, {time.March, 1},
		{time.April, 2}, {time.May, 2}, {time.June, 2},
		{time.July, 3}, {time.August, 3}, {time.September, 3},
		{time.October, 4}, {time.November, 4}, {time.December, 4},
	}

	for _, tt := range tests {
		t.Run(tt.month.String(), func(t *testing.T) {
			ts := time.Date(2025, tt.month, 15, 0, 0, 0, 0, time.UTC)
			source := NewRowTimestampFieldValue()
			field := NewTimestampExtractFieldValue(source, tsquery.TimestampExtractQuarter, time.UTC)

			_, supplier, err := field.Execute(ctx, *fieldMeta)
			require.NoError(t, err)

			row := timeseries.TsRecord[any]{Timestamp: ts, Value: int64(0)}
			val, err := supplier(ctx, row)
			require.NoError(t, err)
			require.Equal(t, tt.expected, val)
		})
	}
}

func TestTimestampExtract_FromConstantTimestamp(t *testing.T) {
	ctx := context.Background()
	fieldMeta, _ := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)

	// Use a constant timestamp as source (not rowTimestamp)
	ts := time.Date(2025, 9, 15, 16, 45, 0, 0, time.UTC)
	tsMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeTimestamp, Required: true}
	source := NewConstantFieldValue(tsMeta, ts)

	// Extract hour from the constant
	field := NewTimestampExtractFieldValue(source, tsquery.TimestampExtractHour, time.UTC)
	meta, supplier, err := field.Execute(ctx, *fieldMeta)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeInteger, meta.DataType)
	require.True(t, meta.Required)

	row := timeseries.TsRecord[any]{Timestamp: time.Now(), Value: int64(0)}
	val, err := supplier(ctx, row)
	require.NoError(t, err)
	require.Equal(t, int64(16), val)
}

func TestTimestampCondition_WithConstantTimestamps(t *testing.T) {
	ctx := context.Background()
	fieldMeta, _ := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)

	ts1 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2025, 6, 16, 10, 0, 0, 0, time.UTC)
	tsMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeTimestamp, Required: true}

	// condition: ts1 < ts2 should be true
	field := NewConditionFieldValue(
		tsquery.ConditionOperatorLessThan,
		NewConstantFieldValue(tsMeta, ts1),
		NewConstantFieldValue(tsMeta, ts2),
	)

	meta, supplier, err := field.Execute(ctx, *fieldMeta)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeBoolean, meta.DataType)

	row := timeseries.TsRecord[any]{Timestamp: time.Now(), Value: int64(0)}
	val, err := supplier(ctx, row)
	require.NoError(t, err)
	require.Equal(t, true, val)

	// condition: ts2 < ts1 should be false
	field2 := NewConditionFieldValue(
		tsquery.ConditionOperatorLessThan,
		NewConstantFieldValue(tsMeta, ts2),
		NewConstantFieldValue(tsMeta, ts1),
	)

	_, supplier2, err := field2.Execute(ctx, *fieldMeta)
	require.NoError(t, err)

	val2, err := supplier2(ctx, row)
	require.NoError(t, err)
	require.Equal(t, false, val2)
}

func TestTimestampExtract_NonTimestampSource(t *testing.T) {
	ctx := context.Background()
	fieldMeta, _ := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)

	// Use an integer source — should fail
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}
	source := NewConstantFieldValue(intMeta, int64(42))
	field := NewTimestampExtractFieldValue(source, tsquery.TimestampExtractHour, time.UTC)

	_, _, err := field.Execute(ctx, *fieldMeta)
	require.Error(t, err)
	require.Contains(t, err.Error(), "timestamp type")
}

func TestTimestampExtract_NilValue(t *testing.T) {
	ctx := context.Background()
	fieldMeta, _ := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)

	// Create a timestamp constant with nil value (optional)
	tsMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeTimestamp, Required: false}
	source := NewConstantFieldValue(tsMeta, nil)
	field := NewTimestampExtractFieldValue(source, tsquery.TimestampExtractHour, time.UTC)

	meta, supplier, err := field.Execute(ctx, *fieldMeta)
	require.NoError(t, err)
	require.False(t, meta.Required)

	row := timeseries.TsRecord[any]{Timestamp: time.Now(), Value: int64(0)}
	val, err := supplier(ctx, row)
	require.NoError(t, err)
	require.Nil(t, val)
}
