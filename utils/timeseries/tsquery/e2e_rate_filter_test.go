package tsquery_test

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestRateFilter_Decimal(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Cumulative energy: 100, 150, 180 kWh over 1 hour intervals
	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 150.0, 180.0})

	// Apply rate filter with unit override
	filtered := datasource.NewFilteredDataSource(ds, datasource.NewRateFilter("kW", 0, false, 0))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, "LifetimeEnergy", meta.Urn())
	require.Equal(t, tsquery.DataTypeDecimal, meta.DataType())
	require.Equal(t, "kW", meta.Unit())
	require.True(t, meta.Required())

	// Verify rates
	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	// Rate 1: (150 - 100) / 3600 = 50/3600 ≈ 0.01389 kW
	require.InDelta(t, 50.0/3600.0, records[0].Value.(float64), 1e-10)
	require.Equal(t, baseTime.Add(1*time.Hour), records[0].Timestamp)

	// Rate 2: (180 - 150) / 3600 = 30/3600 ≈ 0.00833 kW
	require.InDelta(t, 30.0/3600.0, records[1].Value.(float64), 1e-10)
	require.Equal(t, baseTime.Add(2*time.Hour), records[1].Timestamp)
}

func TestRateFilter_Integer(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
	}

	// Integer input: 1000, 4600 (delta of 3600 over 1 hour = 1 per second)
	ds := createDatasource(t, "Counter", tsquery.DataTypeInteger, true, "count",
		timestamps, []any{int64(1000), int64(4600)})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewRateFilter("count/s", 0, false, 0))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Output should be decimal even for integer input
	meta := result.Meta()
	require.Equal(t, tsquery.DataTypeDecimal, meta.DataType())
	require.Equal(t, "count/s", meta.Unit())

	records := result.Data().MustCollect()
	require.Len(t, records, 1)

	// (4600 - 1000) / 3600 = 1.0
	require.InDelta(t, 1.0, records[0].Value.(float64), 1e-10)
}

func TestRateFilter_NoUnitOverride(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime, baseTime.Add(1 * time.Hour)}

	ds := createDatasource(t, "Energy", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 200.0})

	// Empty string for unit override
	filtered := datasource.NewFilteredDataSource(ds, datasource.NewRateFilter("", 0, false, 0))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Unit should be empty
	require.Equal(t, "", result.Meta().Unit())
}

func TestRateFilter_VaryingTimeIntervals(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(30 * time.Minute),  // 30 min interval
		baseTime.Add(2 * time.Hour),     // 90 min interval
	}

	// Values: 0, 30, 120 (same rate of 1 per minute)
	ds := createDatasource(t, "Counter", tsquery.DataTypeDecimal, true, "",
		timestamps, []any{0.0, 30.0, 120.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewRateFilter("per_second", 0, false, 0))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	// Rate 1: 30 / (30*60) = 30/1800 = 1/60 per second
	require.InDelta(t, 1.0/60.0, records[0].Value.(float64), 1e-10)

	// Rate 2: 90 / (90*60) = 90/5400 = 1/60 per second
	require.InDelta(t, 1.0/60.0, records[1].Value.(float64), 1e-10)
}

func TestRateFilter_NegativeRate(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
	}

	// Value decreases: 100 -> 50
	ds := createDatasource(t, "Temperature", tsquery.DataTypeDecimal, true, "celsius",
		timestamps, []any{100.0, 50.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewRateFilter("celsius/s", 0, false, 0))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 1)

	// (50 - 100) / 3600 = -50/3600 ≈ -0.01389
	require.InDelta(t, -50.0/3600.0, records[0].Value.(float64), 1e-10)
}

func TestRateFilter_EmptyStream(t *testing.T) {
	ctx := context.Background()

	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("Empty", tsquery.DataTypeDecimal, true, "", nil)
	require.NoError(t, err)

	ds, err := datasource.NewStaticDatasource(*fieldMeta, stream.Empty[timeseries.TsRecord[any]]())
	require.NoError(t, err)

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewRateFilter("", 0, false, 0))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Empty(t, records)
}

func TestRateFilter_SingleItem(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "Single", tsquery.DataTypeDecimal, true, "",
		[]time.Time{baseTime}, []any{42.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewRateFilter("", 0, false, 0))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Empty(t, records)
}

func TestRateFilter_ErrorOnStringDataType(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "StringField", tsquery.DataTypeString, true, "",
		[]time.Time{baseTime}, []any{"hello"})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewRateFilter("", 0, false, 0))

	_, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "numeric data types")
}

func TestRateFilter_PerSeconds(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Cumulative energy: 100, 150, 180 kWh over 1 hour intervals
	// With perSeconds=3600, rate should be per-hour instead of per-second
	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 150.0, 180.0})

	// perSeconds=3600 means rate per hour
	filtered := datasource.NewFilteredDataSource(ds, datasource.NewRateFilter("kW", 3600, false, 0))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	// Rate 1: (150 - 100) / 3600 * 3600 = 50 kW
	require.InDelta(t, 50.0, records[0].Value.(float64), 1e-10)
	require.Equal(t, baseTime.Add(1*time.Hour), records[0].Timestamp)

	// Rate 2: (180 - 150) / 3600 * 3600 = 30 kW
	require.InDelta(t, 30.0, records[1].Value.(float64), 1e-10)
	require.Equal(t, baseTime.Add(2*time.Hour), records[1].Timestamp)
}

func TestRateFilter_NonNegative(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}

	// Counter with reset: 100, 200, 50 (reset), 150
	ds := createDatasource(t, "Counter", tsquery.DataTypeDecimal, true, "count",
		timestamps, []any{100.0, 200.0, 50.0, 150.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewRateFilter("count/s", 0, true, 0))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	// Rate 1: (200 - 100) / 3600 = 100/3600
	require.InDelta(t, 100.0/3600.0, records[0].Value.(float64), 1e-10)
	require.Equal(t, baseTime.Add(1*time.Hour), records[0].Timestamp)

	// Rate 2: reset detected (50 < 200), delta = 50 (current value), rate = 50/3600
	require.InDelta(t, 50.0/3600.0, records[1].Value.(float64), 1e-10)
	require.Equal(t, baseTime.Add(2*time.Hour), records[1].Timestamp)

	// Rate 3: (150 - 50) / 3600 = 100/3600
	require.InDelta(t, 100.0/3600.0, records[2].Value.(float64), 1e-10)
	require.Equal(t, baseTime.Add(3*time.Hour), records[2].Timestamp)
}

func TestRateFilter_NonNegativeWithMaxCounterValue(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Counter with wraparound: max=1000, values: 900, 100 (wrapped), 300
	ds := createDatasource(t, "Counter", tsquery.DataTypeDecimal, true, "count",
		timestamps, []any{900.0, 100.0, 300.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewRateFilter("count/s", 0, true, 1000))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	// Rate 1: reset detected (100 < 900), delta = (1000 - 900) + 100 = 200, rate = 200/3600
	require.InDelta(t, 200.0/3600.0, records[0].Value.(float64), 1e-10)
	require.Equal(t, baseTime.Add(1*time.Hour), records[0].Timestamp)

	// Rate 2: (300 - 100) / 3600 = 200/3600
	require.InDelta(t, 200.0/3600.0, records[1].Value.(float64), 1e-10)
	require.Equal(t, baseTime.Add(2*time.Hour), records[1].Timestamp)
}

func TestRateFilter_ErrorOnOptionalField(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "OptionalField", tsquery.DataTypeDecimal, false, "",
		[]time.Time{baseTime}, []any{42.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewRateFilter("", 0, false, 0))

	_, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "required")
}
