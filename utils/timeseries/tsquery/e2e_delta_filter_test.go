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

func TestDeltaFilter_Decimal(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}

	// Cumulative energy: 100, 150, 180, 250 kWh
	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 150.0, 180.0, 250.0})

	// Apply delta filter
	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata preserved
	meta := result.Meta()
	require.Equal(t, "LifetimeEnergy", meta.Urn())
	require.Equal(t, tsquery.DataTypeDecimal, meta.DataType())
	require.Equal(t, "kWh", meta.Unit())
	require.True(t, meta.Required())

	// Verify deltas
	records := result.Data().MustCollect()
	require.Len(t, records, 3) // First record dropped

	// Delta 1: 150 - 100 = 50
	require.Equal(t, 50.0, records[0].Value)
	require.Equal(t, baseTime.Add(1*time.Hour), records[0].Timestamp)

	// Delta 2: 180 - 150 = 30
	require.Equal(t, 30.0, records[1].Value)
	require.Equal(t, baseTime.Add(2*time.Hour), records[1].Timestamp)

	// Delta 3: 250 - 180 = 70
	require.Equal(t, 70.0, records[2].Value)
	require.Equal(t, baseTime.Add(3*time.Hour), records[2].Timestamp)
}

func TestDeltaFilter_Integer(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Cumulative counter: 1000, 1042, 1100
	ds := createDatasource(t, "Counter", tsquery.DataTypeInteger, true, "count",
		timestamps, []any{int64(1000), int64(1042), int64(1100)})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, tsquery.DataTypeInteger, meta.DataType())
	require.Equal(t, "count", meta.Unit())

	// Verify deltas
	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	require.Equal(t, int64(42), records[0].Value) // 1042 - 1000
	require.Equal(t, int64(58), records[1].Value) // 1100 - 1042
}

func TestDeltaFilter_NegativeDelta(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Values go down: 100, 80, 50
	ds := createDatasource(t, "Temperature", tsquery.DataTypeDecimal, true, "celsius",
		timestamps, []any{100.0, 80.0, 50.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	require.Equal(t, -20.0, records[0].Value) // 80 - 100 = -20
	require.Equal(t, -30.0, records[1].Value) // 50 - 80 = -30
}

func TestDeltaFilter_EmptyStream(t *testing.T) {
	ctx := context.Background()

	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("Empty", tsquery.DataTypeDecimal, true, "", nil)
	require.NoError(t, err)

	ds, err := datasource.NewStaticDatasource(*fieldMeta, stream.Empty[timeseries.TsRecord[any]]())
	require.NoError(t, err)

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Empty(t, records)
}

func TestDeltaFilter_SingleItem(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "Single", tsquery.DataTypeDecimal, true, "",
		[]time.Time{baseTime}, []any{42.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Single item produces empty output (need at least 2 for delta)
	records := result.Data().MustCollect()
	require.Empty(t, records)
}

func TestDeltaFilter_ErrorOnStringDataType(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "StringField", tsquery.DataTypeString, true, "",
		[]time.Time{baseTime}, []any{"hello"})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, nil))

	_, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "numeric data types")
}

func TestDeltaFilter_ErrorOnBooleanDataType(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "BoolField", tsquery.DataTypeBoolean, true, "",
		[]time.Time{baseTime}, []any{true})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, nil))

	_, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "numeric data types")
}

func TestDeltaFilter_ErrorOnOptionalField(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// required=false
	ds := createDatasource(t, "OptionalField", tsquery.DataTypeDecimal, false, "",
		[]time.Time{baseTime}, []any{42.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, nil))

	_, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "required")
}

func TestDeltaFilter_PreservesUnit(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime, baseTime.Add(1 * time.Hour)}

	ds := createDatasource(t, "Energy", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 150.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	require.Equal(t, "kWh", result.Meta().Unit())
}

// === nonNegative tests ===

func TestDeltaFilter_NonNegative_ResetToZero(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}

	// Counter resets to zero: 100, 150, 0, 30
	ds := createDatasource(t, "Counter", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 150.0, 0.0, 30.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(true, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	require.Equal(t, 50.0, records[0].Value) // 150 - 100 = 50
	require.Equal(t, 0.0, records[1].Value)  // reset: current value = 0
	require.Equal(t, 30.0, records[2].Value) // 30 - 0 = 30
}

func TestDeltaFilter_NonNegative_ResetToNonZero(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}

	// Counter resets to non-zero: 100, 150, 50, 80
	ds := createDatasource(t, "Counter", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 150.0, 50.0, 80.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(true, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	require.Equal(t, 50.0, records[0].Value) // 150 - 100 = 50
	require.Equal(t, 0.0, records[1].Value)  // decrease detected: clamped to 0
	require.Equal(t, 30.0, records[2].Value) // 80 - 50 = 30
}

func TestDeltaFilter_NonNegative_NegativeValueDropped(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}

	// Negative value at index 2: 100, 150, -5, 80
	ds := createDatasource(t, "Counter", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 150.0, -5.0, 80.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(true, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2) // -5 point is dropped

	require.Equal(t, 50.0, records[0].Value) // 150 - 100 = 50
	// After dropping -5, prevItem stays at 150; then 80 < 150 so decrease detected: clamped to 0
	require.Equal(t, 0.0, records[1].Value)
}

func TestDeltaFilter_NonNegative_NormalIncrease(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Normal increasing values: 100, 150, 200
	ds := createDatasource(t, "Counter", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 150.0, 200.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(true, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	require.Equal(t, 50.0, records[0].Value) // 150 - 100 = 50
	require.Equal(t, 50.0, records[1].Value) // 200 - 150 = 50
}

func TestDeltaFilter_NonNegative_Integer(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}

	// Integer counter with reset: 1000, 1050, 200, 250
	ds := createDatasource(t, "Counter", tsquery.DataTypeInteger, true, "count",
		timestamps, []any{int64(1000), int64(1050), int64(200), int64(250)})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(true, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	require.Equal(t, int64(50), records[0].Value) // 1050 - 1000 = 50
	require.Equal(t, int64(0), records[1].Value)  // decrease detected: clamped to 0
	require.Equal(t, int64(50), records[2].Value) // 250 - 200 = 50
}

func TestDeltaFilter_MaxCounterValue_Wraparound(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Counter wraps around max 1000: 900, 950, 100
	ds := createDatasource(t, "Counter", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{900.0, 950.0, 100.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(true, 1000, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	require.Equal(t, 50.0, records[0].Value)  // 950 - 900 = 50
	require.Equal(t, 150.0, records[1].Value) // wrap: (1000 - 950) + 100 = 150
}

func TestDeltaFilter_MaxCounterValue_NormalIncrease(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Normal increase with maxCounterValue set: 100, 200, 300
	ds := createDatasource(t, "Counter", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 200.0, 300.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(true, 1000, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	require.Equal(t, 100.0, records[0].Value) // 200 - 100 = 100
	require.Equal(t, 100.0, records[1].Value) // 300 - 200 = 100
}

// === emitOnReset tests ===

func TestDeltaFilter_EmitOnReset_ResetToNonZero(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}

	// Counter resets to non-zero: 100, 150, 50, 80
	ds := createDatasource(t, "Counter", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 150.0, 50.0, 80.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(true, 0, true, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	require.Equal(t, 50.0, records[0].Value) // 150 - 100 = 50
	require.Equal(t, 50.0, records[1].Value) // reset: emits current value = 50
	require.Equal(t, 30.0, records[2].Value) // 80 - 50 = 30
}

func TestDeltaFilter_EmitOnReset_Integer(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}

	// Integer counter with reset: 1000, 1050, 200, 250
	ds := createDatasource(t, "Counter", tsquery.DataTypeInteger, true, "count",
		timestamps, []any{int64(1000), int64(1050), int64(200), int64(250)})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(true, 0, true, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	require.Equal(t, int64(50), records[0].Value)  // 1050 - 1000 = 50
	require.Equal(t, int64(200), records[1].Value) // reset: emits current value = 200
	require.Equal(t, int64(50), records[2].Value)  // 250 - 200 = 50
}

// === glitch scenario tests ===

func TestDeltaFilter_NonNegative_GlitchClampedToZero(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(5 * time.Minute),
		baseTime.Add(10 * time.Minute),
		baseTime.Add(15 * time.Minute),
		baseTime.Add(20 * time.Minute),
	}

	// Simulates energy meter glitch: cumulative value dips briefly then recovers
	// 12800000, 12800100, 12799580 (glitch!), 12800200, 12800350
	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "Wh",
		timestamps, []any{12800000.0, 12800100.0, 12799580.0, 12800200.0, 12800350.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(true, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 4)

	require.Equal(t, 100.0, records[0].Value) // 12800100 - 12800000 = 100
	require.Equal(t, 0.0, records[1].Value)   // glitch: 12799580 < 12800100 → clamped to 0
	require.Equal(t, 620.0, records[2].Value) // 12800200 - 12799580 = 620 (includes recovery)
	require.Equal(t, 150.0, records[3].Value) // 12800350 - 12800200 = 150
}

func TestDeltaFilter_EmitOnReset_GlitchCausesSpike(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(5 * time.Minute),
		baseTime.Add(10 * time.Minute),
		baseTime.Add(15 * time.Minute),
	}

	// Same glitch scenario but with emitOnReset=true — shows why clamp is better
	// 12800000, 12800100, 12799580 (glitch!), 12800200
	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "Wh",
		timestamps, []any{12800000.0, 12800100.0, 12799580.0, 12800200.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(true, 0, true, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	require.Equal(t, 100.0, records[0].Value)      // 12800100 - 12800000 = 100
	require.Equal(t, 12799580.0, records[1].Value) // glitch: emits current value = 12799580 (huge spike!)
	require.Equal(t, 620.0, records[2].Value)      // 12800200 - 12799580 = 620
}

func TestDeltaFilter_NonNegative_False_StillAllowsNegative(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Decreasing values with nonNegative=false: old behavior preserved
	ds := createDatasource(t, "Temperature", tsquery.DataTypeDecimal, true, "celsius",
		timestamps, []any{100.0, 80.0, 50.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	require.Equal(t, -20.0, records[0].Value) // 80 - 100 = -20
	require.Equal(t, -30.0, records[1].Value) // 50 - 80 = -30
}

// --- MaxGapDuration tests ---

func durationPtr(d time.Duration) *time.Duration { return &d }

func TestDeltaFilter_MaxGapDuration_DropsLargeGap(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(10 * time.Hour), // 9h gap
		baseTime.Add(11 * time.Hour),
	}

	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 200.0, 500.0, 520.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, durationPtr(2*time.Hour)))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	// Delta 1: 200 - 100 = 100 (1h gap, allowed)
	require.Equal(t, 100.0, records[0].Value)
	require.Equal(t, baseTime.Add(1*time.Hour), records[0].Timestamp)

	// Delta at 10h dropped (9h gap > 2h), 500 becomes new baseline
	// Delta 2: 520 - 500 = 20 (1h gap, allowed)
	require.Equal(t, 20.0, records[1].Value)
	require.Equal(t, baseTime.Add(11*time.Hour), records[1].Timestamp)
}

func TestDeltaFilter_MaxGapDuration_NormalSpacingPasses(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}

	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 200.0, 350.0, 500.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, durationPtr(2*time.Hour)))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	require.Equal(t, 100.0, records[0].Value)
	require.Equal(t, 150.0, records[1].Value)
	require.Equal(t, 150.0, records[2].Value)
}

func TestDeltaFilter_MaxGapDuration_ExactBoundaryAllowed(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(2 * time.Hour), // gap exactly equals maxGap
	}

	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 300.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, durationPtr(2*time.Hour)))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 1) // gap == maxGap is NOT dropped (strictly greater than)

	require.Equal(t, 200.0, records[0].Value)
}

func TestDeltaFilter_MaxGapDuration_ConsecutiveGaps(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(10 * time.Hour), // 10h gap
		baseTime.Add(20 * time.Hour), // 10h gap
		baseTime.Add(21 * time.Hour), // 1h gap
	}

	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 500.0, 900.0, 920.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, durationPtr(2*time.Hour)))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 1) // only delta at 21h emitted

	require.Equal(t, 20.0, records[0].Value) // 920 - 900
	require.Equal(t, baseTime.Add(21*time.Hour), records[0].Timestamp)
}

func TestDeltaFilter_MaxGapDuration_BaselineReset(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(10 * time.Hour), // 9h gap
		baseTime.Add(11 * time.Hour),
	}

	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 200.0, 500.0, 520.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, durationPtr(2*time.Hour)))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	// After gap, delta is 520-500=20, NOT 520-200=320
	require.Equal(t, 20.0, records[1].Value)
}

func TestDeltaFilter_MaxGapDuration_NilDisabled(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(10 * time.Hour), // 9h gap
		baseTime.Add(11 * time.Hour),
	}

	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 200.0, 500.0, 520.0})

	// nil maxGapDuration = no gap detection (backwards compatible)
	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, nil))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 3) // all deltas emitted, including the spike

	require.Equal(t, 100.0, records[0].Value) // 200 - 100
	require.Equal(t, 300.0, records[1].Value) // 500 - 200 (the spike)
	require.Equal(t, 20.0, records[2].Value)  // 520 - 500
}

func TestDeltaFilter_MaxGapDuration_WithNonNegative(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),  // counter reset
		baseTime.Add(10 * time.Hour), // 8h gap
		baseTime.Add(11 * time.Hour),
	}

	ds := createDatasource(t, "Counter", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 200.0, 50.0, 500.0, 520.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(true, 0, false, durationPtr(2*time.Hour)))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	// Delta 1: 200 - 100 = 100 (normal)
	require.Equal(t, 100.0, records[0].Value)
	// Delta 2: 50 < 200, counter reset clamped to 0
	require.Equal(t, 0.0, records[1].Value)
	// Delta at 10h: gap 8h > 2h, dropped; 500 becomes baseline
	// Delta 3: 520 - 500 = 20
	require.Equal(t, 20.0, records[2].Value)
}

func TestDeltaFilter_MaxGapDuration_SingleItem(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "kWh",
		[]time.Time{baseTime}, []any{100.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, durationPtr(2*time.Hour)))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 0)
}

func TestDeltaFilter_MaxGapDuration_EmptyStream(t *testing.T) {
	ctx := context.Background()

	ds := createDatasource(t, "LifetimeEnergy", tsquery.DataTypeDecimal, true, "kWh",
		[]time.Time{}, []any{})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, durationPtr(2*time.Hour)))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 0)
}
