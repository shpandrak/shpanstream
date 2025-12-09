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
	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter())

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

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter())

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, tsquery.DataTypeInteger, meta.DataType())
	require.Equal(t, "count", meta.Unit())

	// Verify deltas
	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	require.Equal(t, int64(42), records[0].Value)  // 1042 - 1000
	require.Equal(t, int64(58), records[1].Value)  // 1100 - 1042
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

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter())

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

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter())

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

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter())

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

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter())

	_, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "numeric data types")
}

func TestDeltaFilter_ErrorOnBooleanDataType(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "BoolField", tsquery.DataTypeBoolean, true, "",
		[]time.Time{baseTime}, []any{true})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter())

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

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter())

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

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter())

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	require.Equal(t, "kWh", result.Meta().Unit())
}
