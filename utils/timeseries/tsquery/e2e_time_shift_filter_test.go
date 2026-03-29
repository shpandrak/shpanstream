package tsquery_test

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestTimeShiftFilter_NegativeOffset(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC) // end-of-hour
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	ds := createDatasource(t, "EnergyReading", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 200.0, 300.0})

	// Shift back 1 hour: end-of-hour → start-of-hour
	filtered := datasource.NewFilteredDataSource(ds, datasource.NewTimeShiftFilter(-3600))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata preserved
	meta := result.Meta()
	require.Equal(t, "EnergyReading", meta.Urn())
	require.Equal(t, tsquery.DataTypeDecimal, meta.DataType())
	require.Equal(t, "kWh", meta.Unit())

	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	// Timestamps shifted back by 1 hour
	require.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), records[0].Timestamp)
	require.Equal(t, time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC), records[1].Timestamp)
	require.Equal(t, time.Date(2025, 1, 1, 2, 0, 0, 0, time.UTC), records[2].Timestamp)

	// Values unchanged
	require.Equal(t, 100.0, records[0].Value)
	require.Equal(t, 200.0, records[1].Value)
	require.Equal(t, 300.0, records[2].Value)
}

func TestTimeShiftFilter_PositiveOffset(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
	}

	ds := createDatasource(t, "Metric", tsquery.DataTypeInteger, true, "count",
		timestamps, []any{int64(42), int64(84)})

	// Shift forward 30 minutes
	filtered := datasource.NewFilteredDataSource(ds, datasource.NewTimeShiftFilter(1800))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	require.Equal(t, time.Date(2025, 1, 1, 0, 30, 0, 0, time.UTC), records[0].Timestamp)
	require.Equal(t, time.Date(2025, 1, 1, 1, 30, 0, 0, time.UTC), records[1].Timestamp)

	require.Equal(t, int64(42), records[0].Value)
	require.Equal(t, int64(84), records[1].Value)
}

func TestTimeShiftFilter_MultipleDataTypes(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime, baseTime.Add(1 * time.Hour)}

	tests := []struct {
		name     string
		dataType tsquery.DataType
		values   []any
	}{
		{"Decimal", tsquery.DataTypeDecimal, []any{1.5, 2.5}},
		{"Integer", tsquery.DataTypeInteger, []any{int64(10), int64(20)}},
		{"String", tsquery.DataTypeString, []any{"hello", "world"}},
		{"Boolean", tsquery.DataTypeBoolean, []any{true, false}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := createDatasource(t, "Field", tt.dataType, true, "", timestamps, tt.values)
			filtered := datasource.NewFilteredDataSource(ds, datasource.NewTimeShiftFilter(-7200))

			result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
			require.NoError(t, err)

			records := result.Data().MustCollect()
			require.Len(t, records, 2)

			// Timestamps shifted back 2 hours
			require.Equal(t, time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC), records[0].Timestamp)
			require.Equal(t, time.Date(2025, 6, 15, 11, 0, 0, 0, time.UTC), records[1].Timestamp)

			// Values unchanged
			require.Equal(t, tt.values[0], records[0].Value)
			require.Equal(t, tt.values[1], records[1].Value)
		})
	}
}

func TestTimeShiftFilter_EmptyStream(t *testing.T) {
	ctx := context.Background()

	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("Empty", tsquery.DataTypeDecimal, true, "", nil)
	require.NoError(t, err)

	ds, err := datasource.NewStaticDatasource(*fieldMeta, stream.Empty[timeseries.TsRecord[any]]())
	require.NoError(t, err)

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewTimeShiftFilter(-3600))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Empty(t, records)
}

func TestTimeShiftFilter_SingleItem(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "Single", tsquery.DataTypeDecimal, true, "",
		[]time.Time{baseTime}, []any{42.0})

	filtered := datasource.NewFilteredDataSource(ds, datasource.NewTimeShiftFilter(-3600))

	result, err := filtered.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 1)
	require.Equal(t, time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC), records[0].Timestamp)
	require.Equal(t, 42.0, records[0].Value)
}

type TimeShiftReportRow struct {
	Timestamp time.Time
	Energy    float64
	Count     int64
}

func TestTimeShiftReportFilter(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	rows := []TimeShiftReportRow{
		{Timestamp: baseTime, Energy: 100.0, Count: int64(10)},
		{Timestamp: baseTime.Add(1 * time.Hour), Energy: 200.0, Count: int64(20)},
		{Timestamp: baseTime.Add(2 * time.Hour), Energy: 300.0, Count: int64(30)},
	}

	result, err := createResultFromStructs(
		stream.Just(rows...),
		[]string{"Energy", "Count"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeInteger},
		[]string{"kWh", ""},
	)
	require.NoError(t, err)

	// Apply report time shift filter
	shifted, err := report.NewTimeShiftFilter(-3600).Filter(ctx, result)
	require.NoError(t, err)

	// Metadata preserved
	require.Len(t, shifted.FieldsMeta(), 2)
	require.Equal(t, "Energy", shifted.FieldsMeta()[0].Urn())
	require.Equal(t, "kWh", shifted.FieldsMeta()[0].Unit())

	records := shifted.Stream().MustCollect()
	require.Len(t, records, 3)

	// Timestamps shifted back 1 hour
	require.Equal(t, time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), records[0].Timestamp)
	require.Equal(t, time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC), records[1].Timestamp)
	require.Equal(t, time.Date(2025, 1, 1, 2, 0, 0, 0, time.UTC), records[2].Timestamp)

	// Values unchanged
	require.Equal(t, 100.0, records[0].Value[0])
	require.Equal(t, int64(10), records[0].Value[1])
	require.Equal(t, 200.0, records[1].Value[0])
	require.Equal(t, int64(20), records[1].Value[1])
}

func TestTimeShiftFilter_CompositionWithDelta(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC) // end-of-hour
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Cumulative energy readings at end-of-hour
	ds := createDatasource(t, "Energy", tsquery.DataTypeDecimal, true, "kWh",
		timestamps, []any{100.0, 250.0, 400.0})

	// First apply delta, then time shift
	withDelta := datasource.NewFilteredDataSource(ds, datasource.NewDeltaFilter(false, 0, false, nil))
	withShift := datasource.NewFilteredDataSource(withDelta, datasource.NewTimeShiftFilter(-3600))

	result, err := withShift.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	// Delta values: 150, 150
	require.Equal(t, 150.0, records[0].Value)
	require.Equal(t, 150.0, records[1].Value)

	// Delta timestamps shifted back by 1 hour
	require.Equal(t, time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC), records[0].Timestamp)
	require.Equal(t, time.Date(2025, 1, 1, 2, 0, 0, 0, time.UTC), records[1].Timestamp)
}
