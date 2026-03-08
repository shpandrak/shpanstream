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

func alignerFilterPtr(af datasource.AlignerFilter) *datasource.AlignerFilter { return &af }

// Helper function to create a single-value datasource from scalar values
func createDatasource(t *testing.T, urn string, dataType tsquery.DataType, required bool, unit string, timestamps []time.Time, values []any) datasource.DataSource {
	require.Equal(t, len(timestamps), len(values), "timestamps and values must have same length")

	// Create field metadata
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(urn, dataType, required, unit, nil)
	require.NoError(t, err)

	// Create records
	records := make([]timeseries.TsRecord[any], len(timestamps))
	for i := range timestamps {
		records[i] = timeseries.TsRecord[any]{
			Timestamp: timestamps[i],
			Value:     values[i],
		}
	}

	ds, err := datasource.NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	return ds
}

// --- Sum Tests ---

func TestReductionDatasource_SumAllDatasources_Decimal(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Create three datasources with different units
	tempDS := createDatasource(t, "Temperature", tsquery.DataTypeDecimal, true, "celsius",
		timestamps, []any{20.0, 22.0, 18.0})
	humidityDS := createDatasource(t, "Humidity", tsquery.DataTypeDecimal, true, "percent",
		timestamps, []any{60.0, 65.0, 70.0})
	pressureDS := createDatasource(t, "Pressure", tsquery.DataTypeDecimal, true, "hPa",
		timestamps, []any{1013.0, 1015.0, 1012.0})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{tempDS, humidityDS, pressureDS})

	// Create a reduction datasource with 1 hour alignment
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "total"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, "total", meta.Urn())
	require.Equal(t, tsquery.DataTypeDecimal, meta.DataType())
	require.True(t, meta.Required())
	require.Equal(t, "", meta.Unit()) // Different units, so no unit preserved

	// Verify values
	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	// First record: 20.0 + 60.0 + 1013.0 = 1093.0
	require.Equal(t, 1093.0, records[0].Value)

	// Second record: 22.0 + 65.0 + 1015.0 = 1102.0
	require.Equal(t, 1102.0, records[1].Value)

	// Third record: 18.0 + 70.0 + 1012.0 = 1100.0
	require.Equal(t, 1100.0, records[2].Value)
}

func TestReductionDatasource_Sum_Integer(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
	}

	// Create three datasources with same unit
	count1DS := createDatasource(t, "Count1", tsquery.DataTypeInteger, true, "items",
		timestamps, []any{int64(10), int64(15)})
	count2DS := createDatasource(t, "Count2", tsquery.DataTypeInteger, true, "items",
		timestamps, []any{int64(20), int64(25)})
	count3DS := createDatasource(t, "Count3", tsquery.DataTypeInteger, true, "items",
		timestamps, []any{int64(30), int64(35)})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{count1DS, count2DS, count3DS})

	// Create reduction datasource
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "sum_counts"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, "sum_counts", meta.Urn())
	require.Equal(t, tsquery.DataTypeInteger, meta.DataType())
	require.True(t, meta.Required())
	require.Equal(t, "items", meta.Unit()) // Same unit preserved

	// Verify values
	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	// First record: 10 + 20 + 30 = 60
	require.Equal(t, int64(60), records[0].Value)

	// Second record: 15 + 25 + 35 = 75
	require.Equal(t, int64(75), records[1].Value)
}

// --- Average Tests ---

func TestReductionDatasource_AvgAllDatasources_Decimal(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	// Create three datasources
	tempDS := createDatasource(t, "Temperature", tsquery.DataTypeDecimal, true, "",
		timestamps, []any{20.0})
	humidityDS := createDatasource(t, "Humidity", tsquery.DataTypeDecimal, true, "",
		timestamps, []any{60.0})
	pressureDS := createDatasource(t, "Pressure", tsquery.DataTypeDecimal, true, "",
		timestamps, []any{90.0})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{tempDS, humidityDS, pressureDS})

	// Create reduction datasource
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeAvg,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "average"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, "average", meta.Urn())
	require.Equal(t, tsquery.DataTypeDecimal, meta.DataType()) // Average always returns decimal
	require.True(t, meta.Required())

	// Verify values
	records := result.Data().MustCollect()
	require.Len(t, records, 1)

	// (20.0 + 60.0 + 90.0) / 3 = 56.666...
	require.InDelta(t, 56.666666666, records[0].Value.(float64), 0.00001)
}

func TestReductionDatasource_MinMax_Integer(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	// Create three datasources
	count1DS := createDatasource(t, "Count1", tsquery.DataTypeInteger, true, "items",
		timestamps, []any{int64(100)})
	count2DS := createDatasource(t, "Count2", tsquery.DataTypeInteger, true, "items",
		timestamps, []any{int64(50)})
	count3DS := createDatasource(t, "Count3", tsquery.DataTypeInteger, true, "items",
		timestamps, []any{int64(75)})

	// Test MIN
	multiDSMin := datasource.NewListMultiDatasource([]datasource.DataSource{count1DS, count2DS})
	reductionDSMin := datasource.NewReductionDatasource(
		tsquery.ReductionTypeMin,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDSMin,
		tsquery.AddFieldMeta{Urn: "result"},
	)

	minResult, err := reductionDSMin.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	minRecords := minResult.Data().MustCollect()
	require.Equal(t, int64(50), minRecords[0].Value) // min(100, 50) = 50

	// Test MAX
	multiDSMax := datasource.NewListMultiDatasource([]datasource.DataSource{count2DS, count3DS})
	reductionDSMax := datasource.NewReductionDatasource(
		tsquery.ReductionTypeMax,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDSMax,
		tsquery.AddFieldMeta{Urn: "result"},
	)

	maxResult, err := reductionDSMax.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	maxRecords := maxResult.Data().MustCollect()
	require.Equal(t, int64(75), maxRecords[0].Value) // max(50, 75) = 75
}

// --- Count Test ---

func TestReductionDatasource_Count(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	// Create three datasources
	tempDS := createDatasource(t, "Temperature", tsquery.DataTypeDecimal, true, "",
		timestamps, []any{20.0})
	humidityDS := createDatasource(t, "Humidity", tsquery.DataTypeDecimal, true, "",
		timestamps, []any{60.0})
	pressureDS := createDatasource(t, "Pressure", tsquery.DataTypeDecimal, true, "",
		timestamps, []any{1013.0})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{tempDS, humidityDS, pressureDS})

	// Create reduction datasource
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeCount,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "datasource_count"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, "datasource_count", meta.Urn())
	require.Equal(t, tsquery.DataTypeInteger, meta.DataType()) // Count always returns integer
	require.True(t, meta.Required())

	// Verify values
	records := result.Data().MustCollect()
	require.Len(t, records, 1)

	// Count of 3 datasources
	require.Equal(t, int64(3), records[0].Value)
}

// --- Error Cases ---

func TestReductionDatasource_ErrorOnMixedDataTypes(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	// Create datasources with different data types
	tempDS := createDatasource(t, "Temperature", tsquery.DataTypeDecimal, true, "",
		timestamps, []any{20.0})
	countDS := createDatasource(t, "Count", tsquery.DataTypeInteger, true, "",
		timestamps, []any{int64(10)})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{tempDS, countDS})

	// Create reduction datasource
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "result"},
	)

	// Execute - should fail
	_, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "all datasources must have the same data type")
}

func TestReductionDatasource_ErrorOnOptionalDatasource(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	// Create datasources - one optional
	required1DS := createDatasource(t, "Required1", tsquery.DataTypeDecimal, true, "",
		timestamps, []any{10.0})
	optionalDS := createDatasource(t, "Optional1", tsquery.DataTypeDecimal, false, "",
		timestamps, []any{20.0})
	required2DS := createDatasource(t, "Required2", tsquery.DataTypeDecimal, true, "",
		timestamps, []any{30.0})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{required1DS, optionalDS, required2DS})

	// Create reduction datasource
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "result"},
	)

	// Execute - should fail
	_, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be required")
}

// --- Unit Preservation Tests ---

func TestReductionDatasource_PreservesUnitWhenAllSame(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	// Create three datasources with same unit
	count1DS := createDatasource(t, "Count1", tsquery.DataTypeInteger, true, "items",
		timestamps, []any{int64(10)})
	count2DS := createDatasource(t, "Count2", tsquery.DataTypeInteger, true, "items",
		timestamps, []any{int64(20)})
	count3DS := createDatasource(t, "Count3", tsquery.DataTypeInteger, true, "items",
		timestamps, []any{int64(30)})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{count1DS, count2DS, count3DS})

	// Create reduction datasource
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "result"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify unit is preserved
	meta := result.Meta()
	require.Equal(t, "items", meta.Unit())
}

func TestReductionDatasource_NoUnitWhenDifferent(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	// Create three datasources with different units
	tempDS := createDatasource(t, "Temperature", tsquery.DataTypeDecimal, true, "celsius",
		timestamps, []any{20.0})
	humidityDS := createDatasource(t, "Humidity", tsquery.DataTypeDecimal, true, "percent",
		timestamps, []any{60.0})
	pressureDS := createDatasource(t, "Pressure", tsquery.DataTypeDecimal, true, "hPa",
		timestamps, []any{1013.0})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{tempDS, humidityDS, pressureDS})

	// Create reduction datasource
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "result"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify no unit when they differ
	meta := result.Meta()
	require.Equal(t, "", meta.Unit())
}

// --- Multiple Records Test ---

func TestReductionDatasource_MultipleRecordsProcessedCorrectly(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}

	// Create three datasources
	count1DS := createDatasource(t, "Count1", tsquery.DataTypeInteger, true, "",
		timestamps, []any{int64(1), int64(4), int64(7), int64(10)})
	count2DS := createDatasource(t, "Count2", tsquery.DataTypeInteger, true, "",
		timestamps, []any{int64(2), int64(5), int64(8), int64(11)})
	count3DS := createDatasource(t, "Count3", tsquery.DataTypeInteger, true, "",
		timestamps, []any{int64(3), int64(6), int64(9), int64(12)})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{count1DS, count2DS, count3DS})

	// Create reduction datasource
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "result"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify all records processed
	records := result.Data().MustCollect()
	require.Len(t, records, 4)

	// Verify each record's sum
	require.Equal(t, int64(6), records[0].Value)  // 1 + 2 + 3
	require.Equal(t, int64(15), records[1].Value) // 4 + 5 + 6
	require.Equal(t, int64(24), records[2].Value) // 7 + 8 + 9
	require.Equal(t, int64(33), records[3].Value) // 10 + 11 + 12

	// Verify timestamps preserved
	require.Equal(t, baseTime, records[0].Timestamp)
	require.Equal(t, baseTime.Add(1*time.Hour), records[1].Timestamp)
	require.Equal(t, baseTime.Add(2*time.Hour), records[2].Timestamp)
	require.Equal(t, baseTime.Add(3*time.Hour), records[3].Timestamp)
}

// --- PHASE 2: Alignment Tests ---

// TestReductionDatasource_AlignsMisalignedData_Sum verifies that the reduction datasource
// correctly aligns misaligned datasources before reducing them
func TestReductionDatasource_AlignsMisalignedData_Sum(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create datasources with misaligned timestamps
	// DS1: Data at exact hour boundaries (0:00, 1:00, 2:00)
	ds1Timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}
	ds1 := createDatasource(t, "DS1", tsquery.DataTypeInteger, true, "items",
		ds1Timestamps, []any{int64(100), int64(100), int64(100)})

	// DS2: Data offset by 30 minutes (0:30, 1:30, 2:30)
	ds2Timestamps := []time.Time{
		baseTime.Add(30 * time.Minute),
		baseTime.Add(1*time.Hour + 30*time.Minute),
		baseTime.Add(2*time.Hour + 30*time.Minute),
	}
	ds2 := createDatasource(t, "DS2", tsquery.DataTypeInteger, true, "items",
		ds2Timestamps, []any{int64(200), int64(200), int64(200)})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds1, ds2})

	// Create reduction datasource with 1 hour alignment
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "total"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify records
	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	// At 0:00 - DS1 has 100 (exact), DS2 needs interpolation (smearing first value) = 200
	// Sum = 100 + 200 = 300
	require.Equal(t, int64(300), records[0].Value)
	require.Equal(t, baseTime, records[0].Timestamp)

	// At 1:00 - DS1 has 100 (exact), DS2 interpolates between 200@0:30 and 200@1:30 = 200
	// Sum = 100 + 200 = 300
	require.Equal(t, int64(300), records[1].Value)
	require.Equal(t, baseTime.Add(1*time.Hour), records[1].Timestamp)

	// At 2:00 - DS1 has 100 (exact), DS2 interpolates between 200@1:30 and 200@2:30 = 200
	// Sum = 100 + 200 = 300
	require.Equal(t, int64(300), records[2].Value)
	require.Equal(t, baseTime.Add(2*time.Hour), records[2].Timestamp)
}

// TestReductionDatasource_AlignsMisalignedData_WeightedAverage verifies time-weighted averaging
// during alignment with non-constant values
func TestReductionDatasource_AlignsMisalignedData_WeightedAverage(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create datasources with misaligned timestamps and varying values
	// DS1: Data at exact hour boundaries
	ds1Timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}
	ds1 := createDatasource(t, "DS1", tsquery.DataTypeDecimal, true, "",
		ds1Timestamps, []any{10.0, 20.0, 30.0})

	// DS2: Data offset by 30 minutes with changing values
	// This will require time-weighted interpolation at alignment boundaries
	ds2Timestamps := []time.Time{
		baseTime.Add(30 * time.Minute),
		baseTime.Add(1*time.Hour + 30*time.Minute),
		baseTime.Add(2*time.Hour + 30*time.Minute),
	}
	ds2 := createDatasource(t, "DS2", tsquery.DataTypeDecimal, true, "",
		ds2Timestamps, []any{100.0, 300.0, 500.0})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds1, ds2})

	// Create reduction datasource with 1 hour alignment
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "total"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify records
	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	// At 0:00 - DS1 = 10.0, DS2 smears first value = 100.0
	// Sum = 10.0 + 100.0 = 110.0
	require.Equal(t, 110.0, records[0].Value)

	// At 1:00 - DS1 = 20.0, DS2 interpolates between 100@0:30 and 300@1:30
	// 1:00 is halfway between 0:30 and 1:30, so DS2 = (100 + 300) / 2 = 200.0
	// Sum = 20.0 + 200.0 = 220.0
	require.Equal(t, 220.0, records[1].Value)

	// At 2:00 - DS1 = 30.0, DS2 interpolates between 300@1:30 and 500@2:30
	// 2:00 is halfway between 1:30 and 2:30, so DS2 = (300 + 500) / 2 = 400.0
	// Sum = 30.0 + 400.0 = 430.0
	require.Equal(t, 430.0, records[2].Value)
}

// TestReductionDatasource_HandlesPartialOverlap verifies that inner join only produces
// records where all datasources have data
func TestReductionDatasource_HandlesPartialOverlap(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// DS1: Has data for hours 0, 1, 2, 3
	ds1Timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}
	ds1 := createDatasource(t, "DS1", tsquery.DataTypeInteger, true, "",
		ds1Timestamps, []any{int64(10), int64(20), int64(30), int64(40)})

	// DS2: Has data for hours 1, 2, 3, 4 (shifted by 1 hour, misaligned)
	ds2Timestamps := []time.Time{
		baseTime.Add(1*time.Hour + 15*time.Minute),
		baseTime.Add(2*time.Hour + 15*time.Minute),
		baseTime.Add(3*time.Hour + 15*time.Minute),
		baseTime.Add(4*time.Hour + 15*time.Minute),
	}
	ds2 := createDatasource(t, "DS2", tsquery.DataTypeInteger, true, "",
		ds2Timestamps, []any{int64(100), int64(200), int64(300), int64(400)})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds1, ds2})

	// Create reduction datasource with 1 hour alignment
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "total"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify only overlapping hours produce results
	// Hour 0: DS1 has data, DS2 has no data in [0:00, 1:00) cluster = no record (inner join)
	// Hour 1: DS1 has 20, DS2 has 100 (smeared) = record (120)
	// Hour 2: DS1 has 30, DS2 interpolates between 100@1:15 and 200@2:15 = 175 (halfway) -> record (205)
	// Hour 3: DS1 has 40, DS2 interpolates between 200@2:15 and 300@3:15 = 275 (halfway) -> record (315)
	// Hour 4: DS2 has data, DS1 doesn't = no record (inner join)
	records := result.Data().MustCollect()
	require.Len(t, records, 3) // Hours 1, 2, 3

	// Verify timestamps and values
	require.Equal(t, baseTime.Add(1*time.Hour), records[0].Timestamp)
	require.Equal(t, int64(120), records[0].Value) // 20 + 100

	require.Equal(t, baseTime.Add(2*time.Hour), records[1].Timestamp)
	// DS2 at 2:00: interpolate between 100@1:15 and 200@2:15
	// 2:00 is 45 minutes after 1:15, out of 60 minutes total
	// Value = 100 + (200-100) * (45/60) = 100 + 75 = 175
	require.Equal(t, int64(205), records[1].Value) // 30 + 175

	require.Equal(t, baseTime.Add(3*time.Hour), records[2].Timestamp)
	// DS2 at 3:00: interpolate between 200@2:15 and 300@3:15
	// 3:00 is 45 minutes after 2:15, out of 60 minutes total
	// Value = 200 + (300-200) * (45/60) = 200 + 75 = 275
	require.Equal(t, int64(315), records[2].Value) // 40 + 275
}

// TestReductionDatasource_NoAligner verifies that a reduction datasource
// works without an aligner when data is already aligned
func TestReductionDatasource_NoAligner(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	ds1 := createDatasource(t, "DS1", tsquery.DataTypeInteger, true, "",
		timestamps, []any{int64(10)})
	ds2 := createDatasource(t, "DS2", tsquery.DataTypeInteger, true, "",
		timestamps, []any{int64(20)})

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds1, ds2})

	// Create reduction datasource with no aligner (nil *AlignerFilter)
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		nil,
		multiDS,
		tsquery.AddFieldMeta{Urn: "total"},
	)

	// Execute - should succeed since data is pre-aligned
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 1)
	require.Equal(t, int64(30), records[0].Value)
	require.Equal(t, baseTime, records[0].Timestamp)
}

// TestReductionDatasource_ErrorOnEmptyDatasourceValueWithoutAligner verifies that
// emptyDatasourceValue requires an aligner (to generate aligned timestamps)
func TestReductionDatasource_ErrorOnEmptyDatasourceValueWithoutAligner(t *testing.T) {
	ctx := context.Background()

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{})

	// Create a static fallback value
	emptyValue := datasource.NewConstantFieldValue(tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}, int64(0))

	// Create reduction datasource with emptyDatasourceValue but no aligner
	reductionDS := datasource.NewReductionDatasourceWithEmptyFallback(
		tsquery.ReductionTypeSum,
		nil,
		multiDS,
		tsquery.AddFieldMeta{Urn: "total"},
		emptyValue,
	)

	// Execute - should fail because emptyDatasourceValue needs alignment period
	_, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "alignment period is required")
}

// --- Single Datasource Optimization Tests ---

// TestReductionDatasource_SingleDatasource_Sum verifies that when only one datasource
// is provided to Sum reduction, it returns the data as-is (optimization)
func TestReductionDatasource_SingleDatasource_Sum(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Create single datasource
	ds := createDatasource(t, "OnlyOne", tsquery.DataTypeInteger, true, "items",
		timestamps, []any{int64(100), int64(200), int64(300)})

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds})

	// Create reduction datasource with Sum
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "result"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify data returned as-is
	records := result.Data().MustCollect()
	require.Len(t, records, 3)
	require.Equal(t, int64(100), records[0].Value)
	require.Equal(t, int64(200), records[1].Value)
	require.Equal(t, int64(300), records[2].Value)

	// Verify metadata
	require.Equal(t, "result", result.Meta().Urn())
	require.Equal(t, tsquery.DataTypeInteger, result.Meta().DataType())
	require.Equal(t, "items", result.Meta().Unit())
}

// TestReductionDatasource_SingleDatasource_Avg verifies that when only one datasource
// is provided to Avg reduction, it returns the data as-is (optimization)
func TestReductionDatasource_SingleDatasource_Avg(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	// Create single datasource
	ds := createDatasource(t, "OnlyOne", tsquery.DataTypeDecimal, true, "meters",
		timestamps, []any{42.5})

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds})

	// Create reduction datasource with Avg
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeAvg,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "result"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify data returned as-is
	records := result.Data().MustCollect()
	require.Len(t, records, 1)
	require.Equal(t, 42.5, records[0].Value)

	// Verify metadata - Average returns decimal
	require.Equal(t, tsquery.DataTypeDecimal, result.Meta().DataType())
}

// TestReductionDatasource_SingleDatasource_MinMax verifies that when only one datasource
// is provided to Min/Max reduction, it returns the data as-is (optimization)
func TestReductionDatasource_SingleDatasource_MinMax(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
	}

	// Create single datasource
	ds := createDatasource(t, "OnlyOne", tsquery.DataTypeInteger, true, "count",
		timestamps, []any{int64(50), int64(75)})

	// Test Min
	multiDSMin := datasource.NewListMultiDatasource([]datasource.DataSource{ds})
	reductionDSMin := datasource.NewReductionDatasource(
		tsquery.ReductionTypeMin,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDSMin,
		tsquery.AddFieldMeta{Urn: "result"},
	)

	minResult, err := reductionDSMin.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	minRecords := minResult.Data().MustCollect()
	require.Len(t, minRecords, 2)
	require.Equal(t, int64(50), minRecords[0].Value)
	require.Equal(t, int64(75), minRecords[1].Value)

	// Test Max
	multiDSMax := datasource.NewListMultiDatasource([]datasource.DataSource{ds})
	reductionDSMax := datasource.NewReductionDatasource(
		tsquery.ReductionTypeMax,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDSMax,
		tsquery.AddFieldMeta{Urn: "result"},
	)

	maxResult, err := reductionDSMax.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	maxRecords := maxResult.Data().MustCollect()
	require.Len(t, maxRecords, 2)
	require.Equal(t, int64(50), maxRecords[0].Value)
	require.Equal(t, int64(75), maxRecords[1].Value)
}

// TestReductionDatasource_SingleDatasource_Count verifies that when only one datasource
// is provided to Count reduction, it returns the COUNT (1), NOT the original data
func TestReductionDatasource_SingleDatasource_Count(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Create single datasource with values that are NOT 1
	ds := createDatasource(t, "OnlyOne", tsquery.DataTypeInteger, true, "items",
		timestamps, []any{int64(100), int64(200), int64(300)})

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds})

	// Create reduction datasource with Count
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeCount,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "datasource_count"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify Count returns 1 for each record, NOT the original values
	records := result.Data().MustCollect()
	require.Len(t, records, 3)
	require.Equal(t, int64(1), records[0].Value) // NOT 100
	require.Equal(t, int64(1), records[1].Value) // NOT 200
	require.Equal(t, int64(1), records[2].Value) // NOT 300

	// Verify metadata
	require.Equal(t, "datasource_count", result.Meta().Urn())
	require.Equal(t, tsquery.DataTypeInteger, result.Meta().DataType())
}

// --- First/Last with Non-Numeric Types ---

func TestReductionDatasource_FirstWithString(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
	}

	// Create two string datasources
	hostADS := createDatasource(t, "HostA", tsquery.DataTypeString, true, "",
		timestamps, []any{"host-a", "host-a"})
	hostBDS := createDatasource(t, "HostB", tsquery.DataTypeString, true, "",
		timestamps, []any{"host-b", "host-b"})

	// Create multi datasource
	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{hostADS, hostBDS})

	// Create reduction datasource with first + 1 hour alignment
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeFirst,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "first_host"},
	)

	// Execute
	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, "first_host", meta.Urn())
	require.Equal(t, tsquery.DataTypeString, meta.DataType())
	require.True(t, meta.Required())

	// Verify values - first datasource's value should be selected
	records := result.Data().MustCollect()
	require.Len(t, records, 2)
	require.Equal(t, "host-a", records[0].Value)
	require.Equal(t, "host-a", records[1].Value)
}

// --- IncludePartial (Full-Join) Tests ---

// TestReductionDatasource_IncludePartial_SumWithGaps verifies that includePartial
// switches from inner-join to full-join, producing results even when some datasources
// have gaps at certain timestamps.
func TestReductionDatasource_IncludePartial_SumWithGaps(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	allTimestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
		baseTime.Add(4 * time.Hour),
		baseTime.Add(5 * time.Hour),
	}

	// Sub-test: without includePartial (inner join) — only timestamps where ALL sources have data
	t.Run("InnerJoin", func(t *testing.T) {
		sourceA := createDatasource(t, "SourceA", tsquery.DataTypeDecimal, true, "",
			allTimestamps, []any{10.0, 20.0, 30.0, 40.0, 50.0, 60.0})
		sourceB := createDatasource(t, "SourceB", tsquery.DataTypeDecimal, true, "",
			allTimestamps[:3], []any{100.0, 200.0, 300.0}) // gap at T4-T6
		sourceC := createDatasource(t, "SourceC", tsquery.DataTypeDecimal, true, "",
			allTimestamps, []any{1.0, 2.0, 3.0, 4.0, 5.0, 6.0})

		multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{sourceA, sourceB, sourceC})
		reductionDS := datasource.NewReductionDatasource(
			tsquery.ReductionTypeSum, nil, multiDS,
			tsquery.AddFieldMeta{Urn: "total"},
		)

		result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		records := result.Data().MustCollect()
		require.Len(t, records, 3) // Only T1-T3 where all sources have data

		require.Equal(t, 111.0, records[0].Value) // 10 + 100 + 1
		require.Equal(t, 222.0, records[1].Value) // 20 + 200 + 2
		require.Equal(t, 333.0, records[2].Value) // 30 + 300 + 3
	})

	// Sub-test: with includePartial (full join) — all 6 timestamps produce results
	t.Run("FullJoin", func(t *testing.T) {
		sourceA := createDatasource(t, "SourceA", tsquery.DataTypeDecimal, true, "",
			allTimestamps, []any{10.0, 20.0, 30.0, 40.0, 50.0, 60.0})
		sourceB := createDatasource(t, "SourceB", tsquery.DataTypeDecimal, true, "",
			allTimestamps[:3], []any{100.0, 200.0, 300.0}) // gap at T4-T6
		sourceC := createDatasource(t, "SourceC", tsquery.DataTypeDecimal, true, "",
			allTimestamps, []any{1.0, 2.0, 3.0, 4.0, 5.0, 6.0})

		multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{sourceA, sourceB, sourceC})
		reductionDS := datasource.NewReductionDatasource(
			tsquery.ReductionTypeSum, nil, multiDS,
			tsquery.AddFieldMeta{Urn: "total"},
		).WithIncludePartial()

		result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
		require.NoError(t, err)

		records := result.Data().MustCollect()
		require.Len(t, records, 6) // All 6 timestamps

		// T1-T3: all sources present → A+B+C
		require.Equal(t, 111.0, records[0].Value) // 10 + 100 + 1
		require.Equal(t, 222.0, records[1].Value) // 20 + 200 + 2
		require.Equal(t, 333.0, records[2].Value) // 30 + 300 + 3

		// T4-T6: only A and C present → A+C
		require.Equal(t, 44.0, records[3].Value) // 40 + 4
		require.Equal(t, 55.0, records[4].Value) // 50 + 5
		require.Equal(t, 66.0, records[5].Value) // 60 + 6
	})
}

// TestReductionDatasource_IncludePartial_AvgWithGaps verifies that with includePartial,
// the average denominator is the count of *present* values, not total datasource count.
func TestReductionDatasource_IncludePartial_AvgWithGaps(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	allTimestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Source A: all 3 timestamps
	sourceA := createDatasource(t, "SourceA", tsquery.DataTypeDecimal, true, "",
		allTimestamps, []any{30.0, 60.0, 90.0})
	// Source B: only first timestamp (gap at T2, T3)
	sourceB := createDatasource(t, "SourceB", tsquery.DataTypeDecimal, true, "",
		allTimestamps[:1], []any{60.0})
	// Source C: all 3 timestamps
	sourceC := createDatasource(t, "SourceC", tsquery.DataTypeDecimal, true, "",
		allTimestamps, []any{90.0, 120.0, 150.0})

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{sourceA, sourceB, sourceC})
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeAvg, nil, multiDS,
		tsquery.AddFieldMeta{Urn: "avg"},
	).WithIncludePartial()

	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	// T1: all 3 present → (30+60+90)/3 = 60.0
	require.InDelta(t, 60.0, records[0].Value.(float64), 0.00001)

	// T2: A and C only → (60+120)/2 = 90.0, NOT (60+0+120)/3
	require.InDelta(t, 90.0, records[1].Value.(float64), 0.00001)

	// T3: A and C only → (90+150)/2 = 120.0
	require.InDelta(t, 120.0, records[2].Value.(float64), 0.00001)
}

// TestReductionDatasource_IncludePartial_CountWithGaps verifies that with includePartial,
// count reflects the number of datasources present at each timestamp.
func TestReductionDatasource_IncludePartial_CountWithGaps(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	allTimestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Source A: all 3 timestamps
	sourceA := createDatasource(t, "SourceA", tsquery.DataTypeDecimal, true, "",
		allTimestamps, []any{10.0, 20.0, 30.0})
	// Source B: only first timestamp
	sourceB := createDatasource(t, "SourceB", tsquery.DataTypeDecimal, true, "",
		allTimestamps[:1], []any{100.0})
	// Source C: all 3 timestamps
	sourceC := createDatasource(t, "SourceC", tsquery.DataTypeDecimal, true, "",
		allTimestamps, []any{1.0, 2.0, 3.0})

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{sourceA, sourceB, sourceC})
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeCount, nil, multiDS,
		tsquery.AddFieldMeta{Urn: "count"},
	).WithIncludePartial()

	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	// T1: all 3 present → count = 3
	require.Equal(t, int64(3), records[0].Value)

	// T2: A and C only → count = 2
	require.Equal(t, int64(2), records[1].Value)

	// T3: A and C only → count = 2
	require.Equal(t, int64(2), records[2].Value)
}

// TestReductionDatasource_IncludePartial_NoOverlap verifies that with includePartial,
// completely disjoint datasources still produce results at every timestamp.
func TestReductionDatasource_IncludePartial_NoOverlap(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Source A: T1, T2 only
	sourceA := createDatasource(t, "SourceA", tsquery.DataTypeDecimal, true, "",
		[]time.Time{baseTime, baseTime.Add(1 * time.Hour)},
		[]any{10.0, 20.0})

	// Source B: T3, T4 only (completely disjoint from A)
	sourceB := createDatasource(t, "SourceB", tsquery.DataTypeDecimal, true, "",
		[]time.Time{baseTime.Add(2 * time.Hour), baseTime.Add(3 * time.Hour)},
		[]any{30.0, 40.0})

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{sourceA, sourceB})
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum, nil, multiDS,
		tsquery.AddFieldMeta{Urn: "total"},
	).WithIncludePartial()

	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 4) // All 4 timestamps produce results

	// T1: only A → sum = 10.0
	require.Equal(t, baseTime, records[0].Timestamp)
	require.Equal(t, 10.0, records[0].Value)

	// T2: only A → sum = 20.0
	require.Equal(t, baseTime.Add(1*time.Hour), records[1].Timestamp)
	require.Equal(t, 20.0, records[1].Value)

	// T3: only B → sum = 30.0
	require.Equal(t, baseTime.Add(2*time.Hour), records[2].Timestamp)
	require.Equal(t, 30.0, records[2].Value)

	// T4: only B → sum = 40.0
	require.Equal(t, baseTime.Add(3*time.Hour), records[3].Timestamp)
	require.Equal(t, 40.0, records[3].Value)
}

// TestReductionDatasource_IncludePartial_SingleDatasource verifies that includePartial
// doesn't break the single-datasource identity optimization.
func TestReductionDatasource_IncludePartial_SingleDatasource(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	ds := createDatasource(t, "OnlyOne", tsquery.DataTypeInteger, true, "items",
		timestamps, []any{int64(100), int64(200), int64(300)})

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds})

	// Create reduction with includePartial AND single datasource
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum, nil, multiDS,
		tsquery.AddFieldMeta{Urn: "result"},
	).WithIncludePartial()

	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Identity optimization should still apply — data returned as-is
	records := result.Data().MustCollect()
	require.Len(t, records, 3)
	require.Equal(t, int64(100), records[0].Value)
	require.Equal(t, int64(200), records[1].Value)
	require.Equal(t, int64(300), records[2].Value)

	// Verify metadata
	require.Equal(t, "result", result.Meta().Urn())
	require.Equal(t, tsquery.DataTypeInteger, result.Meta().DataType())
	require.Equal(t, "items", result.Meta().Unit())
}
