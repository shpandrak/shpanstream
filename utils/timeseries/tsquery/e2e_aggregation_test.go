package tsquery_test

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/aggregation"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// --- Datasource Aggregation Tests ---

func TestAggregation_AllSevenReductions_Integer(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a static datasource with 10 integer records
	timestamps := make([]time.Time, 10)
	values := make([]any, 10)
	for i := 0; i < 10; i++ {
		timestamps[i] = baseTime.Add(time.Duration(i) * time.Hour)
		values[i] = int64(i + 1) // 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	}
	ds := createDatasource(t, "energy", tsquery.DataTypeInteger, true, "kWh", timestamps, values)

	// Aggregate with all 7 reductions
	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
		{ReductionType: tsquery.ReductionTypeAvg},
		{ReductionType: tsquery.ReductionTypeMin},
		{ReductionType: tsquery.ReductionTypeMax},
		{ReductionType: tsquery.ReductionTypeCount},
		{ReductionType: tsquery.ReductionTypeFirst},
		{ReductionType: tsquery.ReductionTypeLast},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 7)

	// sum: 1+2+...+10 = 55
	require.Equal(t, int64(55), fields[0].Value)
	require.Equal(t, "energy_sum", metas[0].Urn())
	require.Equal(t, tsquery.DataTypeInteger, metas[0].DataType())
	require.Equal(t, "kWh", metas[0].Unit())

	// avg: 55/10 = 5.5
	require.InDelta(t, 5.5, fields[1].Value.(float64), 0.0001)
	require.Equal(t, "energy_avg", metas[1].Urn())
	require.Equal(t, tsquery.DataTypeDecimal, metas[1].DataType())

	// min: 1
	require.Equal(t, int64(1), fields[2].Value)
	require.Equal(t, "energy_min", metas[2].Urn())
	require.Equal(t, tsquery.DataTypeInteger, metas[2].DataType())

	// max: 10
	require.Equal(t, int64(10), fields[3].Value)
	require.Equal(t, "energy_max", metas[3].Urn())
	require.Equal(t, tsquery.DataTypeInteger, metas[3].DataType())

	// count: 10
	require.Equal(t, int64(10), fields[4].Value)
	require.Equal(t, "energy_count", metas[4].Urn())
	require.Equal(t, tsquery.DataTypeInteger, metas[4].DataType())
	require.Equal(t, "", metas[4].Unit()) // count has no unit

	// first: 1
	require.Equal(t, int64(1), fields[5].Value)
	require.Equal(t, "energy_first", metas[5].Urn())

	// last: 10
	require.Equal(t, int64(10), fields[6].Value)
	require.Equal(t, "energy_last", metas[6].Urn())
}

func TestAggregation_TimestampsOnResults(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
		baseTime.Add(4 * time.Hour),
	}
	// Values: 30, 10, 50, 20, 40 — min=10 at hour 1, max=50 at hour 2
	values := []any{int64(30), int64(10), int64(50), int64(20), int64(40)}
	ds := createDatasource(t, "temp", tsquery.DataTypeInteger, true, "", timestamps, values)

	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
		{ReductionType: tsquery.ReductionTypeAvg},
		{ReductionType: tsquery.ReductionTypeCount},
		{ReductionType: tsquery.ReductionTypeMin},
		{ReductionType: tsquery.ReductionTypeMax},
		{ReductionType: tsquery.ReductionTypeFirst},
		{ReductionType: tsquery.ReductionTypeLast},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)

	// sum, avg, count: no timestamp
	require.Nil(t, fields[0].Timestamp, "sum should have no timestamp")
	require.Nil(t, fields[1].Timestamp, "avg should have no timestamp")
	require.Nil(t, fields[2].Timestamp, "count should have no timestamp")

	// min: timestamp at hour 1 (value 10)
	require.NotNil(t, fields[3].Timestamp, "min should have timestamp")
	require.Equal(t, baseTime.Add(1*time.Hour), *fields[3].Timestamp)

	// max: timestamp at hour 2 (value 50)
	require.NotNil(t, fields[4].Timestamp, "max should have timestamp")
	require.Equal(t, baseTime.Add(2*time.Hour), *fields[4].Timestamp)

	// first: timestamp at hour 0
	require.NotNil(t, fields[5].Timestamp, "first should have timestamp")
	require.Equal(t, baseTime, *fields[5].Timestamp)

	// last: timestamp at hour 4
	require.NotNil(t, fields[6].Timestamp, "last should have timestamp")
	require.Equal(t, baseTime.Add(4*time.Hour), *fields[6].Timestamp)
}

func TestAggregation_CustomFieldMeta(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	timestamps := []time.Time{baseTime, baseTime.Add(1 * time.Hour)}
	values := []any{int64(10), int64(20)}
	ds := createDatasource(t, "energy", tsquery.DataTypeInteger, true, "kWh", timestamps, values)

	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{
			ReductionType: tsquery.ReductionTypeSum,
			AddFieldMeta:  &tsquery.AddFieldMeta{Urn: "total_energy", OverrideUnit: "MWh"},
		},
		{
			ReductionType: tsquery.ReductionTypeAvg,
			AddFieldMeta:  &tsquery.AddFieldMeta{Urn: "avg_energy"},
		},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 2)

	// Custom URN and unit
	require.Equal(t, "total_energy", metas[0].Urn())
	require.Equal(t, "MWh", metas[0].Unit())
	require.Equal(t, int64(30), fields[0].Value)

	// Custom URN, unit from source
	require.Equal(t, "avg_energy", metas[1].Urn())
	require.Equal(t, "kWh", metas[1].Unit())
}

func TestAggregation_EmptyStreamWithFallback(t *testing.T) {
	ctx := context.Background()

	// Create an empty datasource
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("energy", tsquery.DataTypeInteger, true, "kWh", nil)
	require.NoError(t, err)
	ds, err := datasource.NewStaticDatasource(*fieldMeta, stream.Empty[timeseries.TsRecord[any]]())
	require.NoError(t, err)

	// Create a constant static value for fallback
	constValue := datasource.NewConstantFieldValue(tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}, int64(0))

	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{
			ReductionType: tsquery.ReductionTypeSum,
			EmptyValue:    constValue,
		},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Equal(t, int64(0), fields[0].Value)
}

func TestAggregation_EmptyStreamWithoutFallback(t *testing.T) {
	ctx := context.Background()

	// Create an empty datasource
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("energy", tsquery.DataTypeInteger, true, "kWh", nil)
	require.NoError(t, err)
	ds, err := datasource.NewStaticDatasource(*fieldMeta, stream.Empty[timeseries.TsRecord[any]]())
	require.NoError(t, err)

	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Nil(t, fields[0].Value, "empty stream without fallback should produce nil")
}

func TestAggregation_SingleRecord(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "energy", tsquery.DataTypeInteger, true, "kWh", []time.Time{ts}, []any{int64(42)})

	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
		{ReductionType: tsquery.ReductionTypeAvg},
		{ReductionType: tsquery.ReductionTypeMin},
		{ReductionType: tsquery.ReductionTypeMax},
		{ReductionType: tsquery.ReductionTypeFirst},
		{ReductionType: tsquery.ReductionTypeLast},
		{ReductionType: tsquery.ReductionTypeCount},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 7)

	require.Equal(t, int64(42), fields[0].Value)                // sum
	require.InDelta(t, 42.0, fields[1].Value.(float64), 0.0001) // avg
	require.Equal(t, int64(42), fields[2].Value)                // min
	require.Equal(t, int64(42), fields[3].Value)                // max
	require.Equal(t, int64(42), fields[4].Value)                // first
	require.Equal(t, int64(42), fields[5].Value)                // last
	require.Equal(t, int64(1), fields[6].Value)                 // count

	// All timestamp-bearing reductions should point to the single record's time
	require.Equal(t, ts, *fields[2].Timestamp) // min
	require.Equal(t, ts, *fields[3].Timestamp) // max
	require.Equal(t, ts, *fields[4].Timestamp) // first
	require.Equal(t, ts, *fields[5].Timestamp) // last
}

func TestAggregation_IntegerVsDecimal(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime, baseTime.Add(1 * time.Hour)}

	// Integer datasource
	intDS := createDatasource(t, "count", tsquery.DataTypeInteger, true, "", timestamps, []any{int64(10), int64(20)})

	intAgg := aggregation.NewFromDatasourceAggregation(intDS, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
		{ReductionType: tsquery.ReductionTypeAvg},
		{ReductionType: tsquery.ReductionTypeFirst},
	})

	intResult, err := intAgg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	intMetas := intResult.FieldsMeta()
	intFields, err := intResult.Fields().Get(ctx)
	require.NoError(t, err)

	// Sum of integers = integer
	require.Equal(t, tsquery.DataTypeInteger, intMetas[0].DataType())
	require.Equal(t, int64(30), intFields[0].Value)

	// Avg of integers = decimal
	require.Equal(t, tsquery.DataTypeDecimal, intMetas[1].DataType())
	require.InDelta(t, 15.0, intFields[1].Value.(float64), 0.0001)

	// First of integers preserves type
	require.Equal(t, tsquery.DataTypeInteger, intMetas[2].DataType())
	require.Equal(t, int64(10), intFields[2].Value)

	// Decimal datasource
	decDS := createDatasource(t, "temp", tsquery.DataTypeDecimal, true, "", timestamps, []any{10.5, 20.5})

	decAgg := aggregation.NewFromDatasourceAggregation(decDS, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
		{ReductionType: tsquery.ReductionTypeAvg},
		{ReductionType: tsquery.ReductionTypeLast},
	})

	decResult, err := decAgg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	decMetas := decResult.FieldsMeta()
	decFields, err := decResult.Fields().Get(ctx)
	require.NoError(t, err)

	// Sum of decimals = decimal
	require.Equal(t, tsquery.DataTypeDecimal, decMetas[0].DataType())
	require.Equal(t, 31.0, decFields[0].Value)

	// Avg of decimals = decimal
	require.Equal(t, tsquery.DataTypeDecimal, decMetas[1].DataType())
	require.InDelta(t, 15.5, decFields[1].Value.(float64), 0.0001)

	// Last preserves type
	require.Equal(t, tsquery.DataTypeDecimal, decMetas[2].DataType())
	require.Equal(t, 20.5, decFields[2].Value)
}

func TestAggregation_WithFilters(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}
	// Values: 10, 20, 30, 40
	values := []any{int64(10), int64(20), int64(30), int64(40)}
	rawDS := createDatasource(t, "energy", tsquery.DataTypeInteger, true, "kWh", timestamps, values)

	// Apply a delta filter: delta produces 3 values: 10, 10, 10
	filteredDS := datasource.NewFilteredDataSource(rawDS, datasource.NewDeltaFilter(false, 0, false))

	agg := aggregation.NewFromDatasourceAggregation(filteredDS, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
		{ReductionType: tsquery.ReductionTypeCount},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 2)

	// Delta produces 3 values of 10 each (20-10, 30-20, 40-30)
	require.Equal(t, int64(30), fields[0].Value) // sum = 10+10+10
	require.Equal(t, int64(3), fields[1].Value)  // count = 3
}

func TestAggregation_NonNumericRejection(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a string datasource
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("name", tsquery.DataTypeString, true, "", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Timestamp: baseTime, Value: "hello"},
	}
	ds, err := datasource.NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
	})

	_, err = agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-numeric")
}

func TestAggregation_SingleFieldAutoUrn(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "energy", tsquery.DataTypeInteger, true, "kWh", []time.Time{baseTime}, []any{int64(42)})

	// Single field — URN should be source URN without suffix
	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, "energy", result.FieldsMeta()[0].Urn())
}

func TestAggregation_DuplicateUrnRejection(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "energy", tsquery.DataTypeInteger, true, "kWh", []time.Time{baseTime}, []any{int64(42)})

	// Two sum fields without custom URN → both would auto-derive to "energy_sum"
	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
		{ReductionType: tsquery.ReductionTypeSum},
	})

	_, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate URN")
}

func TestAggregation_CustomMetaPropagation(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	customMeta := map[string]any{"source": "meter-1"}
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("energy", tsquery.DataTypeDecimal, true, "kWh", customMeta)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Timestamp: baseTime, Value: 42.0},
	}
	ds, err := datasource.NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, "meter-1", result.FieldsMeta()[0].GetCustomMetaStringValue("source"))
}

// --- Report Aggregation Tests ---

func TestReportAggregation_PerFieldReductions(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a static report datasource with 3 fields: energy, temperature, pressure
	energyMeta, _ := tsquery.NewFieldMetaWithCustomData("energy", tsquery.DataTypeDecimal, true, "kWh", nil)
	tempMeta, _ := tsquery.NewFieldMetaWithCustomData("temperature", tsquery.DataTypeDecimal, true, "°C", nil)
	pressureMeta, _ := tsquery.NewFieldMetaWithCustomData("pressure", tsquery.DataTypeInteger, true, "hPa", nil)

	fieldsMeta := []tsquery.FieldMeta{*energyMeta, *tempMeta, *pressureMeta}

	records := []timeseries.TsRecord[[]any]{
		{Timestamp: baseTime, Value: []any{10.0, 20.0, int64(1013)}},
		{Timestamp: baseTime.Add(1 * time.Hour), Value: []any{20.0, 25.0, int64(1015)}},
		{Timestamp: baseTime.Add(2 * time.Hour), Value: []any{30.0, 18.0, int64(1010)}},
	}

	reportDS, err := report.NewStaticDatasource(fieldsMeta, stream.FromSlice(records))
	require.NoError(t, err)

	agg := aggregation.NewFromReportAggregation(reportDS, []aggregation.ReportAggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum, SourceFieldUrn: "energy"},
		{ReductionType: tsquery.ReductionTypeAvg, SourceFieldUrn: "temperature"},
		{ReductionType: tsquery.ReductionTypeMax, SourceFieldUrn: "pressure"},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 3)

	// energy sum: 10 + 20 + 30 = 60
	require.Equal(t, 60.0, fields[0].Value)
	require.Equal(t, "energy_sum", metas[0].Urn())
	require.Equal(t, "kWh", metas[0].Unit())

	// temperature avg: (20 + 25 + 18) / 3 = 21
	require.InDelta(t, 21.0, fields[1].Value.(float64), 0.0001)
	require.Equal(t, "temperature_avg", metas[1].Urn())

	// pressure max: 1015 at hour 1
	require.Equal(t, int64(1015), fields[2].Value)
	require.Equal(t, "pressure_max", metas[2].Urn())
	require.Equal(t, "hPa", metas[2].Unit())
	require.NotNil(t, fields[2].Timestamp)
	require.Equal(t, baseTime.Add(1*time.Hour), *fields[2].Timestamp)
}

func TestReportAggregation_AutoDerivedMetadata(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	energyMeta, _ := tsquery.NewFieldMetaWithCustomData("energy", tsquery.DataTypeDecimal, true, "kWh", map[string]any{"source": "meter-1"})
	fieldsMeta := []tsquery.FieldMeta{*energyMeta}

	records := []timeseries.TsRecord[[]any]{
		{Timestamp: baseTime, Value: []any{42.0}},
	}
	reportDS, err := report.NewStaticDatasource(fieldsMeta, stream.FromSlice(records))
	require.NoError(t, err)

	agg := aggregation.NewFromReportAggregation(reportDS, []aggregation.ReportAggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum, SourceFieldUrn: "energy"},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 1)

	// Single field — URN should be source URN without suffix
	require.Equal(t, "energy", metas[0].Urn())
	require.Equal(t, "kWh", metas[0].Unit())
	require.Equal(t, "meter-1", metas[0].GetCustomMetaStringValue("source"))
}

func TestReportAggregation_InvalidSourceFieldUrn(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	energyMeta, _ := tsquery.NewFieldMetaWithCustomData("energy", tsquery.DataTypeDecimal, true, "kWh", nil)
	fieldsMeta := []tsquery.FieldMeta{*energyMeta}

	records := []timeseries.TsRecord[[]any]{
		{Timestamp: baseTime, Value: []any{42.0}},
	}
	reportDS, err := report.NewStaticDatasource(fieldsMeta, stream.FromSlice(records))
	require.NoError(t, err)

	agg := aggregation.NewFromReportAggregation(reportDS, []aggregation.ReportAggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum, SourceFieldUrn: "nonexistent"},
	})

	_, err = agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestReportAggregation_FirstLastOnReport(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	energyMeta, _ := tsquery.NewFieldMetaWithCustomData("energy", tsquery.DataTypeDecimal, true, "kWh", nil)
	fieldsMeta := []tsquery.FieldMeta{*energyMeta}

	records := []timeseries.TsRecord[[]any]{
		{Timestamp: baseTime, Value: []any{10.0}},
		{Timestamp: baseTime.Add(1 * time.Hour), Value: []any{20.0}},
		{Timestamp: baseTime.Add(2 * time.Hour), Value: []any{30.0}},
	}
	reportDS, err := report.NewStaticDatasource(fieldsMeta, stream.FromSlice(records))
	require.NoError(t, err)

	agg := aggregation.NewFromReportAggregation(reportDS, []aggregation.ReportAggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeFirst, SourceFieldUrn: "energy"},
		{ReductionType: tsquery.ReductionTypeLast, SourceFieldUrn: "energy"},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 2)

	// first: 10 at baseTime
	require.Equal(t, 10.0, fields[0].Value)
	require.Equal(t, baseTime, *fields[0].Timestamp)

	// last: 30 at baseTime+2h
	require.Equal(t, 30.0, fields[1].Value)
	require.Equal(t, baseTime.Add(2*time.Hour), *fields[1].Timestamp)
}

// --- Composite Aggregation Tests ---

func TestCompositeAggregation(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// First aggregator: datasource aggregation on energy
	energyDS := createDatasource(t, "energy", tsquery.DataTypeInteger, true, "kWh",
		[]time.Time{baseTime, baseTime.Add(1 * time.Hour)},
		[]any{int64(10), int64(20)},
	)
	dsAgg := aggregation.NewFromDatasourceAggregation(energyDS, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
	})

	// Second aggregator: report aggregation on temperature
	tempMeta, _ := tsquery.NewFieldMetaWithCustomData("temperature", tsquery.DataTypeDecimal, true, "°C", nil)
	reportRecords := []timeseries.TsRecord[[]any]{
		{Timestamp: baseTime, Value: []any{20.0}},
		{Timestamp: baseTime.Add(1 * time.Hour), Value: []any{25.0}},
		{Timestamp: baseTime.Add(2 * time.Hour), Value: []any{18.0}},
	}
	reportDS, err := report.NewStaticDatasource([]tsquery.FieldMeta{*tempMeta}, stream.FromSlice(reportRecords))
	require.NoError(t, err)
	reportAgg := aggregation.NewFromReportAggregation(reportDS, []aggregation.ReportAggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeMax, SourceFieldUrn: "temperature"},
	})

	// Composite
	composite := aggregation.NewCompositeAggregation(dsAgg, reportAgg)
	result, err := composite.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	require.Len(t, metas, 2)
	require.Equal(t, "energy", metas[0].Urn())
	require.Equal(t, "temperature", metas[1].Urn())

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 2)

	// energy sum: 10 + 20 = 30
	require.Equal(t, int64(30), fields[0].Value)

	// temperature max: 25
	require.Equal(t, 25.0, fields[1].Value)
	require.NotNil(t, fields[1].Timestamp)
	require.Equal(t, baseTime.Add(1*time.Hour), *fields[1].Timestamp)
}

func TestCompositeAggregation_DuplicateUrnRejection(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Two aggregators producing same URN "energy"
	ds1 := createDatasource(t, "energy", tsquery.DataTypeInteger, true, "kWh",
		[]time.Time{baseTime}, []any{int64(10)},
	)
	ds2 := createDatasource(t, "energy", tsquery.DataTypeInteger, true, "kWh",
		[]time.Time{baseTime}, []any{int64(20)},
	)

	agg1 := aggregation.NewFromDatasourceAggregation(ds1, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
	})
	agg2 := aggregation.NewFromDatasourceAggregation(ds2, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
	})

	composite := aggregation.NewCompositeAggregation(agg1, agg2)
	_, err := composite.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate URN")
}

func TestCompositeAggregation_MetadataAvailableBeforeGet(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "energy", tsquery.DataTypeInteger, true, "kWh",
		[]time.Time{baseTime}, []any{int64(42)},
	)
	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
		{ReductionType: tsquery.ReductionTypeCount},
	})

	composite := aggregation.NewCompositeAggregation(agg)
	result, err := composite.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Metadata should be available before calling Fields().Get()
	metas := result.FieldsMeta()
	require.Len(t, metas, 2)
	require.Equal(t, "energy_sum", metas[0].Urn())
	require.Equal(t, "energy_count", metas[1].Urn())
	require.Equal(t, tsquery.DataTypeInteger, metas[0].DataType())
	require.Equal(t, tsquery.DataTypeInteger, metas[1].DataType())
}

// --- ReductionDatasource first/last Tests ---

func TestReductionDatasource_First(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	// 3 datasources with values 100, 200, 300
	ds1 := createDatasource(t, "DS1", tsquery.DataTypeInteger, true, "items", timestamps, []any{int64(100)})
	ds2 := createDatasource(t, "DS2", tsquery.DataTypeInteger, true, "items", timestamps, []any{int64(200)})
	ds3 := createDatasource(t, "DS3", tsquery.DataTypeInteger, true, "items", timestamps, []any{int64(300)})

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds1, ds2, ds3})

	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeFirst,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "first_value"},
	)

	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 1)
	require.Equal(t, int64(100), records[0].Value) // first = first datasource's value
}

func TestReductionDatasource_Last(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	ds1 := createDatasource(t, "DS1", tsquery.DataTypeInteger, true, "items", timestamps, []any{int64(100)})
	ds2 := createDatasource(t, "DS2", tsquery.DataTypeInteger, true, "items", timestamps, []any{int64(200)})
	ds3 := createDatasource(t, "DS3", tsquery.DataTypeInteger, true, "items", timestamps, []any{int64(300)})

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds1, ds2, ds3})

	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeLast,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "last_value"},
	)

	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 1)
	require.Equal(t, int64(300), records[0].Value) // last = last datasource's value
}

func TestReductionDatasource_FirstLast_SingleDatasource(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime, baseTime.Add(1 * time.Hour)}

	ds := createDatasource(t, "DS1", tsquery.DataTypeInteger, true, "", timestamps, []any{int64(42), int64(99)})

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds})

	// First with single datasource: identity optimization
	firstDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeFirst,
		alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
		multiDS,
		tsquery.AddFieldMeta{Urn: "result"},
	)

	firstResult, err := firstDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	firstRecords := firstResult.Data().MustCollect()
	require.Len(t, firstRecords, 2)
	require.Equal(t, int64(42), firstRecords[0].Value)
	require.Equal(t, int64(99), firstRecords[1].Value)
}

func TestReductionDatasource_AllSevenReductions(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	// 3 integer datasources
	ds1Int := createDatasource(t, "DS1", tsquery.DataTypeInteger, true, "", timestamps, []any{int64(10)})
	ds2Int := createDatasource(t, "DS2", tsquery.DataTypeInteger, true, "", timestamps, []any{int64(20)})
	ds3Int := createDatasource(t, "DS3", tsquery.DataTypeInteger, true, "", timestamps, []any{int64(30)})

	reductionTypes := []tsquery.ReductionType{
		tsquery.ReductionTypeSum,
		tsquery.ReductionTypeAvg,
		tsquery.ReductionTypeMin,
		tsquery.ReductionTypeMax,
		tsquery.ReductionTypeCount,
		tsquery.ReductionTypeFirst,
		tsquery.ReductionTypeLast,
	}

	expectedValues := []any{
		int64(60), // sum: 10+20+30
		20.0,      // avg: 60/3
		int64(10), // min
		int64(30), // max
		int64(3),  // count
		int64(10), // first
		int64(30), // last
	}

	for i, rt := range reductionTypes {
		t.Run(string(rt), func(t *testing.T) {
			multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds1Int, ds2Int, ds3Int})
			reductionDS := datasource.NewReductionDatasource(
				rt,
				alignerFilterPtr(datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC))),
				multiDS,
				tsquery.AddFieldMeta{Urn: "result"},
			)

			result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
			require.NoError(t, err)

			records := result.Data().MustCollect()
			require.Len(t, records, 1)

			expected := expectedValues[i]
			switch ev := expected.(type) {
			case float64:
				require.InDelta(t, ev, records[0].Value.(float64), 0.0001)
			default:
				require.Equal(t, expected, records[0].Value)
			}
		})
	}
}

// --- Expression Aggregation Tests ---

func TestExpressionAggregation_BinaryOps(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "value", tsquery.DataTypeDecimal, true, "",
		[]time.Time{baseTime, baseTime.Add(1 * time.Hour)},
		[]any{10.0, 20.0},
	)

	sourceAgg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum, AddFieldMeta: &tsquery.AddFieldMeta{Urn: "total"}},
		{ReductionType: tsquery.ReductionTypeCount, AddFieldMeta: &tsquery.AddFieldMeta{Urn: "cnt"}},
	})

	tests := []struct {
		name     string
		op       tsquery.BinaryNumericOperatorType
		op1      aggregation.AggregationValue
		op2      aggregation.AggregationValue
		expected float64
	}{
		{
			"add",
			tsquery.BinaryNumericOperatorAdd,
			aggregation.NewRefAggregationValue("total"),
			aggregation.NewConstantAggregationValue(tsquery.DataTypeDecimal, 5.0),
			35.0, // sum=30, 30+5=35
		},
		{
			"sub",
			tsquery.BinaryNumericOperatorSub,
			aggregation.NewRefAggregationValue("total"),
			aggregation.NewConstantAggregationValue(tsquery.DataTypeDecimal, 5.0),
			25.0, // 30-5=25
		},
		{
			"mul",
			tsquery.BinaryNumericOperatorMul,
			aggregation.NewRefAggregationValue("total"),
			aggregation.NewConstantAggregationValue(tsquery.DataTypeDecimal, 2.0),
			60.0, // 30*2=60
		},
		{
			"div",
			tsquery.BinaryNumericOperatorDiv,
			aggregation.NewRefAggregationValue("total"),
			aggregation.NewConstantAggregationValue(tsquery.DataTypeDecimal, 3.0),
			10.0, // 30/3=10
		},
		{
			"pow",
			tsquery.BinaryNumericOperatorPow,
			aggregation.NewRefAggregationValue("total"),
			aggregation.NewConstantAggregationValue(tsquery.DataTypeDecimal, 2.0),
			900.0, // 30^2=900
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exprAgg := aggregation.NewExpressionAggregation(sourceAgg, []aggregation.ExpressionAggregationFieldDef{
				{
					AddFieldMeta: tsquery.AddFieldMeta{Urn: "result"},
					Value:        aggregation.NewNumericExpressionAggregationValue(tt.op1, tt.op, tt.op2),
				},
			})

			result, err := exprAgg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
			require.NoError(t, err)

			fields, err := result.Fields().Get(ctx)
			require.NoError(t, err)
			require.Len(t, fields, 1)
			require.InDelta(t, tt.expected, fields[0].Value.(float64), 0.0001)
		})
	}
}

func TestExpressionAggregation_OnlyExpressionFieldsInOutput(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "value", tsquery.DataTypeInteger, true, "",
		[]time.Time{baseTime},
		[]any{int64(42)},
	)

	sourceAgg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum, AddFieldMeta: &tsquery.AddFieldMeta{Urn: "total"}},
		{ReductionType: tsquery.ReductionTypeCount, AddFieldMeta: &tsquery.AddFieldMeta{Urn: "cnt"}},
	})

	// Only output one computed field — source fields (total, cnt) should NOT appear
	exprAgg := aggregation.NewExpressionAggregation(sourceAgg, []aggregation.ExpressionAggregationFieldDef{
		{
			AddFieldMeta: tsquery.AddFieldMeta{Urn: "doubled"},
			Value: aggregation.NewNumericExpressionAggregationValue(
				aggregation.NewRefAggregationValue("total"),
				tsquery.BinaryNumericOperatorMul,
				aggregation.NewConstantAggregationValue(tsquery.DataTypeInteger, int64(2)),
			),
		},
	})

	result, err := exprAgg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	require.Len(t, metas, 1, "only expression fields should appear in output")
	require.Equal(t, "doubled", metas[0].Urn())

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Equal(t, int64(84), fields[0].Value)
}

func TestExpressionAggregation_MultipleFields(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "value", tsquery.DataTypeDecimal, true, "",
		[]time.Time{baseTime, baseTime.Add(1 * time.Hour), baseTime.Add(2 * time.Hour)},
		[]any{10.0, 20.0, 30.0},
	)

	sourceAgg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum, AddFieldMeta: &tsquery.AddFieldMeta{Urn: "total"}},
		{ReductionType: tsquery.ReductionTypeCount, AddFieldMeta: &tsquery.AddFieldMeta{Urn: "cnt"}},
	})

	exprAgg := aggregation.NewExpressionAggregation(sourceAgg, []aggregation.ExpressionAggregationFieldDef{
		{
			AddFieldMeta: tsquery.AddFieldMeta{Urn: "sum_x2"},
			Value: aggregation.NewNumericExpressionAggregationValue(
				aggregation.NewRefAggregationValue("total"),
				tsquery.BinaryNumericOperatorMul,
				aggregation.NewConstantAggregationValue(tsquery.DataTypeDecimal, 2.0),
			),
		},
		{
			AddFieldMeta: tsquery.AddFieldMeta{Urn: "avg"},
			Value: aggregation.NewNumericExpressionAggregationValue(
				aggregation.NewRefAggregationValue("total"),
				tsquery.BinaryNumericOperatorDiv,
				aggregation.NewCastAggregationValue(
					aggregation.NewRefAggregationValue("cnt"),
					tsquery.DataTypeDecimal,
				),
			),
		},
		{
			AddFieldMeta: tsquery.AddFieldMeta{Urn: "total_passthrough"},
			Value:        aggregation.NewRefAggregationValue("total"),
		},
	})

	result, err := exprAgg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	require.Len(t, metas, 3)
	require.Equal(t, "sum_x2", metas[0].Urn())
	require.Equal(t, "avg", metas[1].Urn())
	require.Equal(t, "total_passthrough", metas[2].Urn())

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 3)

	// total=60, cnt=3
	require.InDelta(t, 120.0, fields[0].Value.(float64), 0.0001) // 60*2
	require.InDelta(t, 20.0, fields[1].Value.(float64), 0.0001)  // 60/3
	require.Equal(t, 60.0, fields[2].Value)                      // passthrough
}
