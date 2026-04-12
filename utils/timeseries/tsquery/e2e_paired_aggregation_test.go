package tsquery_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/aggregation"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
	"github.com/stretchr/testify/require"
)

// createPairedReportDS creates a report datasource with "actual" and "predicted" decimal fields.
func createPairedReportDS(t *testing.T, actuals, predictions []float64) report.DataSource {
	t.Helper()
	require.Equal(t, len(actuals), len(predictions), "actuals and predictions must have same length")

	actualMeta, _ := tsquery.NewFieldMetaWithCustomData("actual", tsquery.DataTypeDecimal, true, "kWh", nil)
	predictedMeta, _ := tsquery.NewFieldMetaWithCustomData("predicted", tsquery.DataTypeDecimal, true, "kWh", nil)
	fieldsMeta := []tsquery.FieldMeta{*actualMeta, *predictedMeta}

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	records := make([]timeseries.TsRecord[[]any], len(actuals))
	for i := range actuals {
		records[i] = timeseries.TsRecord[[]any]{
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
			Value:     []any{actuals[i], predictions[i]},
		}
	}

	ds, err := report.NewStaticDatasource(fieldsMeta, stream.FromSlice(records))
	require.NoError(t, err)
	return ds
}

func TestE2EPairedAggregation_AllSixReductions(t *testing.T) {
	ctx := context.Background()
	ds := createPairedReportDS(t,
		[]float64{10, 20, 30, 40, 50},
		[]float64{12, 18, 33, 38, 52},
	)
	// errors (a-p): -2, 2, -3, 2, -2
	// |errors|: 2, 2, 3, 2, 2 → MAE = 11/5 = 2.2
	// sq errors: 4, 4, 9, 4, 4 → MSE = 25/5 = 5 → RMSE = √5
	// MBE = (-2+2-3+2-2)/5 = -3/5 = -0.6

	agg := aggregation.NewFromReportAggregation(ds, []aggregation.ReportAggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeMAE, SourceFieldUrn: "actual", CompareFieldUrn: "predicted",
			AddFieldMeta: &tsquery.AddFieldMeta{Urn: "mae"}},
		{ReductionType: tsquery.ReductionTypeRMSE, SourceFieldUrn: "actual", CompareFieldUrn: "predicted",
			AddFieldMeta: &tsquery.AddFieldMeta{Urn: "rmse"}},
		{ReductionType: tsquery.ReductionTypeMBE, SourceFieldUrn: "actual", CompareFieldUrn: "predicted",
			AddFieldMeta: &tsquery.AddFieldMeta{Urn: "mbe"}},
		{ReductionType: tsquery.ReductionTypeMAPE, SourceFieldUrn: "actual", CompareFieldUrn: "predicted",
			AddFieldMeta: &tsquery.AddFieldMeta{Urn: "mape"}},
		{ReductionType: tsquery.ReductionTypePearson, SourceFieldUrn: "actual", CompareFieldUrn: "predicted",
			AddFieldMeta: &tsquery.AddFieldMeta{Urn: "pearson"}},
		{ReductionType: tsquery.ReductionTypeR2, SourceFieldUrn: "actual", CompareFieldUrn: "predicted",
			AddFieldMeta: &tsquery.AddFieldMeta{Urn: "r2"}},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 6)

	// MAE = 2.2 (unit: kWh — error in source units)
	require.Equal(t, "mae", metas[0].Urn())
	require.Equal(t, tsquery.DataTypeDecimal, metas[0].DataType())
	require.Equal(t, "kWh", metas[0].Unit())
	require.InDelta(t, 2.2, fields[0].Value.(float64), 0.0001)
	require.Nil(t, fields[0].Timestamp)

	// RMSE = √5 ≈ 2.2360 (unit: kWh)
	require.Equal(t, "rmse", metas[1].Urn())
	require.Equal(t, "kWh", metas[1].Unit())
	require.InDelta(t, math.Sqrt(5.0), fields[1].Value.(float64), 0.0001)

	// MBE = -0.6 (unit: kWh)
	require.Equal(t, "mbe", metas[2].Urn())
	require.Equal(t, "kWh", metas[2].Unit())
	require.InDelta(t, -0.6, fields[2].Value.(float64), 0.0001)

	// MAPE: |(-2)/10|=0.2, |2/20|=0.1, |(-3)/30|=0.1, |2/40|=0.05, |(-2)/50|=0.04 → mean=0.098 → 9.8%
	// (unit: percent — dimensionless percentage)
	require.Equal(t, "mape", metas[3].Urn())
	require.Equal(t, "percent", metas[3].Unit())
	require.InDelta(t, 9.8, fields[3].Value.(float64), 0.0001)

	// Pearson: high positive correlation (unit: empty — dimensionless coefficient)
	require.Equal(t, "pearson", metas[4].Urn())
	require.Equal(t, "", metas[4].Unit())
	pearson := fields[4].Value.(float64)
	require.True(t, pearson > 0.98, "pearson should be > 0.98 for near-perfect linear, got %f", pearson)

	// R²: high explanatory power (unit: empty — dimensionless coefficient)
	require.Equal(t, "r2", metas[5].Urn())
	require.Equal(t, "", metas[5].Unit())
	r2 := fields[5].Value.(float64)
	require.True(t, r2 > 0.97, "R² should be > 0.97 for near-perfect, got %f", r2)
}

func TestE2EPairedAggregation_MixedWithSingleFieldReductions(t *testing.T) {
	ctx := context.Background()
	ds := createPairedReportDS(t,
		[]float64{10, 20, 30},
		[]float64{12, 18, 25},
	)

	// Mix single-field (avg of actual) with paired (MAE)
	agg := aggregation.NewFromReportAggregation(ds, []aggregation.ReportAggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeAvg, SourceFieldUrn: "actual",
			AddFieldMeta: &tsquery.AddFieldMeta{Urn: "avg_actual"}},
		{ReductionType: tsquery.ReductionTypeMAE, SourceFieldUrn: "actual", CompareFieldUrn: "predicted",
			AddFieldMeta: &tsquery.AddFieldMeta{Urn: "mae"}},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 2)

	// avg(actual) = (10+20+30)/3 = 20
	require.InDelta(t, 20.0, fields[0].Value.(float64), 0.0001)

	// MAE = (2+2+5)/3 = 3.0
	require.InDelta(t, 3.0, fields[1].Value.(float64), 0.0001)
}

func TestE2EPairedAggregation_EmptyStream(t *testing.T) {
	ctx := context.Background()

	actualMeta, _ := tsquery.NewFieldMetaWithCustomData("actual", tsquery.DataTypeDecimal, true, "kWh", nil)
	predictedMeta, _ := tsquery.NewFieldMetaWithCustomData("predicted", tsquery.DataTypeDecimal, true, "kWh", nil)
	fieldsMeta := []tsquery.FieldMeta{*actualMeta, *predictedMeta}

	ds, err := report.NewStaticDatasource(fieldsMeta, stream.Empty[timeseries.TsRecord[[]any]]())
	require.NoError(t, err)

	agg := aggregation.NewFromReportAggregation(ds, []aggregation.ReportAggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeMAE, SourceFieldUrn: "actual", CompareFieldUrn: "predicted",
			AddFieldMeta: &tsquery.AddFieldMeta{Urn: "mae"}},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Nil(t, fields[0].Value)
}

func TestE2EPairedAggregation_ValidationErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("missing compareFieldUrn for paired reduction", func(t *testing.T) {
		ds := createPairedReportDS(t, []float64{1}, []float64{2})
		agg := aggregation.NewFromReportAggregation(ds, []aggregation.ReportAggregationFieldDef{
			{ReductionType: tsquery.ReductionTypeMAE, SourceFieldUrn: "actual"},
		})

		_, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Error(t, err)
		require.Contains(t, err.Error(), "compareFieldUrn")
	})

	t.Run("compareFieldUrn set for non-paired reduction", func(t *testing.T) {
		ds := createPairedReportDS(t, []float64{1}, []float64{2})
		agg := aggregation.NewFromReportAggregation(ds, []aggregation.ReportAggregationFieldDef{
			{ReductionType: tsquery.ReductionTypeSum, SourceFieldUrn: "actual", CompareFieldUrn: "predicted"},
		})

		_, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Error(t, err)
		require.Contains(t, err.Error(), "must not be set")
	})

	t.Run("compare field URN not found", func(t *testing.T) {
		ds := createPairedReportDS(t, []float64{1}, []float64{2})
		agg := aggregation.NewFromReportAggregation(ds, []aggregation.ReportAggregationFieldDef{
			{ReductionType: tsquery.ReductionTypeMAE, SourceFieldUrn: "actual", CompareFieldUrn: "nonexistent"},
		})

		_, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not found")
	})

	t.Run("non-numeric compare field", func(t *testing.T) {
		actualMeta, _ := tsquery.NewFieldMetaWithCustomData("actual", tsquery.DataTypeDecimal, true, "", nil)
		labelMeta, _ := tsquery.NewFieldMetaWithCustomData("label", tsquery.DataTypeString, true, "", nil)
		fieldsMeta := []tsquery.FieldMeta{*actualMeta, *labelMeta}

		baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		records := []timeseries.TsRecord[[]any]{
			{Timestamp: baseTime, Value: []any{1.0, "foo"}},
		}
		ds, err := report.NewStaticDatasource(fieldsMeta, stream.FromSlice(records))
		require.NoError(t, err)

		agg := aggregation.NewFromReportAggregation(ds, []aggregation.ReportAggregationFieldDef{
			{ReductionType: tsquery.ReductionTypeMAE, SourceFieldUrn: "actual", CompareFieldUrn: "label"},
		})

		_, err = agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-numeric compare field")
	})
}

func TestE2EPairedAggregation_URNAutoDerivation(t *testing.T) {
	ctx := context.Background()
	ds := createPairedReportDS(t, []float64{10, 20}, []float64{12, 18})

	agg := aggregation.NewFromReportAggregation(ds, []aggregation.ReportAggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeMAE, SourceFieldUrn: "actual", CompareFieldUrn: "predicted"},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	// Single field — should use source URN (auto-derived)
	require.Equal(t, "actual", metas[0].Urn())
}

func TestE2EPairedAggregation_MetadataCorrectness(t *testing.T) {
	ctx := context.Background()
	ds := createPairedReportDS(t, []float64{10, 20}, []float64{12, 18})

	agg := aggregation.NewFromReportAggregation(ds, []aggregation.ReportAggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeMAE, SourceFieldUrn: "actual", CompareFieldUrn: "predicted",
			AddFieldMeta: &tsquery.AddFieldMeta{Urn: "forecast_mae", OverrideUnit: "kWh"}},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	require.Equal(t, "forecast_mae", metas[0].Urn())
	require.Equal(t, tsquery.DataTypeDecimal, metas[0].DataType())
	require.Equal(t, "kWh", metas[0].Unit())
}

func TestE2EPairedAggregation_IntegerInputs(t *testing.T) {
	ctx := context.Background()

	actualMeta, _ := tsquery.NewFieldMetaWithCustomData("actual", tsquery.DataTypeInteger, true, "", nil)
	predictedMeta, _ := tsquery.NewFieldMetaWithCustomData("predicted", tsquery.DataTypeInteger, true, "", nil)
	fieldsMeta := []tsquery.FieldMeta{*actualMeta, *predictedMeta}

	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	records := []timeseries.TsRecord[[]any]{
		{Timestamp: baseTime, Value: []any{int64(10), int64(12)}},
		{Timestamp: baseTime.Add(time.Hour), Value: []any{int64(20), int64(18)}},
	}
	ds, err := report.NewStaticDatasource(fieldsMeta, stream.FromSlice(records))
	require.NoError(t, err)

	agg := aggregation.NewFromReportAggregation(ds, []aggregation.ReportAggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeMAE, SourceFieldUrn: "actual", CompareFieldUrn: "predicted",
			AddFieldMeta: &tsquery.AddFieldMeta{Urn: "mae"}},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	require.Equal(t, tsquery.DataTypeDecimal, metas[0].DataType()) // paired always produces decimal

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.InDelta(t, 2.0, fields[0].Value.(float64), 0.0001)
}
