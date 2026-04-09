package tsquery_test

import (
	"context"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/aggregation"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFromDatasourceAggregation_DefaultsGauge verifies that without any
// OverrideMetricKind, an aggregation's result FieldMeta defaults to Gauge
// (aggregation produces a terminal scalar — gauge is the safe default).
func TestFromDatasourceAggregation_DefaultsGauge(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Source datasource tagged as Cumulative — but aggregation result should NOT inherit
	srcMeta, err := tsquery.NewFieldMetaFull("energy", tsquery.DataTypeDecimal, tsquery.MetricKindCumulative, true, "kWh", nil)
	require.NoError(t, err)
	ds, err := datasource.NewStaticDatasource(*srcMeta, stream.Just(
		timeseries.TsRecord[any]{Value: 10.0, Timestamp: baseTime},
		timeseries.TsRecord[any]{Value: 20.0, Timestamp: baseTime.Add(time.Hour)},
	))
	require.NoError(t, err)

	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	require.Len(t, metas, 1)
	// Aggregation output defaults to Gauge, does NOT inherit source Cumulative
	assert.Equal(t, tsquery.MetricKindGauge, metas[0].MetricKind())
}

// TestFromDatasourceAggregation_OverrideMetricKind verifies that
// AddFieldMeta.OverrideMetricKind is honored by the datasource-based aggregation.
func TestFromDatasourceAggregation_OverrideMetricKind(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	srcMeta, err := tsquery.NewFieldMetaFull("energy", tsquery.DataTypeDecimal, tsquery.MetricKindGauge, true, "kWh", nil)
	require.NoError(t, err)
	ds, err := datasource.NewStaticDatasource(*srcMeta, stream.Just(
		timeseries.TsRecord[any]{Value: 10.0, Timestamp: baseTime},
		timeseries.TsRecord[any]{Value: 20.0, Timestamp: baseTime.Add(time.Hour)},
	))
	require.NoError(t, err)

	agg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{
			ReductionType: tsquery.ReductionTypeSum,
			AddFieldMeta: &tsquery.AddFieldMeta{
				Urn:                "total_energy",
				OverrideMetricKind: tsquery.MetricKindDelta,
			},
		},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	require.Len(t, metas, 1)
	assert.Equal(t, "total_energy", metas[0].Urn())
	assert.Equal(t, tsquery.MetricKindDelta, metas[0].MetricKind())
}

// TestFromReportAggregation_DefaultsGauge verifies the report-based aggregation
// result defaults to Gauge when no override is set.
func TestFromReportAggregation_DefaultsGauge(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	energyMeta, err := tsquery.NewFieldMetaFull("energy", tsquery.DataTypeDecimal, tsquery.MetricKindCumulative, true, "kWh", nil)
	require.NoError(t, err)
	fieldsMeta := []tsquery.FieldMeta{*energyMeta}

	records := []timeseries.TsRecord[[]any]{
		{Timestamp: baseTime, Value: []any{10.0}},
		{Timestamp: baseTime.Add(time.Hour), Value: []any{20.0}},
	}
	reportDS, err := report.NewStaticDatasource(fieldsMeta, stream.FromSlice(records))
	require.NoError(t, err)

	agg := aggregation.NewFromReportAggregation(reportDS, []aggregation.ReportAggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum, SourceFieldUrn: "energy"},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	require.Len(t, metas, 1)
	assert.Equal(t, tsquery.MetricKindGauge, metas[0].MetricKind())
}

// TestFromReportAggregation_OverrideMetricKind verifies that
// AddFieldMeta.OverrideMetricKind is honored by the report-based aggregation.
func TestFromReportAggregation_OverrideMetricKind(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	energyMeta, err := tsquery.NewFieldMetaFull("energy", tsquery.DataTypeDecimal, tsquery.MetricKindGauge, true, "kWh", nil)
	require.NoError(t, err)
	fieldsMeta := []tsquery.FieldMeta{*energyMeta}

	records := []timeseries.TsRecord[[]any]{
		{Timestamp: baseTime, Value: []any{10.0}},
		{Timestamp: baseTime.Add(time.Hour), Value: []any{20.0}},
	}
	reportDS, err := report.NewStaticDatasource(fieldsMeta, stream.FromSlice(records))
	require.NoError(t, err)

	agg := aggregation.NewFromReportAggregation(reportDS, []aggregation.ReportAggregationFieldDef{
		{
			ReductionType:  tsquery.ReductionTypeSum,
			SourceFieldUrn: "energy",
			AddFieldMeta: &tsquery.AddFieldMeta{
				Urn:                "total_energy",
				OverrideMetricKind: tsquery.MetricKindCumulative,
			},
		},
	})

	result, err := agg.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	metas := result.FieldsMeta()
	require.Len(t, metas, 1)
	assert.Equal(t, "total_energy", metas[0].Urn())
	assert.Equal(t, tsquery.MetricKindCumulative, metas[0].MetricKind())
}

// TestExpressionAggregation_DefaultsGauge verifies that expression aggregation
// results default to Gauge without override.
func TestExpressionAggregation_DefaultsGauge(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "value", tsquery.DataTypeInteger, true, "",
		[]time.Time{baseTime},
		[]any{int64(42)},
	)

	sourceAgg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum, AddFieldMeta: &tsquery.AddFieldMeta{Urn: "total"}},
	})

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
	require.Len(t, metas, 1)
	assert.Equal(t, tsquery.MetricKindGauge, metas[0].MetricKind())
}

// TestExpressionAggregation_OverrideMetricKind verifies that
// AddFieldMeta.OverrideMetricKind is honored by expression aggregation.
func TestExpressionAggregation_OverrideMetricKind(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ds := createDatasource(t, "value", tsquery.DataTypeInteger, true, "",
		[]time.Time{baseTime},
		[]any{int64(42)},
	)

	sourceAgg := aggregation.NewFromDatasourceAggregation(ds, []aggregation.AggregationFieldDef{
		{ReductionType: tsquery.ReductionTypeSum, AddFieldMeta: &tsquery.AddFieldMeta{Urn: "total"}},
	})

	exprAgg := aggregation.NewExpressionAggregation(sourceAgg, []aggregation.ExpressionAggregationFieldDef{
		{
			AddFieldMeta: tsquery.AddFieldMeta{
				Urn:                "doubled",
				OverrideMetricKind: tsquery.MetricKindDelta,
			},
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
	require.Len(t, metas, 1)
	assert.Equal(t, "doubled", metas[0].Urn())
	assert.Equal(t, tsquery.MetricKindDelta, metas[0].MetricKind())
}
