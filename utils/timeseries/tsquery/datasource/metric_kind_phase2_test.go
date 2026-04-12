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

// --- Aligner smart defaults ---

func TestAlignerFilter_AutoSum_DeltaInOneBucket(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta, true, "kWh", nil)
	require.NoError(t, err)

	// Three delta values all within a single 15-minute bucket
	records := []timeseries.TsRecord[any]{
		{Value: 10.0, Timestamp: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)},
		{Value: 15.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
		{Value: 20.0, Timestamp: time.Date(2024, 1, 1, 0, 10, 0, 0, time.UTC)},
	}
	result := Result{meta: *fm, data: stream.Just(records...)}

	af := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(15*time.Minute, time.UTC))
	filtered, err := af.Filter(ctx, result)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindDelta, filtered.Meta().MetricKind())

	collected := filtered.Data().MustCollect()
	require.Len(t, collected, 1)
	require.Equal(t, 45.0, collected[0].Value) // 10 + 15 + 20
}

func TestAlignerFilter_AutoSum_DeltaMultipleBuckets(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta, true, "kWh", nil)
	require.NoError(t, err)

	// Delta values spanning two 15-minute buckets
	records := []timeseries.TsRecord[any]{
		{Value: 10.0, Timestamp: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)},  // bucket [0:00-0:15)
		{Value: 15.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},  // bucket [0:00-0:15)
		{Value: 20.0, Timestamp: time.Date(2024, 1, 1, 0, 16, 0, 0, time.UTC)}, // bucket [0:15-0:30)
		{Value: 25.0, Timestamp: time.Date(2024, 1, 1, 0, 20, 0, 0, time.UTC)}, // bucket [0:15-0:30)
	}
	result := Result{meta: *fm, data: stream.Just(records...)}

	af := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(15*time.Minute, time.UTC))
	filtered, err := af.Filter(ctx, result)
	require.NoError(t, err)

	collected := filtered.Data().MustCollect()
	require.Len(t, collected, 2)
	require.Equal(t, 25.0, collected[0].Value) // 10 + 15
	require.Equal(t, 45.0, collected[1].Value) // 20 + 25
}

func TestAlignerFilter_AutoLast_CumulativeInOneBucket(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("counter", tsquery.DataTypeDecimal, tsquery.MetricKindCumulative, true, "kWh", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)},
		{Value: 110.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
		{Value: 125.0, Timestamp: time.Date(2024, 1, 1, 0, 10, 0, 0, time.UTC)},
	}
	result := Result{meta: *fm, data: stream.Just(records...)}

	af := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(15*time.Minute, time.UTC))
	filtered, err := af.Filter(ctx, result)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindCumulative, filtered.Meta().MetricKind())

	collected := filtered.Data().MustCollect()
	require.Len(t, collected, 1)
	require.Equal(t, 125.0, collected[0].Value) // last value in bucket
}

func TestAlignerFilter_AutoLast_CumulativeStandaloneDownsampling(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("counter", tsquery.DataTypeDecimal, tsquery.MetricKindCumulative, true, "kWh", nil)
	require.NoError(t, err)

	// Cumulative counter across two 15-minute buckets (no DeltaFilter)
	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)},
		{Value: 110.0, Timestamp: time.Date(2024, 1, 1, 0, 10, 0, 0, time.UTC)},
		{Value: 125.0, Timestamp: time.Date(2024, 1, 1, 0, 16, 0, 0, time.UTC)},
		{Value: 140.0, Timestamp: time.Date(2024, 1, 1, 0, 25, 0, 0, time.UTC)},
	}
	result := Result{meta: *fm, data: stream.Just(records...)}

	af := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(15*time.Minute, time.UTC))
	filtered, err := af.Filter(ctx, result)
	require.NoError(t, err)

	collected := filtered.Data().MustCollect()
	require.Len(t, collected, 2)
	require.Equal(t, 110.0, collected[0].Value) // last in [0:00-0:15)
	require.Equal(t, 140.0, collected[1].Value) // last in [0:15-0:30)
}

func TestAlignerFilter_ExplicitOverridesAutoSum(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta, true, "kWh", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: 10.0, Timestamp: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)},
		{Value: 20.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
	}
	result := Result{meta: *fm, data: stream.Just(records...)}

	// Explicit Avg overrides auto-Sum for delta
	af := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(15*time.Minute, time.UTC)).
		WithBucketReduction(tsquery.ReductionTypeAvg)
	filtered, err := af.Filter(ctx, result)
	require.NoError(t, err)

	collected := filtered.Data().MustCollect()
	require.Len(t, collected, 1)
	require.Equal(t, 15.0, collected[0].Value) // avg(10, 20) = 15
}

func TestAlignerFilter_ExplicitOverridesAutoLast(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("counter", tsquery.DataTypeDecimal, tsquery.MetricKindCumulative, true, "kWh", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)},
		{Value: 200.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
	}
	result := Result{meta: *fm, data: stream.Just(records...)}

	// Explicit Sum overrides auto-Last for cumulative
	af := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(15*time.Minute, time.UTC)).
		WithBucketReduction(tsquery.ReductionTypeSum)
	filtered, err := af.Filter(ctx, result)
	require.NoError(t, err)

	collected := filtered.Data().MustCollect()
	require.Len(t, collected, 1)
	require.Equal(t, 300.0, collected[0].Value) // sum(100, 200) = 300
}

// --- Aligner output samplePeriod ---

func TestAlignerFilter_OutputSamplePeriod_FixedPeriod(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("temp", tsquery.DataTypeDecimal, tsquery.MetricKindGauge, true, "", nil)
	require.NoError(t, err)

	result := Result{meta: *fm, data: simpleStream(1, 2, 3)}

	fifteenMin := 15 * time.Minute
	af := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(fifteenMin, time.UTC))
	filtered, err := af.Filter(ctx, result)
	require.NoError(t, err)
	require.NotNil(t, filtered.Meta().SamplePeriod())
	require.Equal(t, fifteenMin, *filtered.Meta().SamplePeriod())
}

func TestAlignerFilter_OutputSamplePeriod_CalendarPeriod(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("temp", tsquery.DataTypeDecimal, tsquery.MetricKindGauge, true, "", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: 1.0, Timestamp: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)},
		{Value: 2.0, Timestamp: time.Date(2024, 2, 15, 0, 0, 0, 0, time.UTC)},
	}
	result := Result{meta: *fm, data: stream.Just(records...)}

	af := NewAlignerFilter(timeseries.NewMonthAlignmentPeriod(time.UTC))
	filtered, err := af.Filter(ctx, result)
	require.NoError(t, err)
	// Calendar-based alignment cannot express duration as time.Duration
	require.Nil(t, filtered.Meta().SamplePeriod())
}

// --- DeltaFilter validation ---

func TestDeltaFilter_RejectsDeltaInput(t *testing.T) {
	ctx := context.Background()
	meta := deltaDecimalMeta(t)
	result := Result{meta: meta, data: simpleStream(10, 15, 20)}

	df := NewDeltaFilter(false, 0, false, nil)
	_, err := df.Filter(ctx, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already delta")
}

func TestDeltaFilter_RejectsRateInput(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("rate_field", tsquery.DataTypeDecimal, tsquery.MetricKindRate, true, "", nil)
	require.NoError(t, err)
	result := Result{meta: *fm, data: simpleStream(1, 2, 3)}

	df := NewDeltaFilter(false, 0, false, nil)
	_, err = df.Filter(ctx, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "rate")
}

func TestDeltaFilter_RejectsUntaggedInput(t *testing.T) {
	// A metric created with NewFieldMeta (no explicit kind) defaults to gauge
	// and must be rejected by DeltaFilter with a clear error message.
	ctx := context.Background()
	fm, err := tsquery.NewFieldMeta("untagged_counter", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)
	result := Result{meta: *fm, data: simpleStream(100, 110, 125)}

	df := NewDeltaFilter(false, 0, false, nil)
	_, err = df.Filter(ctx, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "gauge")
	require.Contains(t, err.Error(), "set metricKind to cumulative")
}

// --- DeltaFilter implicit gap threshold ---

func TestDeltaFilter_ImplicitGap_DetectsGap(t *testing.T) {
	ctx := context.Background()
	fiveMin := 5 * time.Minute
	meta := cumulativeDecimalMeta(t).WithSamplePeriod(fiveMin)

	// 5m intervals, but 12m gap between second and third sample
	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Value: 110.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
		{Value: 200.0, Timestamp: time.Date(2024, 1, 1, 0, 17, 0, 0, time.UTC)}, // 12m gap > 10m threshold
		{Value: 215.0, Timestamp: time.Date(2024, 1, 1, 0, 22, 0, 0, time.UTC)},
	}
	result := Result{meta: meta, data: stream.Just(records...)}

	df := NewDeltaFilter(false, 0, false, nil) // no explicit maxGap
	filtered, err := df.Filter(ctx, result)
	require.NoError(t, err)

	collected := filtered.Data().MustCollect()
	// First delta: 110-100=10, gap delta skipped, third delta: 215-200=15
	require.Len(t, collected, 2)
	require.Equal(t, 10.0, collected[0].Value)
	require.Equal(t, 15.0, collected[1].Value)
}

func TestDeltaFilter_ImplicitGap_ExactlyTwoXIsNotGap(t *testing.T) {
	ctx := context.Background()
	fiveMin := 5 * time.Minute
	meta := cumulativeDecimalMeta(t).WithSamplePeriod(fiveMin)

	// Gap is exactly 10m (2×5m) — code uses >, so exactly 10m is NOT a gap
	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Value: 110.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
		{Value: 130.0, Timestamp: time.Date(2024, 1, 1, 0, 15, 0, 0, time.UTC)}, // exactly 10m gap
		{Value: 145.0, Timestamp: time.Date(2024, 1, 1, 0, 20, 0, 0, time.UTC)},
	}
	result := Result{meta: meta, data: stream.Just(records...)}

	df := NewDeltaFilter(false, 0, false, nil)
	filtered, err := df.Filter(ctx, result)
	require.NoError(t, err)

	collected := filtered.Data().MustCollect()
	// All deltas computed: 10, 20, 15
	require.Len(t, collected, 3)
	require.Equal(t, 10.0, collected[0].Value)
	require.Equal(t, 20.0, collected[1].Value) // 130-110, NOT skipped
	require.Equal(t, 15.0, collected[2].Value)
}

func TestDeltaFilter_ImplicitGap_JustOverTwoXIsGap(t *testing.T) {
	ctx := context.Background()
	fiveMin := 5 * time.Minute
	meta := cumulativeDecimalMeta(t).WithSamplePeriod(fiveMin)

	// Gap is 10m01s — just over threshold
	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Value: 110.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
		{Value: 200.0, Timestamp: time.Date(2024, 1, 1, 0, 15, 1, 0, time.UTC)}, // 10m01s gap
		{Value: 215.0, Timestamp: time.Date(2024, 1, 1, 0, 20, 1, 0, time.UTC)},
	}
	result := Result{meta: meta, data: stream.Just(records...)}

	df := NewDeltaFilter(false, 0, false, nil)
	filtered, err := df.Filter(ctx, result)
	require.NoError(t, err)

	collected := filtered.Data().MustCollect()
	// Gap detected, delta skipped: 10, then 15
	require.Len(t, collected, 2)
	require.Equal(t, 10.0, collected[0].Value)
	require.Equal(t, 15.0, collected[1].Value)
}

func TestDeltaFilter_ImplicitGap_ExplicitMaxGapWins(t *testing.T) {
	ctx := context.Background()
	fiveMin := 5 * time.Minute
	twentyMin := 20 * time.Minute
	meta := cumulativeDecimalMeta(t).WithSamplePeriod(fiveMin)

	// 12m gap: implicit threshold would be 10m (gap detected), but explicit 20m wins (no gap)
	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Value: 110.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
		{Value: 200.0, Timestamp: time.Date(2024, 1, 1, 0, 17, 0, 0, time.UTC)}, // 12m gap
		{Value: 215.0, Timestamp: time.Date(2024, 1, 1, 0, 22, 0, 0, time.UTC)},
	}
	result := Result{meta: meta, data: stream.Just(records...)}

	df := NewDeltaFilter(false, 0, false, &twentyMin) // explicit 20m overrides 10m implicit
	filtered, err := df.Filter(ctx, result)
	require.NoError(t, err)

	collected := filtered.Data().MustCollect()
	// All deltas computed: 10, 90, 15
	require.Len(t, collected, 3)
	require.Equal(t, 10.0, collected[0].Value)
	require.Equal(t, 90.0, collected[1].Value) // 200-110, NOT skipped
	require.Equal(t, 15.0, collected[2].Value)
}

func TestDeltaFilter_ImplicitGap_NoSamplePeriodNoGapDetection(t *testing.T) {
	ctx := context.Background()
	meta := cumulativeDecimalMeta(t) // no samplePeriod, no maxGap

	// Large gap, but no gap detection
	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Value: 110.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
		{Value: 200.0, Timestamp: time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)}, // 55m gap
		{Value: 215.0, Timestamp: time.Date(2024, 1, 1, 1, 5, 0, 0, time.UTC)},
	}
	result := Result{meta: meta, data: stream.Just(records...)}

	df := NewDeltaFilter(false, 0, false, nil)
	filtered, err := df.Filter(ctx, result)
	require.NoError(t, err)

	collected := filtered.Data().MustCollect()
	// All deltas computed (no gap detection): 10, 90, 15
	require.Len(t, collected, 3)
}

func TestDeltaFilter_ImplicitGap_NonNegativePath(t *testing.T) {
	ctx := context.Background()
	fiveMin := 5 * time.Minute
	meta := cumulativeDecimalMeta(t).WithSamplePeriod(fiveMin)

	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Value: 110.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
		{Value: 200.0, Timestamp: time.Date(2024, 1, 1, 0, 17, 0, 0, time.UTC)}, // 12m gap
		{Value: 215.0, Timestamp: time.Date(2024, 1, 1, 0, 22, 0, 0, time.UTC)},
	}
	result := Result{meta: meta, data: stream.Just(records...)}

	df := NewDeltaFilter(true, 0, false, nil) // nonNegative=true path
	filtered, err := df.Filter(ctx, result)
	require.NoError(t, err)

	collected := filtered.Data().MustCollect()
	// Gap detected in nonNegative path too: 10, (skip), 15
	require.Len(t, collected, 2)
	require.Equal(t, 10.0, collected[0].Value)
	require.Equal(t, 15.0, collected[1].Value)
}

// --- RateFilter validation ---

func TestRateFilter_RejectsRateInput(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("rate_field", tsquery.DataTypeDecimal, tsquery.MetricKindRate, true, "", nil)
	require.NoError(t, err)
	result := Result{meta: *fm, data: simpleStream(1, 2, 3)}

	rf := NewRateFilter("", 1, false, 0, false, nil)
	_, err = rf.Filter(ctx, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already a rate")
}

func TestRateFilter_AcceptsGaugeInput(t *testing.T) {
	ctx := context.Background()
	meta := gaugeDecimalMeta(t)
	result := Result{meta: meta, data: simpleStream(20, 22)}

	rf := NewRateFilter("", 1, false, 0, false, nil)
	filtered, err := rf.Filter(ctx, result)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindRate, filtered.Meta().MetricKind())
}

func TestRateFilter_AcceptsDeltaInput(t *testing.T) {
	ctx := context.Background()
	meta := deltaDecimalMeta(t)
	result := Result{meta: meta, data: simpleStream(10, 15)}

	rf := NewRateFilter("", 1, false, 0, false, nil)
	filtered, err := rf.Filter(ctx, result)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindRate, filtered.Meta().MetricKind())
}

func TestRateFilter_ImplicitGapFromSamplePeriod(t *testing.T) {
	ctx := context.Background()
	fiveMin := 5 * time.Minute
	meta := cumulativeDecimalMeta(t).WithSamplePeriod(fiveMin)

	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Value: 160.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
		{Value: 500.0, Timestamp: time.Date(2024, 1, 1, 0, 17, 0, 0, time.UTC)}, // 12m gap
		{Value: 560.0, Timestamp: time.Date(2024, 1, 1, 0, 22, 0, 0, time.UTC)},
	}
	result := Result{meta: meta, data: stream.Just(records...)}

	rf := NewRateFilter("", 1, false, 0, false, nil) // no explicit maxGap
	filtered, err := rf.Filter(ctx, result)
	require.NoError(t, err)

	collected := filtered.Data().MustCollect()
	// First rate: (160-100)/300s, gap skipped, third rate: (560-500)/300s
	require.Len(t, collected, 2)
}

// --- Selector kind validation ---

func TestSelectorFieldValue_KindMerge_BothDelta(t *testing.T) {
	ctx := context.Background()
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	cond := NewConstantFieldValue(boolMeta, true)
	trueField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindDelta), 5.0)
	falseField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindDelta), 10.0)

	sel := NewSelectorFieldValue(cond, trueField, falseField)
	valueMeta, _, err := sel.Execute(ctx, dummyFieldMeta())
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindDelta, valueMeta.MetricKind)
}

func TestSelectorFieldValue_KindMerge_BothGauge(t *testing.T) {
	ctx := context.Background()
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	cond := NewConstantFieldValue(boolMeta, true)
	trueField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindGauge), 5.0)
	falseField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindGauge), 10.0)

	sel := NewSelectorFieldValue(cond, trueField, falseField)
	valueMeta, _, err := sel.Execute(ctx, dummyFieldMeta())
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindGauge, valueMeta.MetricKind)
}

func TestSelectorFieldValue_KindMerge_DeltaAndUnset(t *testing.T) {
	ctx := context.Background()
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	cond := NewConstantFieldValue(boolMeta, true)
	trueField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindDelta), 5.0)
	// Unset kind (empty string) — like a constant
	unsetMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: true}
	falseField := NewConstantFieldValue(unsetMeta, 0.0)

	sel := NewSelectorFieldValue(cond, trueField, falseField)
	valueMeta, _, err := sel.Execute(ctx, dummyFieldMeta())
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindDelta, valueMeta.MetricKind, "unset is transparent, delta wins")
}

func TestSelectorFieldValue_KindMerge_UnsetAndDelta(t *testing.T) {
	ctx := context.Background()
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	cond := NewConstantFieldValue(boolMeta, true)
	unsetMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: true}
	trueField := NewConstantFieldValue(unsetMeta, 0.0)
	falseField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindDelta), 5.0)

	sel := NewSelectorFieldValue(cond, trueField, falseField)
	valueMeta, _, err := sel.Execute(ctx, dummyFieldMeta())
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindDelta, valueMeta.MetricKind, "symmetric: unset is transparent regardless of position")
}

func TestSelectorFieldValue_KindMerge_BothUnset(t *testing.T) {
	ctx := context.Background()
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	cond := NewConstantFieldValue(boolMeta, true)
	unsetMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: true}
	trueField := NewConstantFieldValue(unsetMeta, 5.0)
	falseField := NewConstantFieldValue(unsetMeta, 10.0)

	sel := NewSelectorFieldValue(cond, trueField, falseField)
	valueMeta, _, err := sel.Execute(ctx, dummyFieldMeta())
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKind(""), valueMeta.MetricKind, "both unset → output unset")
}

func TestSelectorFieldValue_KindMerge_DeltaVsCumulative_Error(t *testing.T) {
	ctx := context.Background()
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	cond := NewConstantFieldValue(boolMeta, true)
	trueField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindDelta), 5.0)
	falseField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindCumulative), 10.0)

	sel := NewSelectorFieldValue(cond, trueField, falseField)
	_, _, err := sel.Execute(ctx, dummyFieldMeta())
	require.Error(t, err)
	require.Contains(t, err.Error(), "matching metric kinds")
}

func TestSelectorFieldValue_KindMerge_RateVsDelta_Error(t *testing.T) {
	ctx := context.Background()
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	cond := NewConstantFieldValue(boolMeta, true)
	trueField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindRate), 5.0)
	falseField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindDelta), 10.0)

	sel := NewSelectorFieldValue(cond, trueField, falseField)
	_, _, err := sel.Execute(ctx, dummyFieldMeta())
	require.Error(t, err)
	require.Contains(t, err.Error(), "matching metric kinds")
}

func TestSelectorFieldValue_KindMerge_CumulativeVsRate_Error(t *testing.T) {
	ctx := context.Background()
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	cond := NewConstantFieldValue(boolMeta, true)
	trueField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindCumulative), 5.0)
	falseField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindRate), 10.0)

	sel := NewSelectorFieldValue(cond, trueField, falseField)
	_, _, err := sel.Execute(ctx, dummyFieldMeta())
	require.Error(t, err)
	require.Contains(t, err.Error(), "matching metric kinds")
}

func TestSelectorFieldValue_KindMerge_DeltaVsExplicitGauge_Error(t *testing.T) {
	ctx := context.Background()
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	cond := NewConstantFieldValue(boolMeta, true)
	trueField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindDelta), 5.0)
	falseField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindGauge), 10.0)

	sel := NewSelectorFieldValue(cond, trueField, falseField)
	_, _, err := sel.Execute(ctx, dummyFieldMeta())
	require.Error(t, err)
	require.Contains(t, err.Error(), "matching metric kinds")
}

// --- Golden integration tests ---

func TestGolden_CumulativeToDeltaToAligner(t *testing.T) {
	ctx := context.Background()

	// Cumulative energy counter reporting every 5 minutes
	fm, err := tsquery.NewFieldMetaFull("lifetimeEnergy", tsquery.DataTypeDecimal, tsquery.MetricKindCumulative, true, "kWh", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Value: 110.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
		{Value: 125.0, Timestamp: time.Date(2024, 1, 1, 0, 10, 0, 0, time.UTC)},
		{Value: 140.0, Timestamp: time.Date(2024, 1, 1, 0, 15, 0, 0, time.UTC)},
		{Value: 155.0, Timestamp: time.Date(2024, 1, 1, 0, 20, 0, 0, time.UTC)},
		{Value: 170.0, Timestamp: time.Date(2024, 1, 1, 0, 25, 0, 0, time.UTC)},
	}
	result := Result{meta: *fm, data: stream.Just(records...)}

	// Step 1: DeltaFilter (cumulative → delta)
	df := NewDeltaFilter(false, 0, false, nil)
	deltaResult, err := df.Filter(ctx, result)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindDelta, deltaResult.Meta().MetricKind())

	// Step 2: AlignerFilter(15m, no explicit reduction) — should auto-select Sum
	af := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(15*time.Minute, time.UTC))
	alignedResult, err := af.Filter(ctx, deltaResult)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindDelta, alignedResult.Meta().MetricKind())

	collected := alignedResult.Data().MustCollect()
	require.Len(t, collected, 2)

	// Deltas: 10, 15, 15, 15, 15
	// Bucket [0:00-0:15): deltas at 0:05(10), 0:10(15) = 25
	// Bucket [0:15-0:30): deltas at 0:15(15), 0:20(15), 0:25(15) = 45
	require.Equal(t, 25.0, collected[0].Value)
	require.Equal(t, 45.0, collected[1].Value)
}

func TestGolden_GaugeThroughAligner_Unchanged(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("temp", tsquery.DataTypeDecimal, tsquery.MetricKindGauge, true, "celsius", nil)
	require.NoError(t, err)

	// Two values in the same bucket — interpolation should still work
	records := []timeseries.TsRecord[any]{
		{Value: 20.0, Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Value: 25.0, Timestamp: time.Date(2024, 1, 1, 0, 10, 0, 0, time.UTC)},
	}
	result := Result{meta: *fm, data: stream.Just(records...)}

	af := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(15*time.Minute, time.UTC))
	filtered, err := af.Filter(ctx, result)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindGauge, filtered.Meta().MetricKind())

	// Gauge uses interpolation path, not Sum
	collected := filtered.Data().MustCollect()
	require.Len(t, collected, 1)
	require.Equal(t, 20.0, collected[0].Value) // first item smudged to bucket start
}

func TestGolden_GaugeToDeltaFilter_Error(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("temp", tsquery.DataTypeDecimal, tsquery.MetricKindGauge, true, "celsius", nil)
	require.NoError(t, err)
	result := Result{meta: *fm, data: simpleStream(20, 22, 19)}

	df := NewDeltaFilter(false, 0, false, nil)
	_, err = df.Filter(ctx, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "gauge")
}

func TestGolden_PipelineWithGap(t *testing.T) {
	ctx := context.Background()
	fiveMin := 5 * time.Minute
	fm, err := tsquery.NewFieldMetaFull("lifetimeEnergy", tsquery.DataTypeDecimal, tsquery.MetricKindCumulative, true, "kWh", nil)
	require.NoError(t, err)
	*fm = fm.WithSamplePeriod(fiveMin)

	// Counter with a 12m gap between 0:05 and 0:17
	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{Value: 110.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
		// gap: 12 minutes > 10m threshold
		{Value: 200.0, Timestamp: time.Date(2024, 1, 1, 0, 17, 0, 0, time.UTC)},
		{Value: 215.0, Timestamp: time.Date(2024, 1, 1, 0, 22, 0, 0, time.UTC)},
		{Value: 230.0, Timestamp: time.Date(2024, 1, 1, 0, 27, 0, 0, time.UTC)},
	}
	result := Result{meta: *fm, data: stream.Just(records...)}

	// DeltaFilter with implicit gap from samplePeriod
	df := NewDeltaFilter(false, 0, false, nil)
	deltaResult, err := df.Filter(ctx, result)
	require.NoError(t, err)

	// Aligner(15m) with auto-Sum
	af := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(15*time.Minute, time.UTC))
	alignedResult, err := af.Filter(ctx, deltaResult)
	require.NoError(t, err)

	collected := alignedResult.Data().MustCollect()
	// Deltas: 10 @ 0:05, (gap skip), 15 @ 0:22, 15 @ 0:27
	// Bucket [0:00-0:15): delta 10 = 10
	// Bucket [0:15-0:30): deltas 15 + 15 = 30
	require.Len(t, collected, 2)
	require.Equal(t, 10.0, collected[0].Value)
	require.Equal(t, 30.0, collected[1].Value)
}

func TestGolden_RateOfRate_Error(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("rate_field", tsquery.DataTypeDecimal, tsquery.MetricKindRate, true, "", nil)
	require.NoError(t, err)
	result := Result{meta: *fm, data: simpleStream(1, 2, 3)}

	rf := NewRateFilter("", 1, false, 0, false, nil)
	_, err = rf.Filter(ctx, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already a rate")
}
