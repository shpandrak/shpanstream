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

func cumulativeDecimalMeta(t *testing.T) tsquery.FieldMeta {
	t.Helper()
	fm, err := tsquery.NewFieldMetaFull("counter", tsquery.DataTypeDecimal, tsquery.MetricKindCumulative, true, "kWh", nil)
	require.NoError(t, err)
	return *fm
}

func deltaDecimalMeta(t *testing.T) tsquery.FieldMeta {
	t.Helper()
	fm, err := tsquery.NewFieldMetaFull("delta_field", tsquery.DataTypeDecimal, tsquery.MetricKindDelta, true, "kWh", nil)
	require.NoError(t, err)
	return *fm
}

func gaugeDecimalMeta(t *testing.T) tsquery.FieldMeta {
	t.Helper()
	fm, err := tsquery.NewFieldMetaFull("temp", tsquery.DataTypeDecimal, tsquery.MetricKindGauge, true, "celsius", nil)
	require.NoError(t, err)
	return *fm
}

func simpleStream(values ...float64) stream.Stream[timeseries.TsRecord[any]] {
	records := make([]timeseries.TsRecord[any], len(values))
	for i, v := range values {
		records[i] = timeseries.TsRecord[any]{
			Value:     v,
			Timestamp: time.Unix(int64(i*60), 0),
		}
	}
	return stream.Just(records...)
}

func TestDeltaFilter_OutputKindIsDelta(t *testing.T) {
	ctx := context.Background()
	meta := cumulativeDecimalMeta(t)

	result := Result{meta: meta, data: simpleStream(100, 110, 125)}

	df := NewDeltaFilter(false, 0, false, nil)
	filtered, err := df.Filter(ctx, result)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindDelta, filtered.Meta().MetricKind())

	// Verify other metadata preserved
	require.Equal(t, "counter", filtered.Meta().Urn())
	require.Equal(t, tsquery.DataTypeDecimal, filtered.Meta().DataType())
	require.Equal(t, "kWh", filtered.Meta().Unit())
}

func TestDeltaFilter_OutputKindIsDelta_FromGauge(t *testing.T) {
	// In Phase 1, DeltaFilter accepts any kind (no validation yet)
	ctx := context.Background()
	meta := gaugeDecimalMeta(t)

	result := Result{meta: meta, data: simpleStream(20, 22, 19)}

	df := NewDeltaFilter(false, 0, false, nil)
	filtered, err := df.Filter(ctx, result)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindDelta, filtered.Meta().MetricKind())
}

func TestRateFilter_OutputKindIsRate(t *testing.T) {
	ctx := context.Background()
	meta := cumulativeDecimalMeta(t)

	result := Result{meta: meta, data: simpleStream(100, 160)}

	rf := NewRateFilter("kW", 1, false, 0, false, nil)
	filtered, err := rf.Filter(ctx, result)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindRate, filtered.Meta().MetricKind())
	require.Equal(t, "kW", filtered.Meta().Unit())
	require.Equal(t, tsquery.DataTypeDecimal, filtered.Meta().DataType())
}

func TestAlignerFilter_PreservesKind(t *testing.T) {
	ctx := context.Background()

	for _, kind := range []tsquery.MetricKind{tsquery.MetricKindGauge, tsquery.MetricKindDelta, tsquery.MetricKindCumulative, tsquery.MetricKindRate} {
		t.Run(string(kind), func(t *testing.T) {
			fm, err := tsquery.NewFieldMetaFull("field", tsquery.DataTypeDecimal, kind, true, "", nil)
			require.NoError(t, err)

			result := Result{meta: *fm, data: simpleStream(1, 2, 3)}

			af := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(10*time.Minute, time.UTC))
			filtered, err := af.Filter(ctx, result)
			require.NoError(t, err)
			require.Equal(t, kind, filtered.Meta().MetricKind())
		})
	}
}

func TestAlignerFilter_PreservesKind_WithBucketReduction(t *testing.T) {
	ctx := context.Background()
	fm, err := tsquery.NewFieldMetaFull("energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta, true, "kWh", nil)
	require.NoError(t, err)

	// Data with timestamps within a 10-minute bucket
	records := []timeseries.TsRecord[any]{
		{Value: 10.0, Timestamp: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)},
		{Value: 15.0, Timestamp: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
	}
	result := Result{meta: *fm, data: stream.Just(records...)}

	af := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(10*time.Minute, time.UTC)).
		WithBucketReduction(tsquery.ReductionTypeSum)

	filtered, err := af.Filter(ctx, result)
	require.NoError(t, err)

	// Kind preserved even with explicit bucket reduction
	require.Equal(t, tsquery.MetricKindDelta, filtered.Meta().MetricKind())
}

func TestOverrideFieldMetadataFilter_OverrideKind(t *testing.T) {
	ctx := context.Background()
	meta := gaugeDecimalMeta(t)
	result := Result{meta: meta, data: simpleStream(1, 2, 3)}

	cumKind := tsquery.MetricKindCumulative
	filter := NewOverrideFieldMetadataFilter(nil, nil, &cumKind, nil)
	filtered, err := filter.Filter(ctx, result)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindCumulative, filtered.Meta().MetricKind())

	// Other metadata preserved
	require.Equal(t, "temp", filtered.Meta().Urn())
	require.Equal(t, "celsius", filtered.Meta().Unit())
}

func TestOverrideFieldMetadataFilter_PreservesKind_WhenNil(t *testing.T) {
	ctx := context.Background()
	meta := cumulativeDecimalMeta(t)
	result := Result{meta: meta, data: simpleStream(100, 200)}

	// Override URN but not kind
	newUrn := "renamed"
	filter := NewOverrideFieldMetadataFilter(&newUrn, nil, nil, nil)
	filtered, err := filter.Filter(ctx, result)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindCumulative, filtered.Meta().MetricKind())
	require.Equal(t, "renamed", filtered.Meta().Urn())
}

func TestKindPropagation_CumulativeThroughDelta(t *testing.T) {
	ctx := context.Background()
	meta := cumulativeDecimalMeta(t)

	// Simulate cumulative counter: 100, 110, 125, 140
	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Unix(0, 0)},
		{Value: 110.0, Timestamp: time.Unix(60, 0)},
		{Value: 125.0, Timestamp: time.Unix(120, 0)},
		{Value: 140.0, Timestamp: time.Unix(180, 0)},
	}
	result := Result{meta: meta, data: stream.Just(records...)}

	// Apply DeltaFilter
	df := NewDeltaFilter(false, 0, false, nil)
	filtered, err := df.Filter(ctx, result)
	require.NoError(t, err)

	// Kind should be Delta after DeltaFilter
	require.Equal(t, tsquery.MetricKindDelta, filtered.Meta().MetricKind())

	// Values should be deltas: 10, 15, 15
	collected := filtered.Data().MustCollect()
	require.Len(t, collected, 3)
	require.Equal(t, 10.0, collected[0].Value)
	require.Equal(t, 15.0, collected[1].Value)
	require.Equal(t, 15.0, collected[2].Value)
}

// --- Field value propagation tests ---
// These tests verify that computed field values (Cast, Nvl, Unary, NumericExpression,
// Selector) correctly propagate MetricKind from their source operands via ValueMeta.
// RefFieldValue is covered indirectly through existing filter tests (it extracts from
// the FieldMeta passed to Execute).

// valueMetaWithKind is a helper that builds a ValueMeta with an explicit MetricKind.
func valueMetaWithKind(dt tsquery.DataType, kind tsquery.MetricKind) tsquery.ValueMeta {
	return tsquery.ValueMeta{DataType: dt, MetricKind: kind, Required: true}
}

// dummyFieldMeta is required by Value.Execute signature but not used by the field values
// under test (which operate on nested Value operands, not the outer FieldMeta).
func dummyFieldMeta() tsquery.FieldMeta {
	return tsquery.FieldMeta{}
}

func TestRefFieldValue_ExtractsMetricKind(t *testing.T) {
	ctx := context.Background()

	sourceMeta, err := tsquery.NewFieldMetaFull("energy", tsquery.DataTypeDecimal, tsquery.MetricKindCumulative, true, "", nil)
	require.NoError(t, err)

	ref := NewRefFieldValue()
	valueMeta, _, err := ref.Execute(ctx, *sourceMeta)
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindCumulative, valueMeta.MetricKind)
}

func TestCastFieldValue_PropagatesKind(t *testing.T) {
	ctx := context.Background()

	// Cumulative integer source, cast to decimal — should preserve Cumulative
	source := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeInteger, tsquery.MetricKindCumulative), int64(100))
	cast := NewCastFieldValue(source, tsquery.DataTypeDecimal)

	valueMeta, _, err := cast.Execute(ctx, dummyFieldMeta())
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, valueMeta.DataType)
	require.Equal(t, tsquery.MetricKindCumulative, valueMeta.MetricKind)
}

func TestNvlFieldValue_PropagatesKind(t *testing.T) {
	ctx := context.Background()

	// Source is optional delta, alt is a required delta constant — propagate from source
	sourceMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, MetricKind: tsquery.MetricKindDelta, Required: false}
	source := NewConstantFieldValue(sourceMeta, 5.0)
	alt := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindDelta), 0.0)

	nvl := NewNvlFieldValue(source, alt)
	valueMeta, _, err := nvl.Execute(ctx, dummyFieldMeta())
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindDelta, valueMeta.MetricKind)
}

func TestUnaryNumericOperator_PropagatesKind(t *testing.T) {
	ctx := context.Background()

	// -cumulative → should still report Cumulative
	source := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindCumulative), 42.0)
	neg := NewUnaryNumericOperatorFieldValue(source, tsquery.UnaryNumericOperatorNegate)

	valueMeta, _, err := neg.Execute(ctx, dummyFieldMeta())
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindCumulative, valueMeta.MetricKind)
}

// TestNumericExpression_PropagatesKindFromOp1 documents the Phase 1 rule:
// numeric expressions propagate MetricKind from op1 (same pattern as CustomMeta).
// This gives the right default for common cases like `delta * price_per_unit`.
func TestNumericExpression_PropagatesKindFromOp1(t *testing.T) {
	ctx := context.Background()

	// delta * gauge constant → should propagate Delta from op1
	op1 := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindDelta), 10.0)
	op2 := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindGauge), 0.15)

	expr := NewNumericExpressionFieldValue(op1, tsquery.BinaryNumericOperatorMul, op2)
	valueMeta, _, err := expr.Execute(ctx, dummyFieldMeta())
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindDelta, valueMeta.MetricKind, "numeric expression should propagate kind from op1")
}

// TestNumericExpression_OpsDontChangeKindRule verifies that the op1 rule holds
// regardless of whether op2's kind matches.
func TestNumericExpression_OpsDontChangeKindRule(t *testing.T) {
	ctx := context.Background()

	// cumulative * cumulative — should still propagate from op1
	op1 := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindCumulative), 100.0)
	op2 := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindCumulative), 2.0)

	expr := NewNumericExpressionFieldValue(op1, tsquery.BinaryNumericOperatorAdd, op2)
	valueMeta, _, err := expr.Execute(ctx, dummyFieldMeta())
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindCumulative, valueMeta.MetricKind)
}

// TestSelectorFieldValue_PropagatesKindFromTrueBranch documents the Phase 1 rule:
// selector propagates MetricKind from the true branch.
// Phase 2 will add strict validation that both branches must match.
func TestSelectorFieldValue_PropagatesKindFromTrueBranch(t *testing.T) {
	ctx := context.Background()

	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	cond := NewConstantFieldValue(boolMeta, true)
	trueField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindDelta), 5.0)
	falseField := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeDecimal, tsquery.MetricKindDelta), 10.0)

	sel := NewSelectorFieldValue(cond, trueField, falseField)
	valueMeta, _, err := sel.Execute(ctx, dummyFieldMeta())
	require.NoError(t, err)
	require.Equal(t, tsquery.MetricKindDelta, valueMeta.MetricKind, "selector should propagate kind from true branch")
}

// TestValueMetaPropagation_NestedExpressions verifies that kind flows through
// a nested chain: Cast(Unary(Cumulative)) → Cumulative at the top.
func TestValueMetaPropagation_NestedExpressions(t *testing.T) {
	ctx := context.Background()

	// cumulative int → neg → cast to decimal
	source := NewConstantFieldValue(valueMetaWithKind(tsquery.DataTypeInteger, tsquery.MetricKindCumulative), int64(100))
	neg := NewUnaryNumericOperatorFieldValue(source, tsquery.UnaryNumericOperatorNegate)
	cast := NewCastFieldValue(neg, tsquery.DataTypeDecimal)

	valueMeta, _, err := cast.Execute(ctx, dummyFieldMeta())
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, valueMeta.DataType)
	require.Equal(t, tsquery.MetricKindCumulative, valueMeta.MetricKind)
}
