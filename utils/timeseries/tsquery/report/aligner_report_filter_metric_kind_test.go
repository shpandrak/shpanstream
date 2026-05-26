package report

import (
	"context"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers ---

func kindedField(t *testing.T, urn string, dt tsquery.DataType, kind tsquery.MetricKind) tsquery.FieldMeta {
	t.Helper()
	fm, err := tsquery.NewFieldMetaFull(urn, dt, kind, true, "", nil)
	require.NoError(t, err)
	return *fm
}

func tsRow(unixSec int64, vals ...any) timeseries.TsRecord[[]any] {
	return timeseries.TsRecord[[]any]{Timestamp: time.Unix(unixSec, 0).UTC(), Value: vals}
}

func minuteUTC() timeseries.AlignmentPeriod { return timeseries.NewFixedAlignmentPeriod(time.Minute, time.UTC) }
func hourUTC() timeseries.AlignmentPeriod   { return timeseries.NewFixedAlignmentPeriod(time.Hour, time.UTC) }

func collectAligned(
	t *testing.T,
	af AlignerFilter,
	fieldsMeta []tsquery.FieldMeta,
	records ...timeseries.TsRecord[[]any],
) (Result, []timeseries.TsRecord[[]any]) {
	t.Helper()
	out, err := af.Filter(context.Background(), NewResult(fieldsMeta, stream.Just(records...)))
	require.NoError(t, err)
	return out, out.Stream().MustCollect()
}

func filterErr(
	t *testing.T,
	af AlignerFilter,
	fieldsMeta []tsquery.FieldMeta,
	records ...timeseries.TsRecord[[]any],
) error {
	t.Helper()
	_, err := af.Filter(context.Background(), NewResult(fieldsMeta, stream.Just(records...)))
	return err
}

// --- kind-aware defaults ---

// TestReportAligner_DeltaSumsGaugeSmudges proves the two halves of the fix in one bucket:
// a Delta field sums, while a Gauge field keeps the first-item (smudge) behavior — not an average.
// The gauge varies (0.10..0.40) so an average (0.25) would be visibly different from the first (0.10).
func TestReportAligner_DeltaSumsGaugeSmudges(t *testing.T) {
	fields := []tsquery.FieldMeta{
		kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta),
		kindedField(t, "rate", tsquery.DataTypeDecimal, tsquery.MetricKindGauge),
	}
	_, rows := collectAligned(t, NewAlignerFilter(hourUTC()), fields,
		tsRow(0, 100.0, 0.10),
		tsRow(900, 200.0, 0.20),
		tsRow(1800, 300.0, 0.30),
		tsRow(2700, 400.0, 0.40),
	)

	require.Len(t, rows, 1)
	assert.Equal(t, time.Unix(0, 0).UTC(), rows[0].Timestamp)
	assert.InDelta(t, 1000.0, rows[0].Value[0], 1e-9, "energy: Delta → Sum")
	assert.InDelta(t, 0.10, rows[0].Value[1], 1e-9, "rate: Gauge → smudge first, NOT avg (0.25)")
}

func TestReportAligner_DeltaSumsAcrossMultipleBuckets(t *testing.T) {
	fields := []tsquery.FieldMeta{
		kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta),
		kindedField(t, "rate", tsquery.DataTypeDecimal, tsquery.MetricKindGauge),
	}
	_, rows := collectAligned(t, NewAlignerFilter(hourUTC()), fields,
		tsRow(0, 100.0, 0.10), tsRow(900, 200.0, 0.10), tsRow(1800, 300.0, 0.10), tsRow(2700, 400.0, 0.10),
		tsRow(3600, 500.0, 0.10), tsRow(4500, 600.0, 0.10), tsRow(5400, 700.0, 0.10), tsRow(6300, 800.0, 0.10),
	)

	require.Len(t, rows, 2)
	assert.Equal(t, time.Unix(0, 0).UTC(), rows[0].Timestamp)
	assert.InDelta(t, 1000.0, rows[0].Value[0], 1e-9)
	assert.Equal(t, time.Unix(3600, 0).UTC(), rows[1].Timestamp)
	assert.InDelta(t, 2600.0, rows[1].Value[0], 1e-9, "second bucket sum")
	assert.InDelta(t, 0.10, rows[1].Value[1], 1e-9, "gauge stays 0.10")
}

// TestReportAligner_DeltaSumsWhenAlignedToBoundary is the join-at-bucket-start scenario from the bug
// report: even when a bucket's first item lands exactly on the bucket boundary, a Delta field must
// SUM all items in the bucket rather than return the first.
func TestReportAligner_DeltaSumsWhenAlignedToBoundary(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta)}
	// Bucket [60,120): first item at exactly 60s (on the boundary), plus a second item.
	_, rows := collectAligned(t, NewAlignerFilter(minuteUTC()), fields,
		tsRow(0, 5.0),
		tsRow(60, 10.0), tsRow(90, 20.0),
	)

	require.Len(t, rows, 2)
	assert.InDelta(t, 5.0, rows[0].Value[0], 1e-9)
	assert.Equal(t, time.Unix(60, 0).UTC(), rows[1].Timestamp)
	assert.InDelta(t, 30.0, rows[1].Value[0], 1e-9, "boundary-aligned bucket must still sum 10+20")
}

func TestReportAligner_DeltaIntegerSumPreservesType(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "events", tsquery.DataTypeInteger, tsquery.MetricKindDelta)}
	out, rows := collectAligned(t, NewAlignerFilter(hourUTC()), fields,
		tsRow(0, int64(1)), tsRow(900, int64(2)), tsRow(1800, int64(3)),
	)

	require.Len(t, rows, 1)
	assert.Equal(t, int64(6), rows[0].Value[0], "Sum of integers stays integer")
	assert.Equal(t, tsquery.DataTypeInteger, out.FieldsMeta()[0].DataType())
}

// TestReportAligner_OutputPreservesMetricKind verifies alignment changes granularity, not semantics:
// a summed Delta field is still Delta on the output.
func TestReportAligner_OutputPreservesMetricKind(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta)}
	out, _ := collectAligned(t, NewAlignerFilter(hourUTC()), fields, tsRow(0, 1.0), tsRow(900, 2.0))
	assert.Equal(t, tsquery.MetricKindDelta, out.FieldsMeta()[0].MetricKind())
}

// --- regression guards: non-Delta kinds must NOT be reduced ---

// TestReportAligner_CumulativeInterpolatesNotLast guards against re-introducing the "Last" cumulative
// reduction that was deliberately removed from the datasource aligner: cumulative falls through to
// interpolate-to-bucket-start.
func TestReportAligner_CumulativeInterpolatesNotLast(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "counter", tsquery.DataTypeDecimal, tsquery.MetricKindCumulative)}
	_, rows := collectAligned(t, NewAlignerFilter(minuteUTC()), fields,
		tsRow(45, 100.0),
		tsRow(105, 200.0),
	)
	require.Len(t, rows, 2)
	assert.InDelta(t, 100.0, rows[0].Value[0], 1e-9, "first bucket smudges first")
	// Interpolated at t=60 between (45,100) and (105,200): 100 + 100*(15/60) = 125. "Last" would give 200.
	assert.InDelta(t, 125.0, rows[1].Value[0], 1e-9, "cumulative interpolates to bucket-start, not Last")
}

func TestReportAligner_GaugeAndRateInterpolate_NotAvg(t *testing.T) {
	for _, kind := range []tsquery.MetricKind{tsquery.MetricKindGauge, tsquery.MetricKindRate} {
		t.Run(string(kind), func(t *testing.T) {
			fields := []tsquery.FieldMeta{kindedField(t, "v", tsquery.DataTypeDecimal, kind)}
			_, rows := collectAligned(t, NewAlignerFilter(minuteUTC()), fields,
				tsRow(45, 100.0),
				tsRow(105, 200.0),
			)
			require.Len(t, rows, 2)
			assert.InDelta(t, 100.0, rows[0].Value[0], 1e-9)
			assert.InDelta(t, 125.0, rows[1].Value[0], 1e-9, "interpolation, not bucket average")
		})
	}
}

// TestReportAligner_UnsetKindUnchanged confirms backward compatibility: a field with no MetricKind
// (built via NewFieldMeta) keeps the original interpolation behavior.
func TestReportAligner_UnsetKindUnchanged(t *testing.T) {
	fm, err := tsquery.NewFieldMeta("v", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)
	_, rows := collectAligned(t, NewAlignerFilter(minuteUTC()), []tsquery.FieldMeta{*fm},
		tsRow(45, 100.0),
		tsRow(105, 200.0),
	)
	require.Len(t, rows, 2)
	assert.InDelta(t, 100.0, rows[0].Value[0], 1e-9)
	assert.InDelta(t, 125.0, rows[1].Value[0], 1e-9)
}

// --- mixed kinds in a single report, single streaming pass ---

func TestReportAligner_MixedKinds(t *testing.T) {
	fields := []tsquery.FieldMeta{
		kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta),
		kindedField(t, "temp", tsquery.DataTypeDecimal, tsquery.MetricKindGauge),
		kindedField(t, "counter", tsquery.DataTypeDecimal, tsquery.MetricKindCumulative),
	}
	_, rows := collectAligned(t, NewAlignerFilter(minuteUTC()), fields,
		tsRow(45, 10.0, 100.0, 1000.0),
		tsRow(50, 20.0, 110.0, 1010.0),
		tsRow(105, 30.0, 200.0, 2000.0),
	)

	require.Len(t, rows, 2)
	// Bucket [0,60): delta sums (10+20=30); gauge & cumulative smudge first (t=45).
	assert.InDelta(t, 30.0, rows[0].Value[0], 1e-9)
	assert.InDelta(t, 100.0, rows[0].Value[1], 1e-9)
	assert.InDelta(t, 1000.0, rows[0].Value[2], 1e-9)
	// Bucket [60,120): delta sums (30); gauge & cumulative interpolate across the boundary using the
	// previous cluster's last item (t=50) and this cluster's first (t=105). weight = (60-50)/(105-50).
	w := 10.0 / 55.0
	assert.InDelta(t, 30.0, rows[1].Value[0], 1e-9)
	assert.InDelta(t, 110.0+(200.0-110.0)*w, rows[1].Value[1], 1e-9)
	assert.InDelta(t, 1010.0+(2000.0-1010.0)*w, rows[1].Value[2], 1e-9)
}

// --- per-field reduction override ---

func TestReportAligner_FieldReductionOverridesKindDefault(t *testing.T) {
	fields := []tsquery.FieldMeta{
		kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta), // would default to Sum
		kindedField(t, "rate", tsquery.DataTypeDecimal, tsquery.MetricKindGauge),   // would interpolate
	}
	af := NewAlignerFilter(hourUTC()).
		WithFieldReduction("energy", tsquery.ReductionTypeMax). // override Delta's Sum default
		WithFieldReduction("rate", tsquery.ReductionTypeSum)    // force-sum a gauge
	_, rows := collectAligned(t, af, fields,
		tsRow(0, 100.0, 1.0), tsRow(900, 400.0, 2.0), tsRow(1800, 200.0, 3.0),
	)

	require.Len(t, rows, 1)
	assert.InDelta(t, 400.0, rows[0].Value[0], 1e-9, "energy: Max override (not Sum=700)")
	assert.InDelta(t, 6.0, rows[0].Value[1], 1e-9, "rate: Sum override (not interpolated)")
}

func TestReportAligner_OverrideChangesOutputDataType(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "events", tsquery.DataTypeInteger, tsquery.MetricKindGauge)}
	af := NewAlignerFilter(hourUTC()).WithFieldReduction("events", tsquery.ReductionTypeAvg)
	out, rows := collectAligned(t, af, fields,
		tsRow(0, int64(10)), tsRow(900, int64(20)), tsRow(1800, int64(30)),
	)

	require.Len(t, rows, 1)
	assert.Equal(t, tsquery.DataTypeDecimal, out.FieldsMeta()[0].DataType(), "Avg promotes integer → decimal")
	assert.InDelta(t, 20.0, rows[0].Value[0], 1e-9)
}

// --- validations ---

func TestReportAligner_UnknownOverrideUrnErrors(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta)}
	af := NewAlignerFilter(hourUTC()).WithFieldReduction("does-not-exist", tsquery.ReductionTypeSum)
	err := filterErr(t, af, fields, tsRow(0, 1.0))
	require.ErrorContains(t, err, "unknown field urn")
}

func TestReportAligner_PairedReductionRejected(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta)}
	af := NewAlignerFilter(hourUTC()).WithFieldReduction("energy", tsquery.ReductionTypeRMSE)
	err := filterErr(t, af, fields, tsRow(0, 1.0))
	require.ErrorContains(t, err, "paired reduction")
}

func TestReportAligner_InvalidReductionRejected(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta)}
	af := NewAlignerFilter(hourUTC()).WithFieldReduction("energy", tsquery.ReductionType("bogus"))
	err := filterErr(t, af, fields, tsRow(0, 1.0))
	require.ErrorContains(t, err, "invalid reduction type")
}

func TestReportAligner_PerFieldFillRequiresGlobalFill(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "v", tsquery.DataTypeDecimal, tsquery.MetricKindGauge)}
	// No global fill enabled (plain NewAlignerFilter) → per-field fill override is an error.
	af := NewAlignerFilter(minuteUTC()).WithFieldFillMode("v", timeseries.FillModeForwardFill)
	err := filterErr(t, af, fields, tsRow(0, 1.0))
	require.ErrorContains(t, err, "requires fill to be enabled")
}

func TestReportAligner_InvalidPerFieldFillModeRejected(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "v", tsquery.DataTypeDecimal, tsquery.MetricKindGauge)}
	af := NewInterpolatingAlignerFilter(minuteUTC(), timeseries.FillModeLinear).
		WithFieldFillMode("v", timeseries.FillMode("nope"))
	err := filterErr(t, af, fields, tsRow(0, 1.0))
	require.ErrorContains(t, err, "invalid fill mode")
}

// --- per-field gap fill ---

// TestReportAligner_PerFieldForwardFill: global fill is linear, but one field forward-fills. The gap
// at t=60 must interpolate the linear field while carrying the previous value for the forward-fill one.
func TestReportAligner_PerFieldForwardFill(t *testing.T) {
	fields := []tsquery.FieldMeta{
		kindedField(t, "lin", tsquery.DataTypeDecimal, tsquery.MetricKindGauge),
		kindedField(t, "ff", tsquery.DataTypeDecimal, tsquery.MetricKindGauge),
	}
	af := NewInterpolatingAlignerFilter(minuteUTC(), timeseries.FillModeLinear).
		WithFieldFillMode("ff", timeseries.FillModeForwardFill)
	_, rows := collectAligned(t, af, fields,
		tsRow(0, 10.0, 100.0),
		tsRow(120, 30.0, 300.0),
	)

	require.Len(t, rows, 3)
	assert.Equal(t, time.Unix(60, 0).UTC(), rows[1].Timestamp)
	assert.InDelta(t, 20.0, rows[1].Value[0], 1e-9, "lin: linear interpolation 10→30 at midpoint")
	assert.InDelta(t, 100.0, rows[1].Value[1], 1e-9, "ff: forward-fill carries previous 100")
	// endpoints unchanged
	assert.InDelta(t, 10.0, rows[0].Value[0], 1e-9)
	assert.InDelta(t, 30.0, rows[2].Value[0], 1e-9)
}

// TestReportAligner_DeltaSumThenForwardFill is the realistic case: a summed Delta field should
// forward-fill missing buckets (carry the bucket total) rather than interpolate summed deltas.
func TestReportAligner_DeltaSumThenForwardFill(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta)}
	af := NewInterpolatingAlignerFilter(minuteUTC(), timeseries.FillModeLinear).
		WithFieldFillMode("energy", timeseries.FillModeForwardFill)
	_, rows := collectAligned(t, af, fields, tsRow(0, 10.0), tsRow(120, 30.0))

	require.Len(t, rows, 3)
	assert.InDelta(t, 10.0, rows[0].Value[0], 1e-9)
	assert.InDelta(t, 10.0, rows[1].Value[0], 1e-9, "gap forward-fills the previous bucket sum")
	assert.InDelta(t, 30.0, rows[2].Value[0], 1e-9)
}

// TestReportAligner_DeltaSumGlobalLinearFill exercises the reduction + global-fill path with NO
// per-field override (the unchanged single-mode gap-filler), confirming summed buckets are linearly
// gap-filled when no override is present.
func TestReportAligner_DeltaSumGlobalLinearFill(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta)}
	af := NewInterpolatingAlignerFilter(minuteUTC(), timeseries.FillModeLinear)
	_, rows := collectAligned(t, af, fields, tsRow(0, 10.0), tsRow(120, 30.0))

	require.Len(t, rows, 3)
	assert.InDelta(t, 10.0, rows[0].Value[0], 1e-9)
	assert.InDelta(t, 20.0, rows[1].Value[0], 1e-9, "linear fill of the gap between summed buckets")
	assert.InDelta(t, 30.0, rows[2].Value[0], 1e-9)
}

// TestReportAligner_GlobalForwardFillPerFieldLinear is the mirror of the previous test: the global
// default is forward-fill and one field overrides to linear. Verifies the per-field dispatch is
// symmetric (it emulates both modes regardless of which is the default).
func TestReportAligner_GlobalForwardFillPerFieldLinear(t *testing.T) {
	fields := []tsquery.FieldMeta{
		kindedField(t, "lin", tsquery.DataTypeDecimal, tsquery.MetricKindGauge),
		kindedField(t, "ff", tsquery.DataTypeDecimal, tsquery.MetricKindGauge),
	}
	af := NewInterpolatingAlignerFilter(minuteUTC(), timeseries.FillModeForwardFill).
		WithFieldFillMode("lin", timeseries.FillModeLinear)
	_, rows := collectAligned(t, af, fields,
		tsRow(0, 10.0, 100.0),
		tsRow(120, 30.0, 300.0),
	)

	require.Len(t, rows, 3)
	assert.InDelta(t, 20.0, rows[1].Value[0], 1e-9, "lin: override interpolates")
	assert.InDelta(t, 100.0, rows[1].Value[1], 1e-9, "ff: global forward-fill carries previous")
}

// TestReportAligner_UnknownFillOverrideUrnErrors checks the unknown-URN guard also fires for a
// fill-mode override (not just reduction overrides).
func TestReportAligner_UnknownFillOverrideUrnErrors(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "v", tsquery.DataTypeDecimal, tsquery.MetricKindGauge)}
	af := NewInterpolatingAlignerFilter(minuteUTC(), timeseries.FillModeLinear).
		WithFieldFillMode("does-not-exist", timeseries.FillModeForwardFill)
	err := filterErr(t, af, fields, tsRow(0, 1.0))
	require.ErrorContains(t, err, "unknown field urn")
}

// TestReportAligner_ReductionAndFillOverridesCombined exercises reduction overrides and fill
// overrides on different fields of one filter, with a gap to synthesize: a summed Delta field
// forward-fills, while a Max-reduced gauge linearly fills (over its reduced bucket values).
func TestReportAligner_ReductionAndFillOverridesCombined(t *testing.T) {
	fields := []tsquery.FieldMeta{
		kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta), // default Sum
		kindedField(t, "temp", tsquery.DataTypeDecimal, tsquery.MetricKindGauge),
	}
	af := NewInterpolatingAlignerFilter(minuteUTC(), timeseries.FillModeLinear).
		WithFieldReduction("temp", tsquery.ReductionTypeMax).  // reduce gauge by Max instead of interpolating
		WithFieldFillMode("energy", timeseries.FillModeForwardFill) // forward-fill the summed delta
	_, rows := collectAligned(t, af, fields,
		tsRow(0, 10.0, 1.0), tsRow(30, 20.0, 5.0), // bucket [0,60): energy sum=30, temp max=5
		tsRow(120, 100.0, 9.0), // bucket [120,180): energy=100, temp max=9
	)

	require.Len(t, rows, 3)
	// t=0
	assert.InDelta(t, 30.0, rows[0].Value[0], 1e-9)
	assert.InDelta(t, 5.0, rows[0].Value[1], 1e-9)
	// t=60 (synthesized): energy forward-fills 30; temp linearly fills 5→9 = 7
	assert.Equal(t, time.Unix(60, 0).UTC(), rows[1].Timestamp)
	assert.InDelta(t, 30.0, rows[1].Value[0], 1e-9)
	assert.InDelta(t, 7.0, rows[1].Value[1], 1e-9)
	// t=120
	assert.InDelta(t, 100.0, rows[2].Value[0], 1e-9)
	assert.InDelta(t, 9.0, rows[2].Value[1], 1e-9)
}

// --- edges ---

func TestReportAligner_EmptyStreamWithDelta(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta)}
	_, rows := collectAligned(t, NewAlignerFilter(hourUTC()), fields)
	assert.Empty(t, rows)
}

func TestReportAligner_SingleDeltaItem(t *testing.T) {
	fields := []tsquery.FieldMeta{kindedField(t, "energy", tsquery.DataTypeDecimal, tsquery.MetricKindDelta)}
	_, rows := collectAligned(t, NewAlignerFilter(hourUTC()), fields, tsRow(900, 42.0))
	require.Len(t, rows, 1)
	assert.Equal(t, time.Unix(0, 0).UTC(), rows[0].Timestamp)
	assert.InDelta(t, 42.0, rows[0].Value[0], 1e-9, "single item sums to itself")
}
