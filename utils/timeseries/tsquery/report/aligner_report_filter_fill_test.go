package report

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- Test Helper Functions for Interpolating Aligner Filter ---

// testInterpolatingReportAlignerFilterAsExpected runs the interpolating AlignerFilter with the given fill mode
// and asserts the output matches the expected records exactly (timestamps and values).
func testInterpolatingReportAlignerFilterAsExpected(
	t *testing.T,
	fixedDuration time.Duration,
	fillMode timeseries.FillMode,
	records []timeseries.TsRecord[[]any],
	expected []timeseries.TsRecord[[]any],
) {
	t.Helper()
	ctx := context.Background()

	// Create field metadata - all fields are decimal (float64) for these tests
	var fieldsMeta []tsquery.FieldMeta
	if len(records) > 0 && len(records[0].Value) > 0 {
		for i := 0; i < len(records[0].Value); i++ {
			fm, err := tsquery.NewFieldMeta(
				fmt.Sprintf("field_%d", i),
				tsquery.DataTypeDecimal,
				false,
			)
			require.NoError(t, err)
			fieldsMeta = append(fieldsMeta, *fm)
		}
	}

	// Create input Result
	inputStream := stream.Just(records...)
	// Create the interpolating aligner filter
	alignerFilter := NewInterpolatingAlignerFilter(timeseries.NewFixedAlignmentPeriod(fixedDuration, time.Local), fillMode)

	// Apply the filter
	outputResult, err := alignerFilter.Filter(ctx, NewResult(fieldsMeta, inputStream))
	require.NoError(t, err)

	// Collect the resulting aligned records
	result := outputResult.Stream().MustCollect()

	// Assert the results
	// Compare timestamps first for easier debugging if lengths differ
	require.EqualValues(t,
		mapSlice(expected, func(r timeseries.TsRecord[[]any]) time.Time { return r.Timestamp }),
		mapSlice(result, func(r timeseries.TsRecord[[]any]) time.Time { return r.Timestamp }),
		"Timestamps mismatch",
	)

	// Compare values with tolerance for floating-point precision
	assertValuesEqualWithTolerance(t,
		mapSlice(expected, func(r timeseries.TsRecord[[]any]) []any { return r.Value }),
		mapSlice(result, func(r timeseries.TsRecord[[]any]) []any { return r.Value }),
		1e-10,
		"Values mismatch",
	)

	// Explicit length check
	require.Len(t, result, len(expected), "Number of resulting records mismatch")
}

// testInterpolatingReportAlignerFilterWithFieldMeta runs the interpolating AlignerFilter with custom field metadata
// and the given fill mode, then asserts the output matches the expected records.
func testInterpolatingReportAlignerFilterWithFieldMeta(
	t *testing.T,
	fixedDuration time.Duration,
	fillMode timeseries.FillMode,
	dataTypes []tsquery.DataType,
	records []timeseries.TsRecord[[]any],
	expected []timeseries.TsRecord[[]any],
) {
	t.Helper()
	ctx := context.Background()

	// Create field metadata based on provided data types
	var fieldsMeta []tsquery.FieldMeta
	for i, dt := range dataTypes {
		fm, err := tsquery.NewFieldMeta(
			fmt.Sprintf("field_%d", i),
			dt,
			false,
		)
		require.NoError(t, err)
		fieldsMeta = append(fieldsMeta, *fm)
	}

	// Create input Result
	inputStream := stream.Just(records...)
	// Create the interpolating aligner filter
	alignerFilter := NewInterpolatingAlignerFilter(timeseries.NewFixedAlignmentPeriod(fixedDuration, time.Local), fillMode)

	// Apply the filter
	outputResult, err := alignerFilter.Filter(ctx, NewResult(fieldsMeta, inputStream))
	require.NoError(t, err)

	// Collect the resulting aligned records
	result := outputResult.Stream().MustCollect()

	// Assert the results
	require.EqualValues(t,
		mapSlice(expected, func(r timeseries.TsRecord[[]any]) time.Time { return r.Timestamp }),
		mapSlice(result, func(r timeseries.TsRecord[[]any]) time.Time { return r.Timestamp }),
		"Timestamps mismatch",
	)

	assertValuesEqualWithTolerance(t,
		mapSlice(expected, func(r timeseries.TsRecord[[]any]) []any { return r.Value }),
		mapSlice(result, func(r timeseries.TsRecord[[]any]) []any { return r.Value }),
		1e-10,
		"Values mismatch",
	)

	require.Len(t, result, len(expected), "Number of resulting records mismatch")
}

// --- Test Cases for Interpolating Aligner Filter with Gap Filling ---

func TestAlignerFilter_FillLinear_BasicGapFilling(t *testing.T) {
	// Two data points 1 hour apart, aligned to 60s intervals with linear fill.
	// Input: t=0 values=[100.0, 10.0], t=3600 values=[200.0, 20.0]
	// Expected: 61 dense points (t=0, t=60, t=120, ..., t=3600)
	// Linear interpolation between the two endpoints.
	ctx := context.Background()

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{100.0, 10.0}, Timestamp: time.Unix(0, 0)},
		{Value: []any{200.0, 20.0}, Timestamp: time.Unix(3600, 0)},
	}

	var fieldsMeta []tsquery.FieldMeta
	for i := 0; i < 2; i++ {
		fm, err := tsquery.NewFieldMeta(fmt.Sprintf("field_%d", i), tsquery.DataTypeDecimal, false)
		require.NoError(t, err)
		fieldsMeta = append(fieldsMeta, *fm)
	}

	inputStream := stream.Just(records...)
	alignerFilter := NewInterpolatingAlignerFilter(
		timeseries.NewFixedAlignmentPeriod(60*time.Second, time.Local),
		timeseries.FillModeLinear,
	)

	outputResult, err := alignerFilter.Filter(ctx, NewResult(fieldsMeta, inputStream))
	require.NoError(t, err)

	result := outputResult.Stream().MustCollect()

	// Should have 61 points: t=0, t=60, t=120, ..., t=3600
	require.Len(t, result, 61)

	// First point: original values at t=0
	require.Equal(t, time.Unix(0, 0), result[0].Timestamp)
	require.InDelta(t, 100.0, result[0].Value[0].(float64), 1e-10)
	require.InDelta(t, 10.0, result[0].Value[1].(float64), 1e-10)

	// Last point: original values at t=3600
	require.Equal(t, time.Unix(3600, 0), result[60].Timestamp)
	require.InDelta(t, 200.0, result[60].Value[0].(float64), 1e-10)
	require.InDelta(t, 20.0, result[60].Value[1].(float64), 1e-10)

	// Middle point at t=1800 (index 30): halfway, should be 150.0, 15.0
	require.Equal(t, time.Unix(1800, 0), result[30].Timestamp)
	require.InDelta(t, 150.0, result[30].Value[0].(float64), 1e-10)
	require.InDelta(t, 15.0, result[30].Value[1].(float64), 1e-10)

	// Spot-check at t=600 (index 10): 10 minutes in, weight = 600/3600 = 1/6
	// field_0: 100 + (200-100) * 1/6 = 116.666...
	// field_1: 10 + (20-10) * 1/6 = 11.666...
	require.Equal(t, time.Unix(600, 0), result[10].Timestamp)
	require.InDelta(t, 100.0+100.0/6.0, result[10].Value[0].(float64), 1e-10)
	require.InDelta(t, 10.0+10.0/6.0, result[10].Value[1].(float64), 1e-10)
}

func TestAlignerFilter_FillLinear_NoGaps(t *testing.T) {
	// Dense data at every minute (4 points t=0,60,120,180), fillMode=linear.
	// No gaps exist, so output should be the same 4 points unchanged.
	testInterpolatingReportAlignerFilterAsExpected(
		t,
		time.Minute,
		timeseries.FillModeLinear,
		[]timeseries.TsRecord[[]any]{
			{Value: []any{100.0, 10.0}, Timestamp: time.Unix(0, 0)},
			{Value: []any{150.0, 20.0}, Timestamp: time.Unix(60, 0)},
			{Value: []any{200.0, 30.0}, Timestamp: time.Unix(120, 0)},
			{Value: []any{250.0, 40.0}, Timestamp: time.Unix(180, 0)},
		},
		[]timeseries.TsRecord[[]any]{
			{Value: []any{100.0, 10.0}, Timestamp: time.Unix(0, 0)},
			{Value: []any{150.0, 20.0}, Timestamp: time.Unix(60, 0)},
			{Value: []any{200.0, 30.0}, Timestamp: time.Unix(120, 0)},
			{Value: []any{250.0, 40.0}, Timestamp: time.Unix(180, 0)},
		},
	)
}

func TestAlignerFilter_FillForwardFill_BasicGapFilling(t *testing.T) {
	// Two data points 1 hour apart, aligned to 60s intervals with forward-fill.
	// The first 60 points should carry the first point's values; the last point has the second point's values.
	ctx := context.Background()

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{100.0, 10.0}, Timestamp: time.Unix(0, 0)},
		{Value: []any{200.0, 20.0}, Timestamp: time.Unix(3600, 0)},
	}

	var fieldsMeta []tsquery.FieldMeta
	for i := 0; i < 2; i++ {
		fm, err := tsquery.NewFieldMeta(fmt.Sprintf("field_%d", i), tsquery.DataTypeDecimal, false)
		require.NoError(t, err)
		fieldsMeta = append(fieldsMeta, *fm)
	}

	inputStream := stream.Just(records...)
	alignerFilter := NewInterpolatingAlignerFilter(
		timeseries.NewFixedAlignmentPeriod(60*time.Second, time.Local),
		timeseries.FillModeForwardFill,
	)

	outputResult, err := alignerFilter.Filter(ctx, NewResult(fieldsMeta, inputStream))
	require.NoError(t, err)

	result := outputResult.Stream().MustCollect()

	// Should have 61 points: t=0, t=60, t=120, ..., t=3600
	require.Len(t, result, 61)

	// First 60 points (index 0 to 59) should all carry the first point's values (forward-fill)
	for i := 0; i < 60; i++ {
		require.Equal(t, time.Unix(int64(i*60), 0), result[i].Timestamp, "Timestamp mismatch at index %d", i)
		require.InDelta(t, 100.0, result[i].Value[0].(float64), 1e-10, "Value[0] mismatch at index %d", i)
		require.InDelta(t, 10.0, result[i].Value[1].(float64), 1e-10, "Value[1] mismatch at index %d", i)
	}

	// Last point (index 60) at t=3600 should have the second point's values
	require.Equal(t, time.Unix(3600, 0), result[60].Timestamp)
	require.InDelta(t, 200.0, result[60].Value[0].(float64), 1e-10)
	require.InDelta(t, 20.0, result[60].Value[1].(float64), 1e-10)
}

func TestAlignerFilter_FillLinear_SingleDataPoint(t *testing.T) {
	// Single data point with fillMode=linear.
	// Should produce exactly 1 point (smeared to alignment boundary).
	testInterpolatingReportAlignerFilterAsExpected(
		t,
		time.Minute,
		timeseries.FillModeLinear,
		[]timeseries.TsRecord[[]any]{
			{Value: []any{100.0, 50.0}, Timestamp: time.Unix(45, 0)}, // 00:00:45
		},
		[]timeseries.TsRecord[[]any]{
			{Value: []any{100.0, 50.0}, Timestamp: time.Unix(0, 0)}, // 00:00:00 (smeared)
		},
	)
}

func TestAlignerFilter_FillLinear_EmptyStream(t *testing.T) {
	// Empty input stream with fillMode=linear should produce empty output.
	testInterpolatingReportAlignerFilterAsExpected(
		t,
		time.Minute,
		timeseries.FillModeLinear,
		[]timeseries.TsRecord[[]any]{},
		[]timeseries.TsRecord[[]any]{},
	)
}

func TestAlignerFilter_FillLinear_IntegerDataType(t *testing.T) {
	// Integer data types with linear fill. Two points 1 hour apart, aligned to 60s.
	// At t=1800 (halfway): field_0 = 100 + (200-100)*0.5 = 150 (int64), field_1 = 10 + (20-10)*0.5 = 15 (int64)
	// At t=600 (1/6): field_0 = 100 + 100*1/6 = 116.666... -> truncated to 116
	//                  field_1 = 10 + 10*1/6 = 11.666... -> truncated to 11
	ctx := context.Background()

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{int64(100), int64(10)}, Timestamp: time.Unix(0, 0)},
		{Value: []any{int64(200), int64(20)}, Timestamp: time.Unix(3600, 0)},
	}

	dataTypes := []tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger}
	var fieldsMeta []tsquery.FieldMeta
	for i, dt := range dataTypes {
		fm, err := tsquery.NewFieldMeta(fmt.Sprintf("field_%d", i), dt, false)
		require.NoError(t, err)
		fieldsMeta = append(fieldsMeta, *fm)
	}

	inputStream := stream.Just(records...)
	alignerFilter := NewInterpolatingAlignerFilter(
		timeseries.NewFixedAlignmentPeriod(60*time.Second, time.Local),
		timeseries.FillModeLinear,
	)

	outputResult, err := alignerFilter.Filter(ctx, NewResult(fieldsMeta, inputStream))
	require.NoError(t, err)

	result := outputResult.Stream().MustCollect()

	// Should have 61 points
	require.Len(t, result, 61)

	// First point
	require.Equal(t, time.Unix(0, 0), result[0].Timestamp)
	require.Equal(t, int64(100), result[0].Value[0])
	require.Equal(t, int64(10), result[0].Value[1])

	// Last point
	require.Equal(t, time.Unix(3600, 0), result[60].Timestamp)
	require.Equal(t, int64(200), result[60].Value[0])
	require.Equal(t, int64(20), result[60].Value[1])

	// Midpoint at t=1800 (index 30): exactly halfway, should be 150 and 15
	require.Equal(t, time.Unix(1800, 0), result[30].Timestamp)
	require.Equal(t, int64(150), result[30].Value[0])
	require.Equal(t, int64(15), result[30].Value[1])

	// At t=600 (index 10): weight = 600/3600 = 1/6
	// field_0: 100 + 100/6 = 116.666... -> truncated to 116
	// field_1: 10 + 10/6 = 11.666... -> truncated to 11
	require.Equal(t, time.Unix(600, 0), result[10].Timestamp)
	require.Equal(t, int64(116), result[10].Value[0])
	require.Equal(t, int64(11), result[10].Value[1])
}

func TestAlignerFilter_DefaultNoFill(t *testing.T) {
	// Verify that NewAlignerFilter (no fillMode) with 2 points 1 hour apart
	// and 60s alignment produces only 2 points (no gap filling).
	testAlignerFilterAsExpected(
		t,
		60*time.Second,
		[]timeseries.TsRecord[[]any]{
			{Value: []any{100.0, 10.0}, Timestamp: time.Unix(0, 0)},
			{Value: []any{200.0, 20.0}, Timestamp: time.Unix(3600, 0)},
		},
		[]timeseries.TsRecord[[]any]{
			{Value: []any{100.0, 10.0}, Timestamp: time.Unix(0, 0)},
			{Value: []any{200.0, 20.0}, Timestamp: time.Unix(3600, 0)},
		},
	)
}
