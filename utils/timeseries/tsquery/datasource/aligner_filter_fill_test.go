package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- Interpolating Aligner Filter Test Helpers ---

// testInterpolatingAlignerFilterAsExpected runs the AlignerFilter with fill mode and asserts the output.
func testInterpolatingAlignerFilterAsExpected(
	t *testing.T,
	fixedDuration time.Duration,
	fillMode timeseries.FillMode,
	records []timeseries.TsRecord[any],
	expected []timeseries.TsRecord[any],
) {
	t.Helper()

	fieldMeta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	ctx := context.Background()
	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10000, 0))
	require.NoError(t, err)

	alignerFilter := NewInterpolatingAlignerFilter(timeseries.NewFixedAlignmentPeriod(fixedDuration, time.Local), fillMode)

	outputResult, err := alignerFilter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := outputResult.Data().Collect(ctx)
	require.NoError(t, err)

	require.EqualValues(t,
		mapSlice(expected, func(r timeseries.TsRecord[any]) time.Time { return r.Timestamp }),
		mapSlice(resultData, func(r timeseries.TsRecord[any]) time.Time { return r.Timestamp }),
		"Timestamps mismatch",
	)

	assertValuesEqualWithTolerance(t,
		mapSlice(expected, func(r timeseries.TsRecord[any]) any { return r.Value }),
		mapSlice(resultData, func(r timeseries.TsRecord[any]) any { return r.Value }),
		1e-10,
		"Values mismatch",
	)

	require.Len(t, resultData, len(expected), "Number of resulting records mismatch")
}

// testInterpolatingAlignerFilterWithFieldMeta runs the AlignerFilter with fill mode and custom field metadata.
func testInterpolatingAlignerFilterWithFieldMeta(
	t *testing.T,
	fixedDuration time.Duration,
	fillMode timeseries.FillMode,
	dataType tsquery.DataType,
	records []timeseries.TsRecord[any],
	expected []timeseries.TsRecord[any],
) {
	t.Helper()

	fieldMeta, err := tsquery.NewFieldMeta("field", dataType, false)
	require.NoError(t, err)

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	ctx := context.Background()
	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10000, 0))
	require.NoError(t, err)

	alignerFilter := NewInterpolatingAlignerFilter(timeseries.NewFixedAlignmentPeriod(fixedDuration, time.Local), fillMode)

	outputResult, err := alignerFilter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := outputResult.Data().Collect(ctx)
	require.NoError(t, err)

	require.EqualValues(t,
		mapSlice(expected, func(r timeseries.TsRecord[any]) time.Time { return r.Timestamp }),
		mapSlice(resultData, func(r timeseries.TsRecord[any]) time.Time { return r.Timestamp }),
		"Timestamps mismatch",
	)

	assertValuesEqualWithTolerance(t,
		mapSlice(expected, func(r timeseries.TsRecord[any]) any { return r.Value }),
		mapSlice(resultData, func(r timeseries.TsRecord[any]) any { return r.Value }),
		1e-10,
		"Values mismatch",
	)

	require.Len(t, resultData, len(expected), "Number of resulting records mismatch")
}

// --- Test Cases for Interpolating AlignerFilter with Fill Mode ---

func TestAlignerFilter_FillLinear_BasicGapFilling(t *testing.T) {
	// 2 points 1 hour apart: t=0 value=100.0, t=3600 value=200.0
	// Align to 60s with linear fill mode.
	// Should produce 61 dense points from t=0 to t=3600 (inclusive).
	// Linearly interpolated: value at t=T is 100 + 100*(T/3600)
	fieldMeta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Unix(0, 0)},
		{Value: 200.0, Timestamp: time.Unix(3600, 0)},
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	ctx := context.Background()
	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10000, 0))
	require.NoError(t, err)

	alignerFilter := NewInterpolatingAlignerFilter(
		timeseries.NewFixedAlignmentPeriod(time.Minute, time.Local),
		timeseries.FillModeLinear,
	)

	outputResult, err := alignerFilter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := outputResult.Data().Collect(ctx)
	require.NoError(t, err)

	// Should produce 61 points: t=0, t=60, t=120, ..., t=3600
	require.Len(t, resultData, 61, "Expected 61 dense points from t=0 to t=3600 at 60s intervals")

	// First point: t=0, value=100.0
	require.Equal(t, time.Unix(0, 0), resultData[0].Timestamp)
	require.InDelta(t, 100.0, resultData[0].Value.(float64), 1e-10)

	// Last point: t=3600, value=200.0
	require.Equal(t, time.Unix(3600, 0), resultData[60].Timestamp)
	require.InDelta(t, 200.0, resultData[60].Value.(float64), 1e-10)

	// Point at t=60 (index 1): 100 + 100*(60/3600) = 100 + 1.6666... = 101.6666...
	require.Equal(t, time.Unix(60, 0), resultData[1].Timestamp)
	require.InDelta(t, 101.66666666666667, resultData[1].Value.(float64), 1e-6)

	// Point at t=1800 (index 30, halfway): 100 + 100*(1800/3600) = 150.0
	require.Equal(t, time.Unix(1800, 0), resultData[30].Timestamp)
	require.InDelta(t, 150.0, resultData[30].Value.(float64), 1e-10)

	// Point at t=3540 (index 59): 100 + 100*(3540/3600) = 198.3333...
	require.Equal(t, time.Unix(3540, 0), resultData[59].Timestamp)
	require.InDelta(t, 198.33333333333334, resultData[59].Value.(float64), 1e-6)
}

func TestAlignerFilter_FillLinear_NoGaps(t *testing.T) {
	// Dense data at every minute boundary: no gaps to fill.
	// With fillMode=linear, output should be the same 4 points.
	testInterpolatingAlignerFilterAsExpected(
		t,
		time.Minute,
		timeseries.FillModeLinear,
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},
			{Value: 150.0, Timestamp: time.Unix(60, 0)},
			{Value: 200.0, Timestamp: time.Unix(120, 0)},
			{Value: 250.0, Timestamp: time.Unix(180, 0)},
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},
			{Value: 150.0, Timestamp: time.Unix(60, 0)},
			{Value: 200.0, Timestamp: time.Unix(120, 0)},
			{Value: 250.0, Timestamp: time.Unix(180, 0)},
		},
	)
}

func TestAlignerFilter_FillForwardFill_BasicGapFilling(t *testing.T) {
	// 2 points 1 hour apart: t=0 value=100.0, t=3600 value=200.0
	// Align to 60s with forward fill mode.
	// Should produce 61 points. First 60 have value=100.0, last has value=200.0.
	fieldMeta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: 100.0, Timestamp: time.Unix(0, 0)},
		{Value: 200.0, Timestamp: time.Unix(3600, 0)},
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	ctx := context.Background()
	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10000, 0))
	require.NoError(t, err)

	alignerFilter := NewInterpolatingAlignerFilter(
		timeseries.NewFixedAlignmentPeriod(time.Minute, time.Local),
		timeseries.FillModeForwardFill,
	)

	outputResult, err := alignerFilter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := outputResult.Data().Collect(ctx)
	require.NoError(t, err)

	require.Len(t, resultData, 61, "Expected 61 dense points from t=0 to t=3600 at 60s intervals")

	// First point: t=0, value=100.0
	require.Equal(t, time.Unix(0, 0), resultData[0].Timestamp)
	require.InDelta(t, 100.0, resultData[0].Value.(float64), 1e-10)

	// All intermediate points (indices 1-59) should have value=100.0 (forward-filled)
	for i := 1; i < 60; i++ {
		require.Equal(t, time.Unix(int64(i*60), 0), resultData[i].Timestamp, "Timestamp mismatch at index %d", i)
		require.InDelta(t, 100.0, resultData[i].Value.(float64), 1e-10, "Value at index %d should be forward-filled to 100.0", i)
	}

	// Last point: t=3600, value=200.0
	require.Equal(t, time.Unix(3600, 0), resultData[60].Timestamp)
	require.InDelta(t, 200.0, resultData[60].Value.(float64), 1e-10)
}

func TestAlignerFilter_FillLinear_SingleDataPoint(t *testing.T) {
	// Single point at t=45 value=100.0, align to 60s, fillMode=linear.
	// Should produce 1 point: t=0 value=100.0 (smeared to boundary).
	testInterpolatingAlignerFilterAsExpected(
		t,
		time.Minute,
		timeseries.FillModeLinear,
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(45, 0)},
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},
		},
	)
}

func TestAlignerFilter_FillLinear_EmptyStream(t *testing.T) {
	// Empty input with fillMode=linear should produce empty output.
	testInterpolatingAlignerFilterAsExpected(
		t,
		time.Minute,
		timeseries.FillModeLinear,
		[]timeseries.TsRecord[any]{},
		[]timeseries.TsRecord[any]{},
	)
}

func TestAlignerFilter_FillLinear_LargeGaps(t *testing.T) {
	// 3 points: t=0 value=0.0, t=3600 value=100.0, t=7200 value=200.0
	// Align to 60s, fillMode=linear.
	// Should produce 121 points (t=0 to t=7200 at every 60s).
	fieldMeta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: 0.0, Timestamp: time.Unix(0, 0)},
		{Value: 100.0, Timestamp: time.Unix(3600, 0)},
		{Value: 200.0, Timestamp: time.Unix(7200, 0)},
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	ctx := context.Background()
	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10000, 0))
	require.NoError(t, err)

	alignerFilter := NewInterpolatingAlignerFilter(
		timeseries.NewFixedAlignmentPeriod(time.Minute, time.Local),
		timeseries.FillModeLinear,
	)

	outputResult, err := alignerFilter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := outputResult.Data().Collect(ctx)
	require.NoError(t, err)

	require.Len(t, resultData, 121, "Expected 121 dense points from t=0 to t=7200 at 60s intervals")

	// First point: t=0, value=0.0
	require.Equal(t, time.Unix(0, 0), resultData[0].Timestamp)
	require.InDelta(t, 0.0, resultData[0].Value.(float64), 1e-10)

	// Midpoint of first segment: t=1800 (index 30), value should be ~50.0
	// (halfway between 0.0 at t=0 and 100.0 at t=3600)
	require.Equal(t, time.Unix(1800, 0), resultData[30].Timestamp)
	require.InDelta(t, 50.0, resultData[30].Value.(float64), 1e-10)

	// Boundary point: t=3600 (index 60), value=100.0
	require.Equal(t, time.Unix(3600, 0), resultData[60].Timestamp)
	require.InDelta(t, 100.0, resultData[60].Value.(float64), 1e-10)

	// Midpoint of second segment: t=5400 (index 90), value should be ~150.0
	// (halfway between 100.0 at t=3600 and 200.0 at t=7200)
	require.Equal(t, time.Unix(5400, 0), resultData[90].Timestamp)
	require.InDelta(t, 150.0, resultData[90].Value.(float64), 1e-10)

	// Last point: t=7200 (index 120), value=200.0
	require.Equal(t, time.Unix(7200, 0), resultData[120].Timestamp)
	require.InDelta(t, 200.0, resultData[120].Value.(float64), 1e-10)
}

func TestAlignerFilter_FillLinear_IntegerDataType(t *testing.T) {
	// Integer data type: 2 points t=0 value=int64(100), t=3600 value=int64(200).
	// Align to 60s, fillMode=linear.
	// Verify t=1800 produces int64(150) and t=60 produces int64(101) (truncated from 101.666...).
	fieldMeta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: int64(100), Timestamp: time.Unix(0, 0)},
		{Value: int64(200), Timestamp: time.Unix(3600, 0)},
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	ctx := context.Background()
	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10000, 0))
	require.NoError(t, err)

	alignerFilter := NewInterpolatingAlignerFilter(
		timeseries.NewFixedAlignmentPeriod(time.Minute, time.Local),
		timeseries.FillModeLinear,
	)

	outputResult, err := alignerFilter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := outputResult.Data().Collect(ctx)
	require.NoError(t, err)

	require.Len(t, resultData, 61, "Expected 61 dense points from t=0 to t=3600 at 60s intervals")

	// First point: t=0, value=int64(100)
	require.Equal(t, time.Unix(0, 0), resultData[0].Timestamp)
	require.Equal(t, int64(100), resultData[0].Value)

	// Point at t=60 (index 1): 100 + 100*(60/3600) = 101.666... -> truncated to int64(101)
	require.Equal(t, time.Unix(60, 0), resultData[1].Timestamp)
	require.Equal(t, int64(101), resultData[1].Value)

	// Point at t=1800 (index 30, halfway): 100 + 100*(1800/3600) = 150.0 -> int64(150)
	require.Equal(t, time.Unix(1800, 0), resultData[30].Timestamp)
	require.Equal(t, int64(150), resultData[30].Value)

	// Last point: t=3600, value=int64(200)
	require.Equal(t, time.Unix(3600, 0), resultData[60].Timestamp)
	require.Equal(t, int64(200), resultData[60].Value)
}

func TestAlignerFilter_DefaultNoFill(t *testing.T) {
	// Regression test: NewAlignerFilter (no fillMode) with 2 points 1 hour apart
	// and 60s alignment should produce only 2 points (not 61).
	// This verifies that the original behavior is preserved.
	testAlignerFilterAsExpected(
		t,
		time.Minute,
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},
			{Value: 200.0, Timestamp: time.Unix(3600, 0)},
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},
			{Value: 200.0, Timestamp: time.Unix(3600, 0)},
		},
	)
}
