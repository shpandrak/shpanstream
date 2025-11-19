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

// --- Test Cases for AlignerFilter ---

func TestAlignerFilter_Aligned(t *testing.T) {
	// Input data points fall exactly on the minute boundaries.
	// Expected output should be the same points, as no interpolation is needed.
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: 150.0, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: 200.0, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 250.0, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: 150.0, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: 200.0, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 250.0, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
	)
}

func TestAlignerFilter_AlignedExpanded(t *testing.T) {
	// Input data points fall exactly on minute boundaries.
	// Align to every 2 minutes. Expect output only at 0, 120, 240 seconds.
	testAlignerFilterAsExpected(
		t,
		2*time.Minute, // Align to every 2 minutes
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: 150.0, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: 200.0, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 250.0, Timestamp: time.Unix(180, 0)}, // 00:03:00
			{Value: 300.0, Timestamp: time.Unix(240, 0)}, // 00:04:00
			{Value: 350.0, Timestamp: time.Unix(300, 0)}, // 00:05:00
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},   // 00:00:00 (exact match)
			{Value: 200.0, Timestamp: time.Unix(120, 0)}, // 00:02:00 (exact match)
			{Value: 300.0, Timestamp: time.Unix(240, 0)}, // 00:04:00 (exact match)
		},
	)
}

func TestAlignerFilter_NonAligned(t *testing.T) {
	// Input data points are not aligned with minute boundaries.
	// Expect interpolation to calculate values at 60, 120, 180 seconds.
	// Interpolation at t=60: Between t=45 (100) and t=105 (200).
	// 60s is 15s after 45s. Total interval 105-45=60s. Weight = 15/60 = 0.25
	// value: 100 + (200-100) * 0.25 = 100 + 25 = 125
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(45, 0)},  // 00:00:45
			{Value: 200.0, Timestamp: time.Unix(105, 0)}, // 00:01:45
			{Value: 300.0, Timestamp: time.Unix(165, 0)}, // 00:02:45
			{Value: 400.0, Timestamp: time.Unix(225, 0)}, // 00:03:45
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},   // 00:00:00 (smeared)
			{Value: 125.0, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: 225.0, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 325.0, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
	)
}

func TestAlignerFilter_NonAligned_DifferentValues(t *testing.T) {
	// Testing with different value ranges to ensure interpolation works correctly
	// across different magnitudes (small decimals)
	// Interpolation at t=60: Between t=45 (10.0) and t=105 (40.0).
	// Weight = 15/60 = 0.25
	// value: 10 + (40-10) * 0.25 = 10 + 7.5 = 17.5
	testAlignerFilterAsExpected(
		t,
		time.Minute,
		[]timeseries.TsRecord[any]{
			{Value: 10.0, Timestamp: time.Unix(45, 0)},   // 00:00:45
			{Value: 40.0, Timestamp: time.Unix(105, 0)},  // 00:01:45
			{Value: 70.0, Timestamp: time.Unix(165, 0)},  // 00:02:45
			{Value: 100.0, Timestamp: time.Unix(225, 0)}, // 00:03:45
		},
		[]timeseries.TsRecord[any]{
			{Value: 10.0, Timestamp: time.Unix(0, 0)},   // 00:00:00 (smeared)
			{Value: 17.5, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: 47.5, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 77.5, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
	)
}

func TestAlignerFilter_NonAligned_LargeValues(t *testing.T) {
	// Testing with larger value ranges to ensure interpolation works correctly
	// across different magnitudes (thousands)
	// Interpolation at t=60: Between t=45 (1000.0) and t=105 (2000.0).
	// Weight = 15/60 = 0.25
	// value: 1000 + (2000-1000) * 0.25 = 1000 + 250 = 1250
	testAlignerFilterAsExpected(
		t,
		time.Minute,
		[]timeseries.TsRecord[any]{
			{Value: 1000.0, Timestamp: time.Unix(45, 0)},  // 00:00:45
			{Value: 2000.0, Timestamp: time.Unix(105, 0)}, // 00:01:45
			{Value: 3000.0, Timestamp: time.Unix(165, 0)}, // 00:02:45
			{Value: 4000.0, Timestamp: time.Unix(225, 0)}, // 00:03:45
		},
		[]timeseries.TsRecord[any]{
			{Value: 1000.0, Timestamp: time.Unix(0, 0)},  // 00:00:00 (smeared)
			{Value: 1250.0, Timestamp: time.Unix(60, 0)}, // 00:01:00
			{Value: 2250.0, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 3250.0, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
	)
}

func TestAlignerFilter_SingleItem(t *testing.T) {
	// Only one data point. Cannot interpolate to find values at alignment points.
	testAlignerFilterAsExpected(
		t,
		time.Minute,
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(45, 0)}, // 00:00:45
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)}, // 00:00:00 (smeared)
		},
	)
}

func TestAlignerFilter_TwoItems(t *testing.T) {
	// Two data points allow for interpolation between them.
	// Interpolation at t=60: Between t=45 and t=105 (60s interval), 15s after 45s. Weight = 0.25
	// value: 100 + (200-100) * 0.25 = 125
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(45, 0)},  // 00:00:45
			{Value: 200.0, Timestamp: time.Unix(105, 0)}, // 00:01:45
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},  // 00:00:00 (smeared)
			{Value: 125.0, Timestamp: time.Unix(60, 0)}, // 00:01:00
		},
	)
}

func TestAlignerFilter_ExactMatchSinglePoint(t *testing.T) {
	// A single data point that falls exactly on an alignment boundary.
	testAlignerFilterAsExpected(
		t,
		time.Minute,
		[]timeseries.TsRecord[any]{
			{Value: 150.0, Timestamp: time.Unix(60, 0)}, // 00:01:00
		},
		[]timeseries.TsRecord[any]{
			{Value: 150.0, Timestamp: time.Unix(60, 0)}, // 00:01:00
		},
	)
}

func TestAlignerFilter_MultiplePointsWithinInterval(t *testing.T) {
	// Multiple points exist between alignment boundaries.
	// At t=60: Between t=50 (140) and t=70 (200). Weight = 10/20 = 0.5
	// value: 140 + (200-140) * 0.5 = 140 + 30 = 170
	// At t=120: Between t=110 (240) and t=130 (300). Weight = 10/20 = 0.5
	// value: 240 + (300-240) * 0.5 = 240 + 30 = 270
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(10, 0)},  // 00:00:10
			{Value: 120.0, Timestamp: time.Unix(30, 0)},  // 00:00:30
			{Value: 140.0, Timestamp: time.Unix(50, 0)},  // 00:00:50
			{Value: 200.0, Timestamp: time.Unix(70, 0)},  // 00:01:10
			{Value: 220.0, Timestamp: time.Unix(90, 0)},  // 00:01:30
			{Value: 240.0, Timestamp: time.Unix(110, 0)}, // 00:01:50
			{Value: 300.0, Timestamp: time.Unix(130, 0)}, // 00:02:10
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},   // 00:00:00 smeared first item
			{Value: 170.0, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: 270.0, Timestamp: time.Unix(120, 0)}, // 00:02:00
		},
	)
}

func TestAlignerFilter_EmptyStaysEmpty(t *testing.T) {
	// Empty input stream should result in an empty output stream.
	testAlignerFilterAsExpected(
		t,
		time.Minute,
		[]timeseries.TsRecord[any]{}, // Empty input
		[]timeseries.TsRecord[any]{}, // Empty expected output
	)
}

func TestAlignerFilter_UnevenTemporal(t *testing.T) {
	// Input data points have varying intervals between them
	// At t=120: Between t=10 (100) and t=150 (200). Weight = 110/140
	// value: 100 + (200-100) * 110/140 = 100 + 78.571... = 178.571...
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(10, 0)},  // 00:00:10
			{Value: 200.0, Timestamp: time.Unix(150, 0)}, // 00:02:30 (big gap)
			{Value: 300.0, Timestamp: time.Unix(170, 0)}, // 00:02:50 (small gap)
			{Value: 400.0, Timestamp: time.Unix(300, 0)}, // 00:05:00 (medium gap)
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},                 // 00:00:00 (smeared)
			{Value: 178.57142857142858, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 400.0, Timestamp: time.Unix(300, 0)},               // 00:05:00
		},
	)
}

func TestAlignerFilter_LargeGaps(t *testing.T) {
	// Test with large gaps between data points (hours)
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Still align to every minute
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},    // 00:00:00
			{Value: 200.0, Timestamp: time.Unix(3600, 0)}, // 01:00:00 (1 hour later)
			{Value: 300.0, Timestamp: time.Unix(7200, 0)}, // 02:00:00 (1 hour later)
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},    // 00:00:00
			{Value: 200.0, Timestamp: time.Unix(3600, 0)}, // 01:00:00 (1 hour later)
			{Value: 300.0, Timestamp: time.Unix(7200, 0)}, // 02:00:00 (1 hour later)
		},
	)
}

func TestAlignerFilter_PointExactlyAtBoundary(t *testing.T) {
	// Test with points exactly at alignment boundaries
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},   // 00:00:00 (exactly at boundary)
			{Value: 200.0, Timestamp: time.Unix(45, 0)},  // 00:00:45 (not at boundary)
			{Value: 300.0, Timestamp: time.Unix(60, 0)},  // 00:01:00 (exactly at boundary)
			{Value: 400.0, Timestamp: time.Unix(75, 0)},  // 00:01:15 (not at boundary)
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},  // 00:00:00 (preserved exactly)
			{Value: 300.0, Timestamp: time.Unix(60, 0)}, // 00:01:00 (preserved exactly)
		},
	)
}

func TestAlignerFilter_NonIntegerAlignment(t *testing.T) {
	// Test with a non-standard alignment period (e.g., 90 seconds)
	// At t=90: Between t=30 (100) and t=120 (200). Weight = 60/90 = 2/3
	// value: 100 + (200-100) * 2/3 = 100 + 66.666... = 166.666...
	// At t=180: Between t=120 (200) and t=210 (300). Weight = 60/90 = 2/3
	// value: 200 + (300-200) * 2/3 = 200 + 66.666... = 266.666...
	testAlignerFilterAsExpected(
		t,
		90*time.Second, // Align to every 1.5 minutes
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(30, 0)},  // 00:00:30
			{Value: 200.0, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 300.0, Timestamp: time.Unix(210, 0)}, // 00:03:30
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.0, Timestamp: time.Unix(0, 0)},                 // 00:00:00 (smeared)
			{Value: 166.66666666666666, Timestamp: time.Unix(90, 0)},  // 00:01:30
			{Value: 266.6666666666667, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
	)
}

func TestAlignerFilter_IntegerDataType(t *testing.T) {
	// Test with INTEGER data types - values should be converted to float64 for calculation,
	// then truncated back to int64
	// At t=120: Between t=10 (100) and t=150 (200). Weight = 110/140
	// value: 100 + (200-100) * 110/140 = 100 + 78.571... = 178.571... → truncated to 178
	testAlignerFilterWithFieldMeta(
		t,
		time.Minute, // Align to every minute
		tsquery.DataTypeInteger,
		[]timeseries.TsRecord[any]{
			{Value: int64(100), Timestamp: time.Unix(10, 0)},  // 00:00:10
			{Value: int64(200), Timestamp: time.Unix(150, 0)}, // 00:02:30 (big gap)
			{Value: int64(300), Timestamp: time.Unix(170, 0)}, // 00:02:50 (small gap)
			{Value: int64(400), Timestamp: time.Unix(300, 0)}, // 00:05:00 (medium gap)
		},
		[]timeseries.TsRecord[any]{
			{Value: int64(100), Timestamp: time.Unix(0, 0)},   // 00:00:00 (smeared)
			{Value: int64(178), Timestamp: time.Unix(120, 0)}, // 00:02:00 (truncated from weighted average)
			{Value: int64(400), Timestamp: time.Unix(300, 0)}, // 00:05:00
		},
	)
}

func TestAlignerFilter_DecimalDataType(t *testing.T) {
	// Test with DECIMAL data types explicitly to ensure proper handling
	testAlignerFilterWithFieldMeta(
		t,
		time.Minute,
		tsquery.DataTypeDecimal,
		[]timeseries.TsRecord[any]{
			{Value: 100.5, Timestamp: time.Unix(45, 0)},  // 00:00:45
			{Value: 200.5, Timestamp: time.Unix(105, 0)}, // 00:01:45
		},
		[]timeseries.TsRecord[any]{
			{Value: 100.5, Timestamp: time.Unix(0, 0)},  // 00:00:00 (smeared)
			{Value: 125.5, Timestamp: time.Unix(60, 0)}, // 00:01:00 (interpolated)
		},
	)
}

func TestAlignerFilter_ErrorOnStringDataType(t *testing.T) {
	// Test that we get an error when trying to align string data types
	// We need at least 2 records in different alignment periods where the second doesn't
	// fall exactly on the boundary, to trigger interpolation
	fieldMeta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeString, false)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: "value1", Timestamp: time.Unix(10, 0)},  // 00:00:10
		{Value: "value2", Timestamp: time.Unix(105, 0)}, // 00:01:45 (not on boundary, will trigger interpolation)
	}

	// Create input Result
	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	// Get the result from the datasource
	ctx := context.Background()
	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(200, 0))
	require.NoError(t, err)

	// Create the aligner filter
	alignerFilter := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(time.Minute, time.Local))

	// Apply the filter
	outputResult, err := alignerFilter.Filter(result)
	require.NoError(t, err) // Filter itself doesn't error, the error occurs during stream consumption

	// Try to collect the results - this should trigger the error when interpolation is attempted
	_, err = outputResult.Data().Collect(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported data type")
}

func TestAlignerFilter_ErrorOnBooleanDataType(t *testing.T) {
	// Test that we get an error when trying to align boolean data types
	// We need at least 2 records in different alignment periods where the second doesn't
	// fall exactly on the boundary, to trigger interpolation
	fieldMeta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeBoolean, false)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: true, Timestamp: time.Unix(10, 0)},   // 00:00:10
		{Value: false, Timestamp: time.Unix(105, 0)}, // 00:01:45 (not on boundary, will trigger interpolation)
	}

	// Create input Result
	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	// Get the result from the datasource
	ctx := context.Background()
	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(200, 0))
	require.NoError(t, err)

	// Create the aligner filter
	alignerFilter := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(time.Minute, time.Local))

	// Apply the filter
	outputResult, err := alignerFilter.Filter(result)
	require.NoError(t, err) // Filter itself doesn't error, the error occurs during stream consumption

	// Try to collect the results - this should trigger the error when interpolation is attempted
	_, err = outputResult.Data().Collect(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported data type")
}

// --- Test Helper Functions ---

// testAlignerFilterAsExpected runs the AlignerFilter.Filter method and asserts the output.
func testAlignerFilterAsExpected(
	t *testing.T,
	fixedDuration time.Duration,
	records []timeseries.TsRecord[any],
	expected []timeseries.TsRecord[any],
) {
	t.Helper() // Marks this function as a test helper

	// Create field metadata - default to decimal (float64) for these tests
	fieldMeta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)

	// Create input Result using StaticDatasource
	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	// Get the result from the datasource (with a wide time range to include all test data)
	ctx := context.Background()
	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10000, 0))
	require.NoError(t, err)

	// Create the aligner filter
	alignerFilter := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(fixedDuration, time.Local))

	// Apply the filter
	outputResult, err := alignerFilter.Filter(result)
	require.NoError(t, err)

	// Collect the resulting aligned records
	result_data, err := outputResult.Data().Collect(ctx)
	require.NoError(t, err)

	// Assert the results
	// Compare timestamps first for easier debugging if lengths differ
	require.EqualValues(t,
		mapSlice(expected, func(r timeseries.TsRecord[any]) time.Time { return r.Timestamp }),
		mapSlice(result_data, func(r timeseries.TsRecord[any]) time.Time { return r.Timestamp }),
		"Timestamps mismatch",
	)

	// Compare values with tolerance for floating-point precision
	assertValuesEqualWithTolerance(t,
		mapSlice(expected, func(r timeseries.TsRecord[any]) any { return r.Value }),
		mapSlice(result_data, func(r timeseries.TsRecord[any]) any { return r.Value }),
		1e-10, // Tolerance for floating-point comparison
		"Values mismatch",
	)

	// Explicit length check can also be helpful
	require.Len(t, result_data, len(expected), "Number of resulting records mismatch")
}

// testAlignerFilterWithFieldMeta runs the AlignerFilter.Filter method with custom field metadata and asserts the output.
func testAlignerFilterWithFieldMeta(
	t *testing.T,
	fixedDuration time.Duration,
	dataType tsquery.DataType,
	records []timeseries.TsRecord[any],
	expected []timeseries.TsRecord[any],
) {
	t.Helper() // Marks this function as a test helper

	// Create field metadata based on provided data type
	fieldMeta, err := tsquery.NewFieldMeta("field", dataType, false)
	require.NoError(t, err)

	// Create input Result using StaticDatasource
	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	// Get the result from the datasource (with a wide time range to include all test data)
	ctx := context.Background()
	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10000, 0))
	require.NoError(t, err)

	// Create the aligner filter
	alignerFilter := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(fixedDuration, time.Local))

	// Apply the filter
	outputResult, err := alignerFilter.Filter(result)
	require.NoError(t, err)

	// Collect the resulting aligned records
	result_data, err := outputResult.Data().Collect(ctx)
	require.NoError(t, err)

	// Assert the results
	// Compare timestamps first for easier debugging if lengths differ
	require.EqualValues(t,
		mapSlice(expected, func(r timeseries.TsRecord[any]) time.Time { return r.Timestamp }),
		mapSlice(result_data, func(r timeseries.TsRecord[any]) time.Time { return r.Timestamp }),
		"Timestamps mismatch",
	)

	// Compare values with tolerance for floating-point precision
	assertValuesEqualWithTolerance(t,
		mapSlice(expected, func(r timeseries.TsRecord[any]) any { return r.Value }),
		mapSlice(result_data, func(r timeseries.TsRecord[any]) any { return r.Value }),
		1e-10, // Tolerance for floating-point comparison
		"Values mismatch",
	)

	// Explicit length check can also be helpful
	require.Len(t, result_data, len(expected), "Number of resulting records mismatch")
}

// mapSlice is a helper to transform slices
func mapSlice[A any, B any](input []A, m func(a A) B) []B {
	ret := make([]B, len(input))
	for i, currElem := range input {
		ret[i] = m(currElem)
	}
	return ret
}

// assertValuesEqualWithTolerance compares two slices of values with tolerance for floating-point precision
func assertValuesEqualWithTolerance(t *testing.T, expected, actual []any, delta float64, msgAndArgs ...interface{}) {
	t.Helper()

	require.Len(t, actual, len(expected), msgAndArgs...)

	for i := range expected {
		expVal := expected[i]
		actVal := actual[i]

		// If both are float64, use delta comparison
		expFloat, expIsFloat := expVal.(float64)
		actFloat, actIsFloat := actVal.(float64)

		if expIsFloat && actIsFloat {
			require.InDelta(t, expFloat, actFloat, delta, "Record %d: float value mismatch", i)
		} else {
			// For non-float types, use exact comparison
			require.Equal(t, expVal, actVal, "Record %d: value mismatch", i)
		}
	}
}