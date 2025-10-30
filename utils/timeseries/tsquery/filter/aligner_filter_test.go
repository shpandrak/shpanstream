package filter

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

// --- Test Cases for AlignerFilter ---

func TestAlignerFilter_Aligned(t *testing.T) {
	// Input data points fall exactly on the minute boundaries.
	// Expected output should be the same points, as no interpolation is needed.
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]tsquery.Record{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: []any{150.0, 20.0, 1500.0}, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: []any{200.0, 30.0, 2000.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: []any{250.0, 40.0, 2500.0}, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
		[]tsquery.Record{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: []any{150.0, 20.0, 1500.0}, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: []any{200.0, 30.0, 2000.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: []any{250.0, 40.0, 2500.0}, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
	)
}

func TestAlignerFilter_AlignedExpanded(t *testing.T) {
	// Input data points fall exactly on minute boundaries.
	// Align to every 2 minutes. Expect output only at 0, 120, 240 seconds.
	testAlignerFilterAsExpected(
		t,
		2*time.Minute, // Align to every 2 minutes
		[]tsquery.Record{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: []any{150.0, 20.0, 1500.0}, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: []any{200.0, 30.0, 2000.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: []any{250.0, 40.0, 2500.0}, Timestamp: time.Unix(180, 0)}, // 00:03:00
			{Value: []any{300.0, 50.0, 3000.0}, Timestamp: time.Unix(240, 0)}, // 00:04:00
			{Value: []any{350.0, 60.0, 3500.0}, Timestamp: time.Unix(300, 0)}, // 00:05:00
		},
		[]tsquery.Record{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},   // 00:00:00 (exact match)
			{Value: []any{200.0, 30.0, 2000.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00 (exact match)
			{Value: []any{300.0, 50.0, 3000.0}, Timestamp: time.Unix(240, 0)}, // 00:04:00 (exact match)
		},
	)
}

func TestAlignerFilter_NonAligned(t *testing.T) {
	// Input data points are not aligned with minute boundaries.
	// Expect interpolation to calculate values at 60, 120, 180 seconds.
	// Interpolation at t=60: Between t=45 (100, 10, 1000) and t=105 (200, 40, 2000).
	// 60s is 15s after 45s. Total interval 105-45=60s. Weight = 15/60 = 0.25
	// arr[0]: 100 + (200-100) * 0.25 = 100 + 25 = 125
	// arr[1]: 10 + (40-10) * 0.25 = 10 + 7.5 = 17.5
	// arr[2]: 1000 + (2000-1000) * 0.25 = 1000 + 250 = 1250
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]tsquery.Record{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(45, 0)},   // 00:00:45
			{Value: []any{200.0, 40.0, 2000.0}, Timestamp: time.Unix(105, 0)},  // 00:01:45
			{Value: []any{300.0, 70.0, 3000.0}, Timestamp: time.Unix(165, 0)},  // 00:02:45
			{Value: []any{400.0, 100.0, 4000.0}, Timestamp: time.Unix(225, 0)}, // 00:03:45
		},
		[]tsquery.Record{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},   // 00:00:00 (smeared)
			{Value: []any{125.0, 17.5, 1250.0}, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: []any{225.0, 47.5, 2250.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: []any{325.0, 77.5, 3250.0}, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
	)
}

func TestAlignerFilter_SingleItem(t *testing.T) {
	// Only one data point. Cannot interpolate to find values at alignment points.
	testAlignerFilterAsExpected(
		t,
		time.Minute,
		[]tsquery.Record{
			{Value: []any{100.0, 50.0, 2000.0}, Timestamp: time.Unix(45, 0)}, // 00:00:45
		},
		[]tsquery.Record{
			{Value: []any{100.0, 50.0, 2000.0}, Timestamp: time.Unix(0, 0)}, // 00:00:00 (smeared)
		},
	)
}

func TestAlignerFilter_TwoItems(t *testing.T) {
	// Two data points allow for interpolation between them.
	// Interpolation at t=60: Between t=45 and t=105 (60s interval), 15s after 45s. Weight = 0.25
	// arr[0]: 100 + (200-100) * 0.25 = 125
	// arr[1]: 20 + (80-20) * 0.25 = 20 + 15 = 35
	// arr[2]: 500 + (1500-500) * 0.25 = 500 + 250 = 750
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]tsquery.Record{
			{Value: []any{100.0, 20.0, 500.0}, Timestamp: time.Unix(45, 0)},  // 00:00:45
			{Value: []any{200.0, 80.0, 1500.0}, Timestamp: time.Unix(105, 0)}, // 00:01:45
		},
		[]tsquery.Record{
			{Value: []any{100.0, 20.0, 500.0}, Timestamp: time.Unix(0, 0)},  // 00:00:00 (smeared)
			{Value: []any{125.0, 35.0, 750.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00
		},
	)
}

func TestAlignerFilter_ExactMatchSinglePoint(t *testing.T) {
	// A single data point that falls exactly on an alignment boundary.
	testAlignerFilterAsExpected(
		t,
		time.Minute,
		[]tsquery.Record{
			{Value: []any{150.0, 75.0, 3000.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00
		},
		[]tsquery.Record{
			{Value: []any{150.0, 75.0, 3000.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00
		},
	)
}

func TestAlignerFilter_MultiplePointsWithinInterval(t *testing.T) {
	// Multiple points exist between alignment boundaries.
	// At t=60: Between t=50 (140, 14, 1400) and t=70 (200, 20, 2000). Weight = 10/20 = 0.5
	// arr[0]: 140 + (200-140) * 0.5 = 140 + 30 = 170
	// arr[1]: 14 + (20-14) * 0.5 = 14 + 3 = 17
	// arr[2]: 1400 + (2000-1400) * 0.5 = 1400 + 300 = 1700
	// At t=120: Between t=110 (240, 24, 2400) and t=130 (300, 30, 3000). Weight = 10/20 = 0.5
	// arr[0]: 240 + (300-240) * 0.5 = 240 + 30 = 270
	// arr[1]: 24 + (30-24) * 0.5 = 24 + 3 = 27
	// arr[2]: 2400 + (3000-2400) * 0.5 = 2400 + 300 = 2700
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]tsquery.Record{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(10, 0)},  // 00:00:10
			{Value: []any{120.0, 12.0, 1200.0}, Timestamp: time.Unix(30, 0)},  // 00:00:30
			{Value: []any{140.0, 14.0, 1400.0}, Timestamp: time.Unix(50, 0)},  // 00:00:50
			{Value: []any{200.0, 20.0, 2000.0}, Timestamp: time.Unix(70, 0)},  // 00:01:10
			{Value: []any{220.0, 22.0, 2200.0}, Timestamp: time.Unix(90, 0)},  // 00:01:30
			{Value: []any{240.0, 24.0, 2400.0}, Timestamp: time.Unix(110, 0)}, // 00:01:50
			{Value: []any{300.0, 30.0, 3000.0}, Timestamp: time.Unix(130, 0)}, // 00:02:10
		},
		[]tsquery.Record{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},  // 00:00:00 smeared first item
			{Value: []any{170.0, 17.0, 1700.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00
			{Value: []any{270.0, 27.0, 2700.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00
		},
	)
}

func TestAlignerFilter_EmptyStaysEmpty(t *testing.T) {
	// Empty input stream should result in an empty output stream.
	testAlignerFilterAsExpected(
		t,
		time.Minute,
		[]tsquery.Record{}, // Empty input
		[]tsquery.Record{}, // Empty expected output
	)
}

func TestAlignerFilter_UnevenTemporal(t *testing.T) {
	// Input data points have varying intervals between them
	// At t=120: Between t=10 (100, 10, 1000) and t=150 (200, 30, 3000). Weight = 110/140
	// arr[0]: 100 + (200-100) * 110/140 = 100 + 78.571... = 178.571...
	// arr[1]: 10 + (30-10) * 110/140 = 10 + 15.714... = 25.714...
	// arr[2]: 1000 + (3000-1000) * 110/140 = 1000 + 1571.428... = 2571.428...
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]tsquery.Record{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(10, 0)},  // 00:00:10
			{Value: []any{200.0, 30.0, 3000.0}, Timestamp: time.Unix(150, 0)}, // 00:02:30 (big gap)
			{Value: []any{300.0, 50.0, 5000.0}, Timestamp: time.Unix(170, 0)}, // 00:02:50 (small gap)
			{Value: []any{400.0, 70.0, 7000.0}, Timestamp: time.Unix(300, 0)}, // 00:05:00 (medium gap)
		},
		[]tsquery.Record{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},                               // 00:00:00 (smeared)
			{Value: []any{178.57142857142858, 25.714285714285715, 2571.4285714285716}, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: []any{400.0, 70.0, 7000.0}, Timestamp: time.Unix(300, 0)},                             // 00:05:00
		},
	)
}

func TestAlignerFilter_LargeGaps(t *testing.T) {
	// Test with large gaps between data points (hours)
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Still align to every minute
		[]tsquery.Record{
			{Value: []any{100.0, 50.0, 10.0}, Timestamp: time.Unix(0, 0)},    // 00:00:00
			{Value: []any{200.0, 150.0, 20.0}, Timestamp: time.Unix(3600, 0)}, // 01:00:00 (1 hour later)
			{Value: []any{300.0, 250.0, 30.0}, Timestamp: time.Unix(7200, 0)}, // 02:00:00 (1 hour later)
		},
		[]tsquery.Record{
			{Value: []any{100.0, 50.0, 10.0}, Timestamp: time.Unix(0, 0)},    // 00:00:00
			{Value: []any{200.0, 150.0, 20.0}, Timestamp: time.Unix(3600, 0)}, // 01:00:00 (1 hour later)
			{Value: []any{300.0, 250.0, 30.0}, Timestamp: time.Unix(7200, 0)}, // 02:00:00 (1 hour later)
		},
	)
}

func TestAlignerFilter_PointExactlyAtBoundary(t *testing.T) {
	// Test with points exactly at alignment boundaries
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]tsquery.Record{
			{Value: []any{100.0, 25.0, 500.0}, Timestamp: time.Unix(0, 0)},    // 00:00:00 (exactly at boundary)
			{Value: []any{200.0, 75.0, 1500.0}, Timestamp: time.Unix(45, 0)},  // 00:00:45 (not at boundary)
			{Value: []any{300.0, 125.0, 2500.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00 (exactly at boundary)
			{Value: []any{400.0, 175.0, 3500.0}, Timestamp: time.Unix(75, 0)}, // 00:01:15 (not at boundary)
		},
		[]tsquery.Record{
			{Value: []any{100.0, 25.0, 500.0}, Timestamp: time.Unix(0, 0)},   // 00:00:00 (preserved exactly)
			{Value: []any{300.0, 125.0, 2500.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00 (preserved exactly)
		},
	)
}

func TestAlignerFilter_MultipleValues(t *testing.T) {
	// Test with multiple values in the array
	testAlignerFilterAsExpected(
		t,
		time.Minute, // Align to every minute
		[]tsquery.Record{
			{Value: []any{100.0, 50.0}, Timestamp: time.Unix(45, 0)},  // 00:00:45
			{Value: []any{200.0, 150.0}, Timestamp: time.Unix(105, 0)}, // 00:01:45
		},
		[]tsquery.Record{
			{Value: []any{100.0, 50.0}, Timestamp: time.Unix(0, 0)},  // 00:00:00 (smeared)
			{Value: []any{125.0, 75.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00 (interpolated)
		},
	)
}

func TestAlignerFilter_NonIntegerAlignment(t *testing.T) {
	// Test with a non-standard alignment period (e.g., 90 seconds)
	// At t=90: Between t=30 (100, 20, 500) and t=120 (200, 50, 1100). Weight = 60/90 = 2/3
	// arr[0]: 100 + (200-100) * 2/3 = 100 + 66.666... = 166.666...
	// arr[1]: 20 + (50-20) * 2/3 = 20 + 20 = 40
	// arr[2]: 500 + (1100-500) * 2/3 = 500 + 400 = 900
	// At t=180: Between t=120 (200, 50, 1100) and t=210 (300, 80, 1700). Weight = 60/90 = 2/3
	// arr[0]: 200 + (300-200) * 2/3 = 200 + 66.666... = 266.666...
	// arr[1]: 50 + (80-50) * 2/3 = 50 + 20 = 70
	// arr[2]: 1100 + (1700-1100) * 2/3 = 1100 + 400 = 1500
	testAlignerFilterAsExpected(
		t,
		90*time.Second, // Align to every 1.5 minutes
		[]tsquery.Record{
			{Value: []any{100.0, 20.0, 500.0}, Timestamp: time.Unix(30, 0)},  // 00:00:30
			{Value: []any{200.0, 50.0, 1100.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: []any{300.0, 80.0, 1700.0}, Timestamp: time.Unix(210, 0)}, // 00:03:30
		},
		[]tsquery.Record{
			{Value: []any{100.0, 20.0, 500.0}, Timestamp: time.Unix(0, 0)},                         // 00:00:00 (smeared)
			{Value: []any{166.66666666666666, 40.0, 900.0}, Timestamp: time.Unix(90, 0)},           // 00:01:30
			{Value: []any{266.6666666666667, 70.0, 1500.0}, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
	)
}

func TestAlignerFilter_IntegerDataType(t *testing.T) {
	// Test with INTEGER data types - values should be converted to float64 for calculation,
	// then truncated back to int64
	// At t=120: Between t=10 (100, 10, 1000) and t=150 (200, 30, 3000). Weight = 110/140
	// arr[0]: 100 + (200-100) * 110/140 = 100 + 78.571... = 178.571... → truncated to 178
	// arr[1]: 10 + (30-10) * 110/140 = 10 + 15.714... = 25.714... → truncated to 25
	// arr[2]: 1000 + (3000-1000) * 110/140 = 1000 + 1571.428... = 2571.428... → truncated to 2571
	testAlignerFilterWithFieldMeta(
		t,
		time.Minute, // Align to every minute
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		[]tsquery.Record{
			{Value: []any{int64(100), int64(10), int64(1000)}, Timestamp: time.Unix(10, 0)},  // 00:00:10
			{Value: []any{int64(200), int64(30), int64(3000)}, Timestamp: time.Unix(150, 0)}, // 00:02:30 (big gap)
			{Value: []any{int64(300), int64(50), int64(5000)}, Timestamp: time.Unix(170, 0)}, // 00:02:50 (small gap)
			{Value: []any{int64(400), int64(70), int64(7000)}, Timestamp: time.Unix(300, 0)}, // 00:05:00 (medium gap)
		},
		[]tsquery.Record{
			{Value: []any{int64(100), int64(10), int64(1000)}, Timestamp: time.Unix(0, 0)},   // 00:00:00 (smeared)
			{Value: []any{int64(178), int64(25), int64(2571)}, Timestamp: time.Unix(120, 0)}, // 00:02:00 (truncated from weighted average)
			{Value: []any{int64(400), int64(70), int64(7000)}, Timestamp: time.Unix(300, 0)}, // 00:05:00
		},
	)
}

func TestAlignerFilter_MixedDataTypes(t *testing.T) {
	// Test with mixed INTEGER and DECIMAL data types
	// First field is integer, second is decimal, third is integer
	testAlignerFilterWithFieldMeta(
		t,
		time.Minute,
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeDecimal, tsquery.DataTypeInteger},
		[]tsquery.Record{
			{Value: []any{int64(100), 20.5, int64(500)}, Timestamp: time.Unix(45, 0)},  // 00:00:45
			{Value: []any{int64(200), 80.5, int64(1500)}, Timestamp: time.Unix(105, 0)}, // 00:01:45
		},
		[]tsquery.Record{
			{Value: []any{int64(100), 20.5, int64(500)}, Timestamp: time.Unix(0, 0)},  // 00:00:00 (smeared)
			{Value: []any{int64(125), 35.5, int64(750)}, Timestamp: time.Unix(60, 0)}, // 00:01:00 (interpolated)
		},
	)
}

func TestAlignerFilter_ErrorOnNonNumericDataType(t *testing.T) {
	// Test that we get an error when trying to align non-numeric data types
	// We need at least 2 records in different alignment periods where the second doesn't
	// fall exactly on the boundary, to trigger interpolation
	fieldsMeta := []tsquery.FieldMeta{}

	// Create field metadata with a string field
	fm, err := tsquery.NewFieldMeta("field_0", tsquery.DataTypeString, false)
	require.NoError(t, err)
	fieldsMeta = append(fieldsMeta, *fm)

	records := []tsquery.Record{
		{Value: []any{"value1"}, Timestamp: time.Unix(10, 0)},  // 00:00:10
		{Value: []any{"value2"}, Timestamp: time.Unix(105, 0)}, // 00:01:45 (not on boundary, will trigger interpolation)
	}

	// Create input Result
	inputStream := stream.Just(records...)
	inputResult := tsquery.NewResult(fieldsMeta, inputStream)

	// Create the aligner filter
	alignerFilter := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(time.Minute, time.Local))

	// Apply the filter
	outputResult, err := alignerFilter.Filter(*inputResult)
	require.NoError(t, err) // Filter itself doesn't error, the error occurs during stream consumption

	// Try to collect the results - this should trigger the error when interpolation is attempted
	ctx := context.Background()
	_, err = outputResult.Stream().Collect(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported data type")
}

func TestAlignerFilter_ErrorOnBooleanDataType(t *testing.T) {
	// Test that we get an error when trying to align boolean data types
	// We need at least 2 records in different alignment periods where the second doesn't
	// fall exactly on the boundary, to trigger interpolation
	fieldsMeta := []tsquery.FieldMeta{}

	// Create field metadata with a boolean field
	fm, err := tsquery.NewFieldMeta("field_0", tsquery.DataTypeBoolean, false)
	require.NoError(t, err)
	fieldsMeta = append(fieldsMeta, *fm)

	records := []tsquery.Record{
		{Value: []any{true}, Timestamp: time.Unix(10, 0)},   // 00:00:10
		{Value: []any{false}, Timestamp: time.Unix(105, 0)}, // 00:01:45 (not on boundary, will trigger interpolation)
	}

	// Create input Result
	inputStream := stream.Just(records...)
	inputResult := tsquery.NewResult(fieldsMeta, inputStream)

	// Create the aligner filter
	alignerFilter := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(time.Minute, time.Local))

	// Apply the filter
	outputResult, err := alignerFilter.Filter(*inputResult)
	require.NoError(t, err) // Filter itself doesn't error, the error occurs during stream consumption

	// Try to collect the results - this should trigger the error when interpolation is attempted
	ctx := context.Background()
	_, err = outputResult.Stream().Collect(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported data type")
}

// --- Test Helper Functions ---

// testAlignerFilterAsExpected runs the AlignerFilter.Filter method and asserts the output.
func testAlignerFilterAsExpected(
	t *testing.T,
	fixedDuration time.Duration,
	records []tsquery.Record,
	expected []tsquery.Record,
) {
	t.Helper() // Marks this function as a test helper

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
	inputResult := tsquery.NewResult(fieldsMeta, inputStream)

	// Create the aligner filter
	alignerFilter := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(fixedDuration, time.Local))

	// Apply the filter
	outputResult, err := alignerFilter.Filter(*inputResult)
	require.NoError(t, err)

	// Collect the resulting aligned records
	result := outputResult.Stream().MustCollect() // Use MustCollect for simplicity in tests

	// Assert the results
	// Compare timestamps first for easier debugging if lengths differ
	require.EqualValues(t,
		mapSlice(expected, func(r tsquery.Record) time.Time { return r.Timestamp }),
		mapSlice(result, func(r tsquery.Record) time.Time { return r.Timestamp }),
		"Timestamps mismatch",
	)

	// Compare values
	require.EqualValues(t,
		mapSlice(expected, func(r tsquery.Record) []any { return r.Value }),
		mapSlice(result, func(r tsquery.Record) []any { return r.Value }),
		"Values mismatch",
	)

	// Explicit length check can also be helpful
	require.Len(t, result, len(expected), "Number of resulting records mismatch")
}

// testAlignerFilterWithFieldMeta runs the AlignerFilter.Filter method with custom field metadata and asserts the output.
func testAlignerFilterWithFieldMeta(
	t *testing.T,
	fixedDuration time.Duration,
	dataTypes []tsquery.DataType,
	records []tsquery.Record,
	expected []tsquery.Record,
) {
	t.Helper() // Marks this function as a test helper

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
	inputResult := tsquery.NewResult(fieldsMeta, inputStream)

	// Create the aligner filter
	alignerFilter := NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(fixedDuration, time.Local))

	// Apply the filter
	outputResult, err := alignerFilter.Filter(*inputResult)
	require.NoError(t, err)

	// Collect the resulting aligned records
	result := outputResult.Stream().MustCollect() // Use MustCollect for simplicity in tests

	// Assert the results
	// Compare timestamps first for easier debugging if lengths differ
	require.EqualValues(t,
		mapSlice(expected, func(r tsquery.Record) time.Time { return r.Timestamp }),
		mapSlice(result, func(r tsquery.Record) time.Time { return r.Timestamp }),
		"Timestamps mismatch",
	)

	// Compare values
	require.EqualValues(t,
		mapSlice(expected, func(r tsquery.Record) []any { return r.Value }),
		mapSlice(result, func(r tsquery.Record) []any { return r.Value }),
		"Values mismatch",
	)

	// Explicit length check can also be helpful
	require.Len(t, result, len(expected), "Number of resulting records mismatch")
}

// mapSlice is a helper to transform slices
func mapSlice[A any, B any](input []A, m func(a A) B) []B {
	ret := make([]B, len(input))
	for i, currElem := range input {
		ret[i] = m(currElem)
	}
	return ret
}
