package timeseries

import (
	"github.com/shpandrak/shpanstream/stream"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- Test Cases for AlignStreamUntyped ---

func TestAlignStreamUntyped_Aligned(t *testing.T) {
	// Input data points fall exactly on the minute boundaries.
	// Expected output should be the same points, as no interpolation is needed.
	testAlignUntypedAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[[]any]{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: []any{150.0, 20.0, 1500.0}, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: []any{200.0, 30.0, 2000.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: []any{250.0, 40.0, 2500.0}, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
		[]TsRecord[[]any]{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: []any{150.0, 20.0, 1500.0}, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: []any{200.0, 30.0, 2000.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: []any{250.0, 40.0, 2500.0}, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
	)
}

func TestAlignStreamUntyped_AlignedExpanded(t *testing.T) {
	// Input data points fall exactly on minute boundaries.
	// Align to every 2 minutes. Expect output only at 0, 120, 240 seconds.
	testAlignUntypedAsExpected(
		t,
		2*time.Minute, // Align to every 2 minutes
		[]TsRecord[[]any]{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: []any{150.0, 20.0, 1500.0}, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: []any{200.0, 30.0, 2000.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: []any{250.0, 40.0, 2500.0}, Timestamp: time.Unix(180, 0)}, // 00:03:00
			{Value: []any{300.0, 50.0, 3000.0}, Timestamp: time.Unix(240, 0)}, // 00:04:00
			{Value: []any{350.0, 60.0, 3500.0}, Timestamp: time.Unix(300, 0)}, // 00:05:00
		},
		[]TsRecord[[]any]{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},   // 00:00:00 (exact match)
			{Value: []any{200.0, 30.0, 2000.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00 (exact match)
			{Value: []any{300.0, 50.0, 3000.0}, Timestamp: time.Unix(240, 0)}, // 00:04:00 (exact match)
		},
	)
}

func TestAlignStreamUntyped_NonAligned(t *testing.T) {
	// Input data points are not aligned with minute boundaries.
	// Expect interpolation to calculate values at 60, 120, 180 seconds.
	// Interpolation at t=60: Between t=45 (100, 10, 1000) and t=105 (200, 40, 2000).
	// 60s is 15s after 45s. Total interval 105-45=60s. Weight = 15/60 = 0.25
	// arr[0]: 100 + (200-100) * 0.25 = 100 + 25 = 125
	// arr[1]: 10 + (40-10) * 0.25 = 10 + 7.5 = 17.5
	// arr[2]: 1000 + (2000-1000) * 0.25 = 1000 + 250 = 1250
	testAlignUntypedAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[[]any]{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(45, 0)},   // 00:00:45
			{Value: []any{200.0, 40.0, 2000.0}, Timestamp: time.Unix(105, 0)},  // 00:01:45
			{Value: []any{300.0, 70.0, 3000.0}, Timestamp: time.Unix(165, 0)},  // 00:02:45
			{Value: []any{400.0, 100.0, 4000.0}, Timestamp: time.Unix(225, 0)}, // 00:03:45
		},
		[]TsRecord[[]any]{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},   // 00:00:00 (smeared)
			{Value: []any{125.0, 17.5, 1250.0}, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: []any{225.0, 47.5, 2250.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: []any{325.0, 77.5, 3250.0}, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
	)
}

func TestAlignStreamUntyped_SingleItem(t *testing.T) {
	// Only one data point. Cannot interpolate to find values at alignment points.
	testAlignUntypedAsExpected(
		t,
		time.Minute,
		[]TsRecord[[]any]{
			{Value: []any{100.0, 50.0, 2000.0}, Timestamp: time.Unix(45, 0)}, // 00:00:45
		},
		[]TsRecord[[]any]{
			{Value: []any{100.0, 50.0, 2000.0}, Timestamp: time.Unix(0, 0)}, // 00:00:00 (smeared)
		},
	)
}

func TestAlignStreamUntyped_TwoItems(t *testing.T) {
	// Two data points allow for interpolation between them.
	// Interpolation at t=60: Between t=45 and t=105 (60s interval), 15s after 45s. Weight = 0.25
	// arr[0]: 100 + (200-100) * 0.25 = 125
	// arr[1]: 20 + (80-20) * 0.25 = 20 + 15 = 35
	// arr[2]: 500 + (1500-500) * 0.25 = 500 + 250 = 750
	testAlignUntypedAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[[]any]{
			{Value: []any{100.0, 20.0, 500.0}, Timestamp: time.Unix(45, 0)},   // 00:00:45
			{Value: []any{200.0, 80.0, 1500.0}, Timestamp: time.Unix(105, 0)}, // 00:01:45
		},
		[]TsRecord[[]any]{
			{Value: []any{100.0, 20.0, 500.0}, Timestamp: time.Unix(0, 0)},  // 00:00:00 (smeared)
			{Value: []any{125.0, 35.0, 750.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00
		},
	)
}

func TestAlignStreamUntyped_ExactMatchSinglePoint(t *testing.T) {
	// A single data point that falls exactly on an alignment boundary.
	testAlignUntypedAsExpected(
		t,
		time.Minute,
		[]TsRecord[[]any]{
			{Value: []any{150.0, 75.0, 3000.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00
		},
		[]TsRecord[[]any]{
			{Value: []any{150.0, 75.0, 3000.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00
		},
	)
}

func TestAlignStreamUntyped_MultiplePointsWithinInterval(t *testing.T) {
	// Multiple points exist between alignment boundaries.
	// At t=60: Between t=50 (140, 14, 1400) and t=70 (200, 20, 2000). Weight = 10/20 = 0.5
	// arr[0]: 140 + (200-140) * 0.5 = 140 + 30 = 170
	// arr[1]: 14 + (20-14) * 0.5 = 14 + 3 = 17
	// arr[2]: 1400 + (2000-1400) * 0.5 = 1400 + 300 = 1700
	// At t=120: Between t=110 (240, 24, 2400) and t=130 (300, 30, 3000). Weight = 10/20 = 0.5
	// arr[0]: 240 + (300-240) * 0.5 = 240 + 30 = 270
	// arr[1]: 24 + (30-24) * 0.5 = 24 + 3 = 27
	// arr[2]: 2400 + (3000-2400) * 0.5 = 2400 + 300 = 2700
	testAlignUntypedAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[[]any]{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(10, 0)},  // 00:00:10
			{Value: []any{120.0, 12.0, 1200.0}, Timestamp: time.Unix(30, 0)},  // 00:00:30
			{Value: []any{140.0, 14.0, 1400.0}, Timestamp: time.Unix(50, 0)},  // 00:00:50
			{Value: []any{200.0, 20.0, 2000.0}, Timestamp: time.Unix(70, 0)},  // 00:01:10
			{Value: []any{220.0, 22.0, 2200.0}, Timestamp: time.Unix(90, 0)},  // 00:01:30
			{Value: []any{240.0, 24.0, 2400.0}, Timestamp: time.Unix(110, 0)}, // 00:01:50
			{Value: []any{300.0, 30.0, 3000.0}, Timestamp: time.Unix(130, 0)}, // 00:02:10
		},
		[]TsRecord[[]any]{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},   // 00:00:00 smeared first item
			{Value: []any{170.0, 17.0, 1700.0}, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: []any{270.0, 27.0, 2700.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00
		},
	)
}

func TestAlignStreamUntyped_EmptyStaysEmpty(t *testing.T) {
	// Empty input stream should result in an empty output stream.
	testAlignUntypedAsExpected(
		t,
		time.Minute,
		[]TsRecord[[]any]{}, // Empty input
		[]TsRecord[[]any]{}, // Empty expected output
	)
}

func TestAlignStreamUntyped_UnevenTemporal(t *testing.T) {
	// Input data points have varying intervals between them
	// At t=120: Between t=10 (100, 10, 1000) and t=150 (200, 30, 3000). Weight = 110/140
	// arr[0]: 100 + (200-100) * 110/140 = 100 + 78.571... = 178.571...
	// arr[1]: 10 + (30-10) * 110/140 = 10 + 15.714... = 25.714...
	// arr[2]: 1000 + (3000-1000) * 110/140 = 1000 + 1571.428... = 2571.428...
	testAlignUntypedAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[[]any]{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(10, 0)},  // 00:00:10
			{Value: []any{200.0, 30.0, 3000.0}, Timestamp: time.Unix(150, 0)}, // 00:02:30 (big gap)
			{Value: []any{300.0, 50.0, 5000.0}, Timestamp: time.Unix(170, 0)}, // 00:02:50 (small gap)
			{Value: []any{400.0, 70.0, 7000.0}, Timestamp: time.Unix(300, 0)}, // 00:05:00 (medium gap)
		},
		[]TsRecord[[]any]{
			{Value: []any{100.0, 10.0, 1000.0}, Timestamp: time.Unix(0, 0)},                                          // 00:00:00 (smeared)
			{Value: []any{178.57142857142858, 25.714285714285715, 2571.4285714285716}, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: []any{400.0, 70.0, 7000.0}, Timestamp: time.Unix(300, 0)},                                        // 00:05:00
		},
	)
}

func TestAlignStreamUntyped_LargeGaps(t *testing.T) {
	// Test with large gaps between data points (hours)
	testAlignUntypedAsExpected(
		t,
		time.Minute, // Still align to every minute
		[]TsRecord[[]any]{
			{Value: []any{100.0, 50.0, 10.0}, Timestamp: time.Unix(0, 0)},     // 00:00:00
			{Value: []any{200.0, 150.0, 20.0}, Timestamp: time.Unix(3600, 0)}, // 01:00:00 (1 hour later)
			{Value: []any{300.0, 250.0, 30.0}, Timestamp: time.Unix(7200, 0)}, // 02:00:00 (1 hour later)
		},
		[]TsRecord[[]any]{
			{Value: []any{100.0, 50.0, 10.0}, Timestamp: time.Unix(0, 0)},     // 00:00:00
			{Value: []any{200.0, 150.0, 20.0}, Timestamp: time.Unix(3600, 0)}, // 01:00:00 (1 hour later)
			{Value: []any{300.0, 250.0, 30.0}, Timestamp: time.Unix(7200, 0)}, // 02:00:00 (1 hour later)
		},
	)
}

func TestAlignStreamUntyped_PointExactlyAtBoundary(t *testing.T) {
	// Test with points exactly at alignment boundaries
	testAlignUntypedAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[[]any]{
			{Value: []any{100.0, 25.0, 500.0}, Timestamp: time.Unix(0, 0)},    // 00:00:00 (exactly at boundary)
			{Value: []any{200.0, 75.0, 1500.0}, Timestamp: time.Unix(45, 0)},  // 00:00:45 (not at boundary)
			{Value: []any{300.0, 125.0, 2500.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00 (exactly at boundary)
			{Value: []any{400.0, 175.0, 3500.0}, Timestamp: time.Unix(75, 0)}, // 00:01:15 (not at boundary)
		},
		[]TsRecord[[]any]{
			{Value: []any{100.0, 25.0, 500.0}, Timestamp: time.Unix(0, 0)},    // 00:00:00 (preserved exactly)
			{Value: []any{300.0, 125.0, 2500.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00 (preserved exactly)
		},
	)
}

func TestAlignStreamUntyped_MultipleValues(t *testing.T) {
	// Test with multiple values in the array
	testAlignUntypedAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[[]any]{
			{Value: []any{100.0, 50.0}, Timestamp: time.Unix(45, 0)},   // 00:00:45
			{Value: []any{200.0, 150.0}, Timestamp: time.Unix(105, 0)}, // 00:01:45
		},
		[]TsRecord[[]any]{
			{Value: []any{100.0, 50.0}, Timestamp: time.Unix(0, 0)},  // 00:00:00 (smeared)
			{Value: []any{125.0, 75.0}, Timestamp: time.Unix(60, 0)}, // 00:01:00 (interpolated)
		},
	)
}

func TestAlignStreamUntyped_NonIntegerAlignment(t *testing.T) {
	// Test with a non-standard alignment period (e.g., 90 seconds)
	// At t=90: Between t=30 (100, 20, 500) and t=120 (200, 50, 1100). Weight = 60/90 = 2/3
	// arr[0]: 100 + (200-100) * 2/3 = 100 + 66.666... = 166.666...
	// arr[1]: 20 + (50-20) * 2/3 = 20 + 20 = 40
	// arr[2]: 500 + (1100-500) * 2/3 = 500 + 400 = 900
	// At t=180: Between t=120 (200, 50, 1100) and t=210 (300, 80, 1700). Weight = 60/90 = 2/3
	// arr[0]: 200 + (300-200) * 2/3 = 200 + 66.666... = 266.666...
	// arr[1]: 50 + (80-50) * 2/3 = 50 + 20 = 70
	// arr[2]: 1100 + (1700-1100) * 2/3 = 1100 + 400 = 1500
	testAlignUntypedAsExpected(
		t,
		90*time.Second, // Align to every 1.5 minutes
		[]TsRecord[[]any]{
			{Value: []any{100.0, 20.0, 500.0}, Timestamp: time.Unix(30, 0)},   // 00:00:30
			{Value: []any{200.0, 50.0, 1100.0}, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: []any{300.0, 80.0, 1700.0}, Timestamp: time.Unix(210, 0)}, // 00:03:30
		},
		[]TsRecord[[]any]{
			{Value: []any{100.0, 20.0, 500.0}, Timestamp: time.Unix(0, 0)},                // 00:00:00 (smeared)
			{Value: []any{166.66666666666666, 40.0, 900.0}, Timestamp: time.Unix(90, 0)},  // 00:01:30
			{Value: []any{266.6666666666667, 70.0, 1500.0}, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
	)
}

// --- Test Helper Functions ---

// testAlignUntypedAsExpected runs the AlignStreamUntyped function and asserts the output.
func testAlignUntypedAsExpected(
	t *testing.T,
	fixedDuration time.Duration,
	records []TsRecord[[]any],
	expected []TsRecord[[]any],
) {
	t.Helper() // Marks this function as a test helper

	// Use Just to create the input stream
	inputStream := stream.Just(records...)

	alignedStream := AlignStreamUntyped(inputStream, NewFixedAlignmentPeriod(fixedDuration, time.Local))

	// Collect the resulting aligned records
	result := alignedStream.MustCollect() // Use MustCollect for simplicity in tests

	// Assert the results
	// Compare timestamps first for easier debugging if lengths differ
	require.EqualValues(t,
		mapSliceUntyped(expected, func(r TsRecord[[]any]) time.Time { return r.Timestamp }),
		mapSliceUntyped(result, func(r TsRecord[[]any]) time.Time { return r.Timestamp }),
		"Timestamps mismatch",
	)

	// Compare values
	require.EqualValues(t,
		mapSliceUntyped(expected, func(r TsRecord[[]any]) []any { return r.Value }),
		mapSliceUntyped(result, func(r TsRecord[[]any]) []any { return r.Value }),
		"Values mismatch",
	)

	// Explicit length check can also be helpful
	require.Len(t, result, len(expected), "Number of resulting records mismatch")
}

// mapSliceUntyped is a helper to transform slices
func mapSliceUntyped[A any, B any](input []A, m func(a A) B) []B {
	ret := make([]B, len(input))
	for i, currElem := range input {
		ret[i] = m(currElem)
	}
	return ret
}
