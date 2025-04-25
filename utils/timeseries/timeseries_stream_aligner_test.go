package timeseries

import (
	"testing"
	"time"

	"github.com/shpandrak/shpanstream" // Assuming this is your stream library path
	"github.com/stretchr/testify/require"
)

// --- Test Cases for AlignStream ---

func TestAlignStream_Aligned(t *testing.T) {
	// Input data points fall exactly on the minute boundaries.
	// Expected output should be the same points, as no interpolation is needed.
	testAlignAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: 150, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: 200, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 250, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: 150, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: 200, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 250, Timestamp: time.Unix(180, 0)}, // 00:03:00
		},
	)
}

func TestAlignStream_AlignedExpanded(t *testing.T) {
	// Input data points fall exactly on minute boundaries.
	// Align to every 2 minutes. Expect output only at 0, 120, 240 seconds.
	testAlignAsExpected(
		t,
		2*time.Minute, // Align to every 2 minutes
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: 150, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: 200, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 250, Timestamp: time.Unix(180, 0)}, // 00:03:00
			{Value: 300, Timestamp: time.Unix(240, 0)}, // 00:04:00
			{Value: 350, Timestamp: time.Unix(300, 0)}, // 00:05:00
		},
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)},   // 00:00:00 (exact match)
			{Value: 200, Timestamp: time.Unix(120, 0)}, // 00:02:00 (exact match)
			{Value: 300, Timestamp: time.Unix(240, 0)}, // 00:04:00 (exact match)
			// Note: We don't expect an aligned point at 360s (00:06:00) because
			// there's no data point after 300s to interpolate with.
			// If extrapolation is desired, this expectation would change.
		},
	)
}

func TestAlignStream_NonAligned(t *testing.T) {
	// Input data points are not aligned with minute boundaries.
	// Expect interpolation to calculate values at 60, 120, 180 seconds.
	testAlignAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[int64]{
			// t=45, v=100
			{Value: 100, Timestamp: time.Unix(45, 0)}, // 00:00:45
			// t=105, v=200 (Interval: 60s, Right delta: 100)
			{Value: 200, Timestamp: time.Unix(105, 0)}, // 00:01:45
			// t=165, v=300 (Interval: 60s, Right delta: 100)
			{Value: 300, Timestamp: time.Unix(165, 0)}, // 00:02:45
			// t=225, v=400 (Interval: 60s, Right delta: 100)
			{Value: 400, Timestamp: time.Unix(225, 0)}, // 00:03:45
		},
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)}, // 00:00:00
			// Target t=60: Between t=45 (100) and t=105 (200).
			// 60s is 15s after 45s. Total interval 105-45=60s.
			// Interpolation: 100 + (200-100) * (60-45)/(105-45) = 100 + 100 * 15/60 = 100 + 25 = 125
			{Value: 125, Timestamp: time.Unix(60, 0)}, // 00:01:00

			// Target t=120: Between t=105 (200) and t=165 (300).
			// 120s is 15s after 105s. Total interval 165-105=60s.
			// Interpolation: 200 + (300-200) * (120-105)/(165-105) = 200 + 100 * 15/60 = 200 + 25 = 225
			{Value: 225, Timestamp: time.Unix(120, 0)}, // 00:02:00

			// Target t=180: Between t=165 (300) and t=225 (400).
			// 180s is 15s after 165s. Total interval 225-165=60s.
			// Interpolation: 300 + (400-300) * (180-165)/(225-165) = 300 + 100 * 15/60 = 300 + 25 = 325
			{Value: 325, Timestamp: time.Unix(180, 0)}, // 00:03:00

			// Target t=240: No data point after t=225. Cannot interpolate.
			// Expect no record for t=240s unless extrapolation is implemented.
		},
	)
}

func TestAlignStream_SingleItem(t *testing.T) {
	// Only one data point. Cannot interpolate to find values at alignment points.
	testAlignAsExpected(
		t,
		time.Minute,
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(45, 0)}, // 00:00:45
		},
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)}, // 00:00:00
		},
	)
}

func TestAlignStream_TwoItems(t *testing.T) {
	// Two data points allow for interpolation between them.
	testAlignAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(45, 0)},  // 00:00:45
			{Value: 200, Timestamp: time.Unix(105, 0)}, // 00:01:45
		},
		[]TsRecord[int64]{
			// Target t=60: Between t=45 (100) and t=105 (200). Right is 125 (as calculated in NonAligned test).
			{Value: 100, Timestamp: time.Unix(0, 0)},  // 00:00:00
			{Value: 125, Timestamp: time.Unix(60, 0)}, // 00:01:00

			// Target t=120: No data point after t=105. Cannot interpolate.
		},
	)
}

func TestAlignStream_ExactMatchSinglePoint(t *testing.T) {
	// A single data point that falls exactly on an alignment boundary.
	testAlignAsExpected(
		t,
		time.Minute,
		[]TsRecord[int64]{
			{Value: 150, Timestamp: time.Unix(60, 0)}, // 00:01:00
		},
		[]TsRecord[int64]{
			// Since the point matches the boundary exactly, it should be output.
			{Value: 150, Timestamp: time.Unix(60, 0)}, // 00:01:00
			// No other points can be generated.
		},
	)
}

func TestAlignStream_MultiplePointsWithinInterval(t *testing.T) {
	// Multiple points exist between alignment boundaries.
	// Interpolation should use the points immediately surrounding the target time.
	testAlignAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(10, 0)},  // 00:00:10
			{Value: 120, Timestamp: time.Unix(30, 0)},  // 00:00:30
			{Value: 140, Timestamp: time.Unix(50, 0)},  // 00:00:50 <- Point before t=60
			{Value: 200, Timestamp: time.Unix(70, 0)},  // 00:01:10 <- Point after t=60
			{Value: 220, Timestamp: time.Unix(90, 0)},  // 00:01:30
			{Value: 240, Timestamp: time.Unix(110, 0)}, // 00:01:50 <- Point before t=120
			{Value: 300, Timestamp: time.Unix(130, 0)}, // 00:02:10 <- Point after t=120
		},
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)}, // 00:00:00 smeared first item

			// Target t=60: Between t=50 (140) and t=70 (200).
			// 60s is 10s after 50s. Total interval 70-50=20s.
			// Interpolation: 140 + (200-140) * (60-50)/(70-50) = 140 + 60 * 10/20 = 140 + 30 = 170
			{Value: 170, Timestamp: time.Unix(60, 0)}, // 00:01:00

			// Target t=120: Between t=110 (240) and t=130 (300).
			// 120s is 10s after 110s. Total interval 130-110=20s.
			// Interpolation: 240 + (300-240) * (120-110)/(130-110) = 240 + 60 * 10/20 = 240 + 30 = 270
			{Value: 270, Timestamp: time.Unix(120, 0)}, // 00:02:00
		},
	)
}

func TestAlignStream_EmptyStaysEmpty(t *testing.T) {
	// Empty input stream should result in an empty output stream.
	testAlignAsExpected(
		t,
		time.Minute,
		[]TsRecord[int64]{}, // Empty input
		[]TsRecord[int64]{}, // Empty expected output
	)
}

// --- Test Helper Functions ---

// testAlignAsExpected runs the AlignStream function and asserts the output.
func testAlignAsExpected[N Number](
	t *testing.T,
	fixedDuration time.Duration,
	records []TsRecord[N],
	expected []TsRecord[N],
) {
	t.Helper() // Marks this function as a test helper

	// Use Just to create the input stream
	inputStream := shpanstream.Just(records...)

	alignedStream := AlignStream[N](inputStream, fixedDuration)

	// Collect the resulting aligned records
	result := alignedStream.MustCollect() // Use MustCollect for simplicity in tests

	// Assert the results
	// Compare timestamps first for easier debugging if lengths differ
	require.EqualValues(t,
		mapSlice(expected, func(r TsRecord[N]) time.Time { return r.Timestamp }),
		mapSlice(result, func(r TsRecord[N]) time.Time { return r.Timestamp }),
		"Timestamps mismatch",
	)

	// Compare values
	require.EqualValues(t,
		mapSlice(expected, func(r TsRecord[N]) N { return r.Value }),
		mapSlice(result, func(r TsRecord[N]) N { return r.Value }),
		"Values mismatch",
	)

	// Explicit length check can also be helpful
	require.Len(t, result, len(expected), "Number of resulting records mismatch")
}

// TestAlignStream_UnevenTemporal tests with unevenly spaced input points
func TestAlignStream_UnevenTemporal(t *testing.T) {
	// Input data points have varying intervals between them
	testAlignAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(10, 0)},  // 00:00:10
			{Value: 200, Timestamp: time.Unix(150, 0)}, // 00:02:30 (big gap)
			{Value: 300, Timestamp: time.Unix(170, 0)}, // 00:02:50 (small gap)
			{Value: 400, Timestamp: time.Unix(300, 0)}, // 00:05:00 (medium gap)
		},
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)},   // 00:00:00 (smeared)
			{Value: 178, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 400, Timestamp: time.Unix(300, 0)}, // 00:05:00
		},
	)
}

// TestAlignStream_LargeGaps tests with large time gaps between points
func TestAlignStream_LargeGaps(t *testing.T) {
	// Test with large gaps between data points (hours)
	testAlignAsExpected(
		t,
		time.Minute, // Still align to every minute
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)},    // 00:00:00
			{Value: 200, Timestamp: time.Unix(3600, 0)}, // 01:00:00 (1 hour later)
			{Value: 300, Timestamp: time.Unix(7200, 0)}, // 02:00:00 (1 hour later)
		},
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)},    // 00:00:00
			{Value: 200, Timestamp: time.Unix(3600, 0)}, // 01:00:00 (1 hour later)
			{Value: 300, Timestamp: time.Unix(7200, 0)}, // 02:00:00 (1 hour later)
		},
	)
}

// TestAlignStream_PointExactlyAtBoundary tests special case of start point on boundary
func TestAlignStream_PointExactlyAtBoundary(t *testing.T) {
	// Test with points exactly at alignment boundaries
	testAlignAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)},  // 00:00:00 (exactly at boundary)
			{Value: 200, Timestamp: time.Unix(45, 0)}, // 00:00:45 (not at boundary)
			{Value: 300, Timestamp: time.Unix(60, 0)}, // 00:01:00 (exactly at boundary)
			{Value: 400, Timestamp: time.Unix(75, 0)}, // 00:01:15 (not at boundary)
		},
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)},  // 00:00:00 (preserved exactly)
			{Value: 300, Timestamp: time.Unix(60, 0)}, // 00:01:00 (preserved exactly)
		},
	)
}

// TestAlignStream_FloatingPointValues tests with floating point values
func TestAlignStream_FloatingPointValues(t *testing.T) {
	// Test with floating point values to ensure precision in interpolation
	testAlignAsExpected(
		t,
		time.Minute, // Align to every minute
		[]TsRecord[float64]{
			{Value: 100.5, Timestamp: time.Unix(45, 0)},   // 00:00:45
			{Value: 200.75, Timestamp: time.Unix(105, 0)}, // 00:01:45
		},
		[]TsRecord[float64]{
			{Value: 100.5, Timestamp: time.Unix(0, 0)}, // 00:00:00 (smeared)
			// t=60: Between 45 (100.5) and 105 (200.75)
			// Interpolation: 100.5 + (200.75-100.5)*(60-45)/(105-45) = 100.5 + 100.25*15/60 = 100.5 + 25.0625 = 125.5625
			{Value: 125.5625, Timestamp: time.Unix(60, 0)}, // 00:01:00
		},
	)
}

// TestAlignStream_NonIntegerAlignment tests with non-integer alignment duration
func TestAlignStream_NonIntegerAlignment(t *testing.T) {
	// Test with a non-standard alignment period (e.g., 90 seconds)
	testAlignAsExpected(
		t,
		90*time.Second, // Align to every 1.5 minutes
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(30, 0)},  // 00:00:30
			{Value: 200, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 300, Timestamp: time.Unix(210, 0)}, // 00:03:30
		},
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)}, // 00:00:00 (smeared)
			// t=90: Between 30 (100) and 120 (200)
			// Interpolation: 100 + (200-100)*(90-30)/(120-30) = 100 + 100*60/90 = 100 + 66.67 = 166.67 ≈ 167 rounded down to 166
			{Value: 166, Timestamp: time.Unix(90, 0)}, // 00:01:30
			// t=180: Between 120 (200) and 210 (300)
			// Interpolation: 200 + (300-200)*(180-120)/(210-120) = 200 + 100*60/90 = 200 + 66.67 = 266.67 ≈ 267 rounded down to 266
			{Value: 266, Timestamp: time.Unix(180, 0)}, // 00:03:00
			// t=270: Would be beyond the last data point, no output expected.
		},
	)
}
