package timeseries

import (
	"github.com/shpandrak/shpanstream"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCollectDeltaStream_Aligned(t *testing.T) {
	testAsExpected(
		t,
		time.Minute,
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: 150, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: 200, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 250, Timestamp: time.Unix(180, 0)}, // 00:03:00
			{Value: 300, Timestamp: time.Unix(240, 0)}, // 00:04:00
			{Value: 350, Timestamp: time.Unix(300, 0)}, // 00:05:00
		},
		[]TsRecord[int64]{
			{Value: 50, Timestamp: time.Unix(60, 0)},  // 00:01:00 delta: 150 - 100
			{Value: 50, Timestamp: time.Unix(120, 0)}, // 00:02:00 delta: 200 - 150
			{Value: 50, Timestamp: time.Unix(180, 0)}, // 00:03:00 delta: 250 - 200
			{Value: 50, Timestamp: time.Unix(240, 0)}, // 00:04:00 delta: 300 - 250
			{Value: 50, Timestamp: time.Unix(300, 0)}, // 00:05:00 delta: 350 - 300
		})
}

func TestCollectDeltaStream_AlignedExpanded(t *testing.T) {
	testAsExpected(
		t,
		2*time.Minute,
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(0, 0)},   // 00:00:00
			{Value: 150, Timestamp: time.Unix(60, 0)},  // 00:01:00
			{Value: 200, Timestamp: time.Unix(120, 0)}, // 00:02:00
			{Value: 250, Timestamp: time.Unix(180, 0)}, // 00:03:00
			{Value: 300, Timestamp: time.Unix(240, 0)}, // 00:04:00
			{Value: 350, Timestamp: time.Unix(300, 0)}, // 00:05:00
		},
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(120, 0)}, // 00:02:00 delta: 200 - 150
			{Value: 100, Timestamp: time.Unix(240, 0)}, // 00:04:00 delta: 300 - 250
			{Value: 50, Timestamp: time.Unix(360, 0)},  // 00:05:00 delta: 350 - 300
		})
}

func TestCollectDeltaStream_NonAligned(t *testing.T) {
	// Test data: a series of timestamped records where the timestamps don't align exactly to the slot boundaries
	// Define the expected deltas
	testAsExpected(
		t,
		time.Minute,
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(45, 0)},  // 00:00:45
			{Value: 200, Timestamp: time.Unix(105, 0)}, // 00:01:45
			{Value: 300, Timestamp: time.Unix(165, 0)}, // 00:02:45
			{Value: 400, Timestamp: time.Unix(225, 0)}, // 00:03:45
		},
		[]TsRecord[int64]{
			// We expect the weighted average around 1 minute, then 2 minutes, etc.
			{Value: 25, Timestamp: time.Unix(60, 0)},   // 00:01:00 (delta based on interpolation between 00:00:45 and 00:01:45)
			{Value: 100, Timestamp: time.Unix(120, 0)}, // 00:02:00 (delta between 00:01:45 and 00:02:45)
			{Value: 100, Timestamp: time.Unix(180, 0)}, // 00:03:00 (delta between 00:02:45 and 00:03:45)
			{Value: 75, Timestamp: time.Unix(240, 0)},  // 00:03:00 (delta between 00:03:45 and 00:04:00)
		},
	)

}

func TestCollectDeltaStream_SingleItem(t *testing.T) {

	testAsExpected(
		t,
		time.Minute,
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(45, 0)}, // 00:00:45
		},
		[]TsRecord[int64]{},
	)
}

func TestCollectDeltaStream_TwoItems(t *testing.T) {
	// Test data: a series of timestamped records where the timestamps don't align exactly to the slot boundaries
	// Define the expected deltas
	testAsExpected(
		t,
		time.Minute,
		[]TsRecord[int64]{
			{Value: 100, Timestamp: time.Unix(45, 0)},  // 00:00:45
			{Value: 200, Timestamp: time.Unix(105, 0)}, // 00:01:45
		},
		[]TsRecord[int64]{
			// We expect the weighted average around 1 minute, then 2 minutes, etc.
			{Value: 25, Timestamp: time.Unix(60, 0)},  // 00:01:00 (delta based on interpolation between 00:00:45 and 00:01:45)
			{Value: 75, Timestamp: time.Unix(120, 0)}, // 00:02:00 (delta between 00:01:45 and 00:02:45)
		},
	)
}

func TestCollectDeltaStream_EmptyStaysEmpty(t *testing.T) {
	testAsExpected(
		t,
		time.Minute,
		[]TsRecord[int64]{},
		[]TsRecord[int64]{},
	)
}

func testAsExpected(
	t *testing.T,
	fixedDuration time.Duration,
	records []TsRecord[int64],
	expected []TsRecord[int64],
) {
	// Use Just to create the input stream
	inputStream := shpanstream.Just(records...)

	// Create the delta stream using the CreateAlignedDeltaStream function
	deltaStream := AlignDeltaStream[int64](inputStream, NewFixedAlignmentPeriod(fixedDuration, time.Local))

	// Collect the resulting delta records
	result := deltaStream.MustCollect()

	// Assert the results
	require.EqualValues(t,
		mapSlice(expected, func(r TsRecord[int64]) time.Time { return r.Timestamp }),
		mapSlice(result, func(r TsRecord[int64]) time.Time { return r.Timestamp }),
	)
	require.EqualValues(t,
		mapSlice(expected, func(r TsRecord[int64]) int64 { return r.Value }),
		mapSlice(result, func(r TsRecord[int64]) int64 { return r.Value }),
	)
}

func mapSlice[A any, B any](input []A, m func(a A) B) []B {
	ret := make([]B, len(input))
	for i, currElem := range input {
		ret[i] = m(currElem)
	}
	return ret
}
