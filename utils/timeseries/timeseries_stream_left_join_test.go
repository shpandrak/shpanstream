package timeseries

import (
	"github.com/shpandrak/shpanstream/stream"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Helper function to create times in a specific location
func unixInLoc(sec int64, loc *time.Location) time.Time {
	return time.Unix(sec, 0).In(loc)
}

func TestLeftJoinStreams_TwoStreams_FullMatch(t *testing.T) {
	// Two streams with matching timestamps
	loc := time.UTC
	leftStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 10},
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 20},
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 30},
	)

	rightStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 100},
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 200},
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 300},
	)

	// Joiner that sums left + first other value
	joiner := func(left int, others []*int) int {
		if len(others) > 0 && others[0] != nil {
			return left + *others[0]
		}
		return left
	}

	result := LeftJoinStreams(
		[]stream.Stream[TsRecord[int]]{leftStream, rightStream},
		joiner,
	).MustCollect()

	expected := []TsRecord[int]{
		{Timestamp: unixInLoc(60, loc), Value: 110},
		{Timestamp: unixInLoc(120, loc), Value: 220},
		{Timestamp: unixInLoc(180, loc), Value: 330},
	}

	require.Equal(t, expected, result)
}

func TestLeftJoinStreams_TwoStreams_PartialMatch(t *testing.T) {
	// Left stream has more timestamps than right stream
	loc := time.UTC
	leftStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 10},
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 20},
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 30},
		TsRecord[int]{Timestamp: unixInLoc(240, loc), Value: 40},
	)

	rightStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 100},
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 300},
	)

	// Joiner that sums left + first other value (or just left if no match)
	joiner := func(left int, others []*int) int {
		if len(others) > 0 && others[0] != nil {
			return left + *others[0]
		}
		return left
	}

	result := LeftJoinStreams(
		[]stream.Stream[TsRecord[int]]{leftStream, rightStream},
		joiner,
	).MustCollect()

	expected := []TsRecord[int]{
		{Timestamp: unixInLoc(60, loc), Value: 110},  // 10 + 100
		{Timestamp: unixInLoc(120, loc), Value: 20},  // 20 (no match)
		{Timestamp: unixInLoc(180, loc), Value: 330}, // 30 + 300
		{Timestamp: unixInLoc(240, loc), Value: 40},  // 40 (no match)
	}

	require.Equal(t, expected, result)
}

func TestLeftJoinStreams_ThreeStreams_MixedMatches(t *testing.T) {
	// Three streams with various matching patterns
	loc := time.UTC
	leftStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 1},
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 2},
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 3},
		TsRecord[int]{Timestamp: unixInLoc(240, loc), Value: 4},
	)

	stream2 := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 10},
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 30},
	)

	stream3 := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 200},
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 300},
	)

	// Joiner that creates a sum with markers for which streams had data
	joiner := func(left int, others []*int) string {
		result := ""
		result += string(rune('0' + left))
		if len(others) > 0 && others[0] != nil {
			result += "A"
		}
		if len(others) > 1 && others[1] != nil {
			result += "B"
		}
		return result
	}

	result := LeftJoinStreams(
		[]stream.Stream[TsRecord[int]]{leftStream, stream2, stream3},
		joiner,
	).MustCollect()

	expected := []TsRecord[string]{
		{Timestamp: unixInLoc(60, loc), Value: "1A"},   // left=1, stream2=10, stream3=nil
		{Timestamp: unixInLoc(120, loc), Value: "2B"},  // left=2, stream2=nil, stream3=200
		{Timestamp: unixInLoc(180, loc), Value: "3AB"}, // left=3, stream2=30, stream3=300
		{Timestamp: unixInLoc(240, loc), Value: "4"},   // left=4, stream2=nil, stream3=nil
	}

	require.Equal(t, expected, result)
}

func TestLeftJoinStreams_EmptyRightStream(t *testing.T) {
	// Right stream is empty, should still produce all left values
	loc := time.UTC
	leftStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 10},
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 20},
	)

	rightStream := stream.Empty[TsRecord[int]]()

	joiner := func(left int, others []*int) int {
		return left * 2 // Just double the left value
	}

	result := LeftJoinStreams(
		[]stream.Stream[TsRecord[int]]{leftStream, rightStream},
		joiner,
	).MustCollect()

	expected := []TsRecord[int]{
		{Timestamp: unixInLoc(60, loc), Value: 20},
		{Timestamp: unixInLoc(120, loc), Value: 40},
	}

	require.Equal(t, expected, result)
}

func TestLeftJoinStreams_EmptyLeftStream(t *testing.T) {
	// Left stream is empty, should produce no results
	loc := time.UTC
	leftStream := stream.Empty[TsRecord[int]]()

	rightStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 100},
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 200},
	)

	joiner := func(left int, others []*int) int {
		if len(others) > 0 && others[0] != nil {
			return left + *others[0]
		}
		return left
	}

	result := LeftJoinStreams(
		[]stream.Stream[TsRecord[int]]{leftStream, rightStream},
		joiner,
	).MustCollect()

	require.Empty(t, result)
}

func TestLeftJoinStreams_ComplexJoiner(t *testing.T) {
	// Test with a more complex data type and joiner
	type Metrics struct {
		CPU    float64
		Memory float64
	}

	loc := time.UTC
	cpuStream := stream.Just(
		TsRecord[float64]{Timestamp: unixInLoc(60, loc), Value: 50.5},
		TsRecord[float64]{Timestamp: unixInLoc(120, loc), Value: 75.2},
		TsRecord[float64]{Timestamp: unixInLoc(180, loc), Value: 90.1},
	)

	memoryStream := stream.Just(
		TsRecord[float64]{Timestamp: unixInLoc(60, loc), Value: 1024.0},
		TsRecord[float64]{Timestamp: unixInLoc(120, loc), Value: 2048.0},
		TsRecord[float64]{Timestamp: unixInLoc(180, loc), Value: 4096.0},
	)

	joiner := func(cpu float64, others []*float64) Metrics {
		memory := 0.0
		if len(others) > 0 && others[0] != nil {
			memory = *others[0]
		}
		return Metrics{CPU: cpu, Memory: memory}
	}

	result := LeftJoinStreams(
		[]stream.Stream[TsRecord[float64]]{cpuStream, memoryStream},
		joiner,
	).MustCollect()

	expected := []TsRecord[Metrics]{
		{Timestamp: unixInLoc(60, loc), Value: Metrics{CPU: 50.5, Memory: 1024.0}},
		{Timestamp: unixInLoc(120, loc), Value: Metrics{CPU: 75.2, Memory: 2048.0}},
		{Timestamp: unixInLoc(180, loc), Value: Metrics{CPU: 90.1, Memory: 4096.0}},
	}

	require.Equal(t, expected, result)
}

func TestLeftJoinStreams_SingleStream(t *testing.T) {
	// Single stream should just transform the values
	loc := time.UTC
	leftStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 10},
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 20},
	)

	joiner := func(left int, others []*int) int {
		return left * 10
	}

	result := LeftJoinStreams(
		[]stream.Stream[TsRecord[int]]{leftStream},
		joiner,
	).MustCollect()

	expected := []TsRecord[int]{
		{Timestamp: unixInLoc(60, loc), Value: 100},
		{Timestamp: unixInLoc(120, loc), Value: 200},
	}

	require.Equal(t, expected, result)
}

func TestLeftJoinStreams_RightStreamStartsLater(t *testing.T) {
	// Right stream starts after left stream
	loc := time.UTC
	leftStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 1},
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 2},
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 3},
		TsRecord[int]{Timestamp: unixInLoc(240, loc), Value: 4},
	)

	rightStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 300},
		TsRecord[int]{Timestamp: unixInLoc(240, loc), Value: 400},
	)

	joiner := func(left int, others []*int) int {
		if len(others) > 0 && others[0] != nil {
			return left + *others[0]
		}
		return left
	}

	result := LeftJoinStreams(
		[]stream.Stream[TsRecord[int]]{leftStream, rightStream},
		joiner,
	).MustCollect()

	expected := []TsRecord[int]{
		{Timestamp: unixInLoc(60, loc), Value: 1},   // No match
		{Timestamp: unixInLoc(120, loc), Value: 2},  // No match
		{Timestamp: unixInLoc(180, loc), Value: 303}, // Match: 3 + 300
		{Timestamp: unixInLoc(240, loc), Value: 404}, // Match: 4 + 400
	}

	require.Equal(t, expected, result)
}

func TestLeftJoinStreams_NonMatchingTimestamps(t *testing.T) {
	// Streams have interleaved but non-matching timestamps
	loc := time.UTC
	leftStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 1},
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 2},
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 3},
	)

	rightStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(70, loc), Value: 100},
		TsRecord[int]{Timestamp: unixInLoc(130, loc), Value: 200},
		TsRecord[int]{Timestamp: unixInLoc(190, loc), Value: 300},
	)

	joiner := func(left int, others []*int) int {
		if len(others) > 0 && others[0] != nil {
			return left + *others[0]
		}
		return left
	}

	result := LeftJoinStreams(
		[]stream.Stream[TsRecord[int]]{leftStream, rightStream},
		joiner,
	).MustCollect()

	expected := []TsRecord[int]{
		{Timestamp: unixInLoc(60, loc), Value: 1}, // No match
		{Timestamp: unixInLoc(120, loc), Value: 2}, // No match
		{Timestamp: unixInLoc(180, loc), Value: 3}, // No match
	}

	require.Equal(t, expected, result)
}

func TestLeftJoinStreams_FourStreams(t *testing.T) {
	// Four streams with selective matches
	loc := time.UTC
	leftStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 1},
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 2},
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 3},
	)

	stream2 := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 10},
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 30},
	)

	stream3 := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 200},
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 300},
	)

	stream4 := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(180, loc), Value: 3000},
	)

	joiner := func(left int, others []*int) int {
		sum := left
		for _, other := range others {
			if other != nil {
				sum += *other
			}
		}
		return sum
	}

	result := LeftJoinStreams(
		[]stream.Stream[TsRecord[int]]{leftStream, stream2, stream3, stream4},
		joiner,
	).MustCollect()

	expected := []TsRecord[int]{
		{Timestamp: unixInLoc(60, loc), Value: 11},    // 1 + 10
		{Timestamp: unixInLoc(120, loc), Value: 202},  // 2 + 200
		{Timestamp: unixInLoc(180, loc), Value: 3333}, // 3 + 30 + 300 + 3000
	}

	require.Equal(t, expected, result)
}

func TestLeftJoinStreams_NilPointerDistinction(t *testing.T) {
	// Test that nil pointers are properly distinguished from zero values
	loc := time.UTC
	leftStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 10},
		TsRecord[int]{Timestamp: unixInLoc(120, loc), Value: 20},
	)

	rightStream := stream.Just(
		TsRecord[int]{Timestamp: unixInLoc(60, loc), Value: 0}, // Zero value, not nil
	)

	// Joiner that distinguishes between nil and zero value
	joiner := func(left int, others []*int) string {
		if len(others) > 0 {
			if others[0] == nil {
				return "nil"
			}
			return "zero"
		}
		return "empty"
	}

	result := LeftJoinStreams(
		[]stream.Stream[TsRecord[int]]{leftStream, rightStream},
		joiner,
	).MustCollect()

	expected := []TsRecord[string]{
		{Timestamp: unixInLoc(60, loc), Value: "zero"}, // Has value (0), not nil
		{Timestamp: unixInLoc(120, loc), Value: "nil"}, // No match, nil pointer
	}

	require.Equal(t, expected, result)
}
