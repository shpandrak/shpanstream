package stream

import (
	"github.com/shpandrak/shpanstream"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJoinMultipleSortedStreams(t *testing.T) {
	// Helper type for test results
	type result struct {
		values []int
	}

	// Joiner function that collects all values
	joiner := func(values []int) result {
		return result{values: values}
	}

	type testCase struct {
		name    string
		streams []Stream[int]
		want    []result
	}

	tests := []testCase{
		{
			name:    "empty streams",
			streams: []Stream[int]{Empty[int]()},
			want:    nil,
		},
		{
			name:    "single stream with values",
			streams: []Stream[int]{Just(1, 2, 3)},
			want: []result{
				{values: []int{1}},
				{values: []int{2}},
				{values: []int{3}},
			},
		},
		{
			name:    "two streams - no matches",
			streams: []Stream[int]{Just(1, 2, 3), Just(4, 5, 6)},
			want:    nil,
		},
		{
			name:    "two streams - full match",
			streams: []Stream[int]{Just(1, 2, 3), Just(1, 2, 3)},
			want: []result{
				{values: []int{1, 1}},
				{values: []int{2, 2}},
				{values: []int{3, 3}},
			},
		},
		{
			name:    "two streams - partial match",
			streams: []Stream[int]{Just(1, 2, 3, 4), Just(2, 4)},
			want: []result{
				{values: []int{2, 2}},
				{values: []int{4, 4}},
			},
		},
		{
			name:    "two streams - first stream ends early",
			streams: []Stream[int]{Just(1, 2), Just(1, 2, 3, 4, 5)},
			want: []result{
				{values: []int{1, 1}},
				{values: []int{2, 2}},
			},
		},
		{
			name:    "two streams - second stream ends early",
			streams: []Stream[int]{Just(1, 2, 3, 4, 5), Just(1, 2)},
			want: []result{
				{values: []int{1, 1}},
				{values: []int{2, 2}},
			},
		},
		{
			name:    "three streams - all match",
			streams: []Stream[int]{Just(1, 2, 3), Just(1, 2, 3), Just(1, 2, 3)},
			want: []result{
				{values: []int{1, 1, 1}},
				{values: []int{2, 2, 2}},
				{values: []int{3, 3, 3}},
			},
		},
		{
			name:    "three streams - partial matches",
			streams: []Stream[int]{Just(1, 2, 3, 4, 5), Just(1, 3, 5), Just(2, 3, 4)},
			want: []result{
				{values: []int{3, 3, 3}},
			},
		},
		{
			name:    "three streams - no common element",
			streams: []Stream[int]{Just(1, 2, 3), Just(4, 5, 6), Just(7, 8, 9)},
			want:    nil,
		},
		{
			name:    "three streams - one empty",
			streams: []Stream[int]{Just(1, 2, 3), Empty[int](), Just(2, 3)},
			want:    nil,
		},
		{
			name:    "four streams - mixed matches",
			streams: []Stream[int]{Just(1, 2, 3, 4), Just(1, 4), Just(2, 3), Just(1, 2, 3, 4)},
			want:    nil, // No single element appears in all four streams
		},
		{
			name:    "four streams - all have common elements",
			streams: []Stream[int]{Just(1, 2, 3, 4), Just(2, 3, 4), Just(2, 3, 4, 5), Just(2, 3)},
			want: []result{
				{values: []int{2, 2, 2, 2}},
				{values: []int{3, 3, 3, 3}},
			},
		},
		{
			name:    "streams start at different positions",
			streams: []Stream[int]{Just(1, 2, 3, 4, 5), Just(3, 4, 5), Just(4, 5, 6)},
			want: []result{
				{values: []int{4, 4, 4}},
				{values: []int{5, 5, 5}},
			},
		},
		{
			name:    "two streams - one element overlap at end",
			streams: []Stream[int]{Just(1, 2, 3), Just(3, 4, 5)},
			want: []result{
				{values: []int{3, 3}},
			},
		},
		{
			name:    "two streams - one element overlap at start",
			streams: []Stream[int]{Just(3, 4, 5), Just(1, 2, 3)},
			want: []result{
				{values: []int{3, 3}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := JoinMultipleSortedStreams(
				tt.streams,
				shpanstream.ComparatorForOrdered[int](),
				joiner,
			).MustCollect()

			require.Equal(t, tt.want, got)
		})
	}
}

func TestJoinMultipleSortedStreams_UnsortedStreamError(t *testing.T) {
	// Test that unsorted stream causes an error
	joiner := func(values []int) int {
		return values[0]
	}

	stream := JoinMultipleSortedStreams(
		[]Stream[int]{Just(1, 3, 2)}, // Unsorted!
		shpanstream.ComparatorForOrdered[int](),
		joiner,
	)

	require.Panics(t, func() {
		stream.MustCollect()
	})
}

func TestJoinMultipleSortedStreams_MultipleUnsortedStreams(t *testing.T) {
	// Test that unsorted stream in second position causes an error
	// We need to ensure the unsorted element is actually checked, so use a longer stream
	joiner := func(values []int) []int {
		return values
	}

	stream := JoinMultipleSortedStreams(
		[]Stream[int]{Just(1, 2, 3, 4, 5), Just(1, 2, 4, 3, 5)}, // Second stream unsorted (4, 3)!
		shpanstream.ComparatorForOrdered[int](),
		joiner,
	)

	require.Panics(t, func() {
		stream.MustCollect()
	})
}

func TestJoinMultipleSortedStreams_CustomJoiner(t *testing.T) {
	// Test with a custom joiner that creates a more complex result
	type person struct {
		id      int
		name    string
		address string
		phone   string
	}

	// Create streams of IDs - only emit when all three streams have a match
	ids := Just(1, 2, 3)
	addresses := Just(1, 3) // Only 1 and 3 have addresses
	phones := Just(2, 3)    // Only 2 and 3 have phones

	names := map[int]string{1: "Alice", 2: "Bob", 3: "Charlie"}
	addressMap := map[int]string{1: "123 Main St", 3: "789 Oak Ave"}
	phoneMap := map[int]string{2: "555-0100", 3: "555-0200"}

	joiner := func(values []int) person {
		id := values[0]
		return person{
			id:      id,
			name:    names[id],
			address: addressMap[id],
			phone:   phoneMap[id],
		}
	}

	result := JoinMultipleSortedStreams(
		[]Stream[int]{ids, addresses, phones},
		shpanstream.ComparatorForOrdered[int](),
		joiner,
	).MustCollect()

	// Only ID 3 appears in all three streams
	expected := []person{
		{id: 3, name: "Charlie", address: "789 Oak Ave", phone: "555-0200"},
	}

	require.Equal(t, expected, result)
}

func TestJoinMultipleSortedStreams_IntegerJoiner(t *testing.T) {
	// Test with a simple sum joiner
	sumJoiner := func(values []int) int {
		sum := 0
		for _, v := range values {
			sum += v
		}
		return sum
	}

	result := JoinMultipleSortedStreams(
		[]Stream[int]{Just(1, 2, 3), Just(1, 2, 3), Just(1, 2, 3)},
		shpanstream.ComparatorForOrdered[int](),
		sumJoiner,
	).MustCollect()

	expected := []int{3, 6, 9} // 1+1+1, 2+2+2, 3+3+3

	require.Equal(t, expected, result)
}
