package stream

import (
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFullJoinMultipleSortedStreams(t *testing.T) {
	// Helper type for test results
	type result struct {
		values []int // -1 means nil
	}

	// Joiner function that converts []*int to []int with -1 for nil
	joiner := func(values []*int) result {
		res := result{
			values: make([]int, len(values)),
		}
		for i, v := range values {
			if v == nil {
				res.values[i] = -1
			} else {
				res.values[i] = *v
			}
		}
		return res
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
			want: []result{
				{values: []int{1, -1}}, // 1 only in first stream
				{values: []int{2, -1}}, // 2 only in first stream
				{values: []int{3, -1}}, // 3 only in first stream
				{values: []int{-1, 4}}, // 4 only in second stream
				{values: []int{-1, 5}}, // 5 only in second stream
				{values: []int{-1, 6}}, // 6 only in second stream
			},
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
			streams: []Stream[int]{Just(1, 2, 3, 4), Just(2, 4, 5)},
			want: []result{
				{values: []int{1, -1}}, // 1 only in first
				{values: []int{2, 2}},  // 2 in both
				{values: []int{3, -1}}, // 3 only in first
				{values: []int{4, 4}},  // 4 in both
				{values: []int{-1, 5}}, // 5 only in second
			},
		},
		{
			name:    "two streams - first stream ends early",
			streams: []Stream[int]{Just(1, 2), Just(1, 2, 3, 4, 5)},
			want: []result{
				{values: []int{1, 1}},
				{values: []int{2, 2}},
				{values: []int{-1, 3}}, // 3 only in second
				{values: []int{-1, 4}}, // 4 only in second
				{values: []int{-1, 5}}, // 5 only in second
			},
		},
		{
			name:    "two streams - second stream ends early",
			streams: []Stream[int]{Just(1, 2, 3, 4, 5), Just(1, 2)},
			want: []result{
				{values: []int{1, 1}},
				{values: []int{2, 2}},
				{values: []int{3, -1}}, // 3 only in first
				{values: []int{4, -1}}, // 4 only in first
				{values: []int{5, -1}}, // 5 only in first
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
				{values: []int{1, 1, -1}}, // 1 in first two
				{values: []int{2, -1, 2}}, // 2 in first and third
				{values: []int{3, 3, 3}},  // 3 in all
				{values: []int{4, -1, 4}}, // 4 in first and third
				{values: []int{5, 5, -1}}, // 5 in first two
			},
		},
		{
			name:    "three streams - no common element",
			streams: []Stream[int]{Just(1, 2, 3), Just(4, 5, 6), Just(7, 8, 9)},
			want: []result{
				{values: []int{1, -1, -1}},
				{values: []int{2, -1, -1}},
				{values: []int{3, -1, -1}},
				{values: []int{-1, 4, -1}},
				{values: []int{-1, 5, -1}},
				{values: []int{-1, 6, -1}},
				{values: []int{-1, -1, 7}},
				{values: []int{-1, -1, 8}},
				{values: []int{-1, -1, 9}},
			},
		},
		{
			name:    "three streams - some empty",
			streams: []Stream[int]{Just(1, 2, 3), Empty[int](), Just(2, 3, 4)},
			want: []result{
				{values: []int{1, -1, -1}},
				{values: []int{2, -1, 2}},
				{values: []int{3, -1, 3}},
				{values: []int{-1, -1, 4}},
			},
		},
		{
			name:    "three streams - all empty",
			streams: []Stream[int]{Empty[int](), Empty[int](), Empty[int]()},
			want:    nil,
		},
		{
			name:    "four streams - mixed matches",
			streams: []Stream[int]{Just(1, 2, 3, 4), Just(1, 4), Just(2, 3), Just(1, 2, 3, 4)},
			want: []result{
				{values: []int{1, 1, -1, 1}},
				{values: []int{2, -1, 2, 2}},
				{values: []int{3, -1, 3, 3}},
				{values: []int{4, 4, -1, 4}},
			},
		},
		{
			name:    "streams start at different positions",
			streams: []Stream[int]{Just(1, 2, 3, 4, 5), Just(3, 4, 5), Just(4, 5, 6)},
			want: []result{
				{values: []int{1, -1, -1}},
				{values: []int{2, -1, -1}},
				{values: []int{3, 3, -1}},
				{values: []int{4, 4, 4}},
				{values: []int{5, 5, 5}},
				{values: []int{-1, -1, 6}},
			},
		},
		{
			name:    "interleaved values",
			streams: []Stream[int]{Just(1, 3, 5, 7), Just(2, 4, 6, 8)},
			want: []result{
				{values: []int{1, -1}},
				{values: []int{-1, 2}},
				{values: []int{3, -1}},
				{values: []int{-1, 4}},
				{values: []int{5, -1}},
				{values: []int{-1, 6}},
				{values: []int{7, -1}},
				{values: []int{-1, 8}},
			},
		},
		{
			name:    "duplicate handling - same value in all streams",
			streams: []Stream[int]{Just(1, 1, 2), Just(1, 1, 2), Just(1, 1, 2)},
			want: []result{
				{values: []int{1, 1, 1}},
				{values: []int{1, 1, 1}},
				{values: []int{2, 2, 2}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FullJoinMultipleSortedStreams(
				tt.streams,
				shpanstream.ComparatorForOrdered[int](),
				joiner,
			).MustCollect()

			require.Equal(t, tt.want, got)
		})
	}
}

func TestFullJoinMultipleSortedStreams_UnsortedStreamError(t *testing.T) {
	// Test that unsorted stream causes an error
	joiner := func(values []*int) int {
		for _, v := range values {
			if v != nil {
				return *v
			}
		}
		return 0
	}

	stream := FullJoinMultipleSortedStreams(
		[]Stream[int]{Just(1, 3, 2)}, // Unsorted!
		shpanstream.ComparatorForOrdered[int](),
		joiner,
	)

	require.Panics(t, func() {
		stream.MustCollect()
	})
}

func TestFullJoinMultipleSortedStreams_MultipleUnsortedStreams(t *testing.T) {
	// Test that unsorted stream in second position causes an error
	joiner := func(values []*int) []int {
		result := make([]int, 0)
		for _, v := range values {
			if v != nil {
				result = append(result, *v)
			}
		}
		return result
	}

	stream := FullJoinMultipleSortedStreams(
		[]Stream[int]{Just(1, 2, 3, 4, 5), Just(1, 2, 4, 3, 5)}, // Second stream unsorted (4, 3)!
		shpanstream.ComparatorForOrdered[int](),
		joiner,
	)

	require.Panics(t, func() {
		stream.MustCollect()
	})
}

func TestFullJoinMultipleSortedStreams_CustomJoiner(t *testing.T) {
	// Test with a custom joiner that creates a more complex result
	type person struct {
		id      int
		name    string
		address *string
		phone   *string
	}

	// Create streams of IDs with different coverage
	ids := Just(1, 2, 3, 4)
	addresses := Just(1, 2, 3) // Missing 4
	phones := Just(2, 3, 4)    // Missing 1

	names := map[int]string{1: "Alice", 2: "Bob", 3: "Charlie", 4: "David"}
	addressMap := map[int]string{1: "123 Main St", 2: "456 Oak Ave", 3: "789 Pine Rd"}
	phoneMap := map[int]string{2: "555-0100", 3: "555-0200", 4: "555-0300"}

	joiner := func(values []*int) person {
		// Find the ID from any non-nil value
		var id int
		for _, v := range values {
			if v != nil {
				id = *v
				break
			}
		}

		p := person{
			id:   id,
			name: names[id],
		}

		// values[1] is address, values[2] is phone
		if values[1] != nil {
			addr := addressMap[*values[1]]
			p.address = &addr
		}
		if values[2] != nil {
			phone := phoneMap[*values[2]]
			p.phone = &phone
		}
		return p
	}

	result := FullJoinMultipleSortedStreams(
		[]Stream[int]{ids, addresses, phones},
		shpanstream.ComparatorForOrdered[int](),
		joiner,
	).MustCollect()

	expected := []person{
		{id: 1, name: "Alice", address: util.Pointer("123 Main St"), phone: nil},
		{id: 2, name: "Bob", address: util.Pointer("456 Oak Ave"), phone: util.Pointer("555-0100")},
		{id: 3, name: "Charlie", address: util.Pointer("789 Pine Rd"), phone: util.Pointer("555-0200")},
		{id: 4, name: "David", address: nil, phone: util.Pointer("555-0300")},
	}

	require.Equal(t, expected, result)
}

func TestFullJoinMultipleSortedStreams_CountNonNil(t *testing.T) {
	// Test with a joiner that counts how many streams have each value
	countJoiner := func(values []*int) int {
		count := 0
		for _, v := range values {
			if v != nil {
				count++
			}
		}
		return count
	}

	result := FullJoinMultipleSortedStreams(
		[]Stream[int]{
			Just(1, 2, 3, 4, 5),
			Just(2, 3, 4),
			Just(3, 4, 5),
		},
		shpanstream.ComparatorForOrdered[int](),
		countJoiner,
	).MustCollect()

	// 1: only in first stream = 1
	// 2: in first and second = 2
	// 3: in all three = 3
	// 4: in all three = 3
	// 5: in first and third = 2
	expected := []int{1, 2, 3, 3, 2}

	require.Equal(t, expected, result)
}
