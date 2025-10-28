package stream

import (
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLeftJoinMultipleSortedStreams(t *testing.T) {
	// Helper type for test results
	type result struct {
		left   int
		others []int // -1 means nil
	}

	// Joiner function that converts []*int to []int with -1 for nil
	joiner := func(left int, others []*int) result {
		res := result{
			left:   left,
			others: make([]int, len(others)),
		}
		for i, v := range others {
			if v == nil {
				res.others[i] = -1
			} else {
				res.others[i] = *v
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
				{left: 1, others: []int{}},
				{left: 2, others: []int{}},
				{left: 3, others: []int{}},
			},
		},
		{
			name:    "two streams - no matches",
			streams: []Stream[int]{Just(1, 2, 3), Just(4, 5, 6)},
			want: []result{
				{left: 1, others: []int{-1}},
				{left: 2, others: []int{-1}},
				{left: 3, others: []int{-1}},
			},
		},
		{
			name:    "two streams - full match",
			streams: []Stream[int]{Just(1, 2, 3), Just(1, 2, 3)},
			want: []result{
				{left: 1, others: []int{1}},
				{left: 2, others: []int{2}},
				{left: 3, others: []int{3}},
			},
		},
		{
			name:    "two streams - partial match",
			streams: []Stream[int]{Just(1, 2, 3, 4), Just(2, 4)},
			want: []result{
				{left: 1, others: []int{-1}},
				{left: 2, others: []int{2}},
				{left: 3, others: []int{-1}},
				{left: 4, others: []int{4}},
			},
		},
		{
			name:    "two streams - right stream ends early",
			streams: []Stream[int]{Just(1, 2, 3, 4, 5), Just(1, 2)},
			want: []result{
				{left: 1, others: []int{1}},
				{left: 2, others: []int{2}},
				{left: 3, others: []int{-1}},
				{left: 4, others: []int{-1}},
				{left: 5, others: []int{-1}},
			},
		},
		{
			name:    "three streams - all match",
			streams: []Stream[int]{Just(1, 2, 3), Just(1, 2, 3), Just(1, 2, 3)},
			want: []result{
				{left: 1, others: []int{1, 1}},
				{left: 2, others: []int{2, 2}},
				{left: 3, others: []int{3, 3}},
			},
		},
		{
			name:    "three streams - partial matches",
			streams: []Stream[int]{Just(1, 2, 3, 4, 5), Just(1, 3, 5), Just(2, 3, 4)},
			want: []result{
				{left: 1, others: []int{1, -1}},
				{left: 2, others: []int{-1, 2}},
				{left: 3, others: []int{3, 3}},
				{left: 4, others: []int{-1, 4}},
				{left: 5, others: []int{5, -1}},
			},
		},
		{
			name:    "three streams - no matches",
			streams: []Stream[int]{Just(1, 2, 3), Just(4, 5, 6), Just(7, 8, 9)},
			want: []result{
				{left: 1, others: []int{-1, -1}},
				{left: 2, others: []int{-1, -1}},
				{left: 3, others: []int{-1, -1}},
			},
		},
		{
			name:    "three streams - some empty",
			streams: []Stream[int]{Just(1, 2, 3), Empty[int](), Just(2, 3)},
			want: []result{
				{left: 1, others: []int{-1, -1}},
				{left: 2, others: []int{-1, 2}},
				{left: 3, others: []int{-1, 3}},
			},
		},
		{
			name:    "four streams - mixed matches",
			streams: []Stream[int]{Just(1, 2, 3, 4), Just(1, 4), Just(2, 3), Just(1, 2, 3, 4)},
			want: []result{
				{left: 1, others: []int{1, -1, 1}},
				{left: 2, others: []int{-1, 2, 2}},
				{left: 3, others: []int{-1, 3, 3}},
				{left: 4, others: []int{4, -1, 4}},
			},
		},
		{
			name:    "right streams start higher than left",
			streams: []Stream[int]{Just(1, 2, 3, 4, 5), Just(3, 4, 5), Just(4, 5, 6)},
			want: []result{
				{left: 1, others: []int{-1, -1}},
				{left: 2, others: []int{-1, -1}},
				{left: 3, others: []int{3, -1}},
				{left: 4, others: []int{4, 4}},
				{left: 5, others: []int{5, 5}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := LeftJoinMultipleSortedStreams(
				tt.streams,
				shpanstream.ComparatorForOrdered[int](),
				joiner,
			).MustCollect()

			require.Equal(t, tt.want, got)
		})
	}
}

func TestLeftJoinMultipleSortedStreams_UnsortedStreamError(t *testing.T) {
	// Test that unsorted left stream causes an error
	joiner := func(left int, others []*int) int {
		return left
	}

	stream := LeftJoinMultipleSortedStreams(
		[]Stream[int]{Just(1, 3, 2)}, // Unsorted!
		shpanstream.ComparatorForOrdered[int](),
		joiner,
	)

	require.Panics(t, func() {
		stream.MustCollect()
	})
}

func TestLeftJoinMultipleSortedStreams_CustomJoiner(t *testing.T) {
	// Test with a custom joiner that creates a more complex result
	type person struct {
		id      int
		name    string
		address *string
		phone   *string
	}

	// Create streams of IDs
	ids := Just(1, 2, 3)
	addresses := Just(1, 3) // Only 1 and 3 have addresses
	phones := Just(2, 3)    // Only 2 and 3 have phones

	names := map[int]string{1: "Alice", 2: "Bob", 3: "Charlie"}
	addressMap := map[int]string{1: "123 Main St", 3: "789 Oak Ave"}
	phoneMap := map[int]string{2: "555-0100", 3: "555-0200"}

	joiner := func(id int, others []*int) person {
		p := person{
			id:   id,
			name: names[id],
		}
		// others[0] is address, others[1] is phone
		if others[0] != nil {
			addr := addressMap[*others[0]]
			p.address = &addr
		}
		if others[1] != nil {
			phone := phoneMap[*others[1]]
			p.phone = &phone
		}
		return p
	}

	result := LeftJoinMultipleSortedStreams(
		[]Stream[int]{ids, addresses, phones},
		shpanstream.ComparatorForOrdered[int](),
		joiner,
	).MustCollect()

	expected := []person{
		{id: 1, name: "Alice", address: util.Pointer("123 Main St"), phone: nil},
		{id: 2, name: "Bob", address: nil, phone: util.Pointer("555-0100")},
		{id: 3, name: "Charlie", address: util.Pointer("789 Oak Ave"), phone: util.Pointer("555-0200")},
	}

	require.Equal(t, expected, result)
}
