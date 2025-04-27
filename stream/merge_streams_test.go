package stream

import (
	"cmp"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMergedSortedStream(t *testing.T) {
	// Merge sorted streams and assert that the merged stream is sorted as expected
	require.Equal(
		t,
		// Expected result after merging and sorting
		[]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 9},
		MergeSortedStreams(
			cmp.Compare,
			Just(1, 4, 7),
			Just(2, 5, 8, 9),
			EmptyStream[int](),
			Just(3, 6, 9),
		).MustCollect(),
	)
}

func TestMergedSortedStream_Empty(t *testing.T) {

	// Merge Empty streams and assert that the merged stream is empty
	require.Len(t, MergeSortedStreams(
		cmp.Compare,
		EmptyStream[int](),
		EmptyStream[int](),
		EmptyStream[int](),
	).MustCollect(), 0)

	//Just one empty stream
	require.Len(
		t,
		MergeSortedStreams(cmp.Compare, EmptyStream[int]()).MustCollect(),
		0,
	)
}
