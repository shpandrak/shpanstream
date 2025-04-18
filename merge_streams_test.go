package shpanstream

import (
	"cmp"
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMergedSortedStream(t *testing.T) {
	ctx := context.Background()

	// Create individual streams
	// Merge streams
	mergedStream := MergeSortedStreams(
		cmp.Compare,
		Just(1, 4, 7),
		Just(2, 5, 8, 9),
		EmptyStream[int](),
		Just(3, 6, 9),
	)

	// Expected result after merging and sorting
	expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 9}

	// Collect results from the merged Stream
	results, err := mergedStream.Collect(ctx)
	require.NoError(t, err)

	require.Equal(t, expected, results)
}

func TestMergedSortedStream_Empty(t *testing.T) {
	ctx := context.Background()

	// Merge streams
	mergedStream := MergeSortedStreams(cmp.Compare, EmptyStream[int](), EmptyStream[int](), EmptyStream[int]())
	// Collect results from the merged Stream
	results, err := mergedStream.Collect(ctx)

	require.NoError(t, err)

	require.Len(t, results, 0)

	mergedStream = MergeSortedStreams(cmp.Compare, EmptyStream[int]())
	require.NoError(t, err)

	// Collect results from the merged Stream
	results, err = mergedStream.Collect(ctx)

	require.NoError(t, err)

	require.Len(t, results, 0)
}
