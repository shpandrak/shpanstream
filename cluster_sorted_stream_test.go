package shpanstream

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

// Simple integer comparator for cluster grouping based on a predicate.
func intClusterPredicate(i int) int {
	return i
}

// Simple integer merger that combines a cluster into its sum.
func intClusterMerger(ctx context.Context, _ int, clusterStream Stream[int], _ *int) (int, error) {
	sum := 0
	collect, err := clusterStream.Collect(ctx)
	if err != nil {
		return 0, err
	}
	for _, num := range collect {
		sum += num
	}
	return sum, nil
}

func TestClusterSortedStream(t *testing.T) {
	// Create a Stream
	clusteredStream := ClusterSortedStreamOrdered(
		intClusterMerger,
		intClusterPredicate,
		Just(1, 1, 2, 2, 3, 4, 5, 6, 7, 8, 8, 9, 9),
	)

	// Expected result after clustering and merging
	expected := []int{2, 4, 3, 4, 5, 6, 7, 16, 18}

	// Collect results from the clustered Stream and compare with expected
	require.Equal(t, expected, clusteredStream.MustCollect())
}

func TestClusterSortedStream_Empty(t *testing.T) {
	// Create an empty clustered Stream
	clusteredStream := ClusterSortedStreamOrdered(intClusterMerger, intClusterPredicate, EmptyStream[int]())
	// Collect results from the clustered Stream

	require.Len(t, clusteredStream.MustCollect(), 0)
}

func TestClusterSortedStream_SingleElement(t *testing.T) {
	ctx := context.Background()

	// Create an empty clustered Stream
	clusteredStream := ClusterSortedStreamOrdered(intClusterMerger, intClusterPredicate, Just(12))
	// Collect results from the clustered Stream
	results, err := clusteredStream.Collect(ctx)

	require.NoError(t, err)

	require.Len(t, results, 1)
	require.Equal(t, 12, results[0])
}
