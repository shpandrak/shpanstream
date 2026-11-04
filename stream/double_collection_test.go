package stream

import (
	"cmp"
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDoubleCollectionOfMergedStreamWithLocks reproduces the original bug
func TestDoubleCollectionOfMergedStreamWithLocks(t *testing.T) {
	mu := &sync.RWMutex{}

	s1 := Just(1, 3, 5).WithLockWhileMaterializing(mu.RLocker())
	s2 := Just(2, 4, 6).WithLockWhileMaterializing(mu.RLocker())

	merged := MergeSortedStreams(cmp.Compare, s1, s2)

	// First collection should work
	result1, err := merged.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5, 6}, result1)

	// Second collection should also work
	result2, err := merged.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5, 6}, result2, "Second collection should return same results")
}

// TestConcatStreamDoubleCollection tests concat which dynamically adds streams
func TestConcatStreamDoubleCollection(t *testing.T) {
	mu := &sync.RWMutex{}

	innerStreams := []Stream[int]{
		Just(1, 2, 3).WithLockWhileMaterializing(mu.RLocker()),
		Just(4, 5, 6).WithLockWhileMaterializing(mu.RLocker()),
		Just(7, 8, 9).WithLockWhileMaterializing(mu.RLocker()),
	}
	outerStream := Just(innerStreams...)
	concatenated := Concat(outerStream)

	// First collection
	result1, err := concatenated.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, result1)

	// Second collection
	result2, err := concatenated.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, result2, "Second collection should return same results")

	// Third collection to verify no memory leak
	result3, err := concatenated.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, result3, "Third collection should return same results")
}

// TestFlatMapDoubleCollection tests FlatMap (which uses Concat internally)
func TestFlatMapDoubleCollection(t *testing.T) {
	source := Just(1, 2, 3)

	flatMapped := FlatMap(source, func(x int) Stream[int] {
		return Just(x*10, x*10+1, x*10+2)
	})

	// First collection
	result1, err := flatMapped.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{10, 11, 12, 20, 21, 22, 30, 31, 32}, result1)

	// Second collection - THIS IS THE BUG!
	result2, err := flatMapped.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{10, 11, 12, 20, 21, 22, 30, 31, 32}, result2, "Second collection should return same results")
}

// TestFromMapKeysDoubleCollection - THIS WILL FAIL!
func TestFromMapKeysDoubleCollection(t *testing.T) {
	m := map[string]int{
		"a": 1,
		"b": 2,
		"c": 3,
	}

	stream := FromMapKeys(m)

	// First collection
	result1, err := stream.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, len(result1))

	// Second collection - BUG: Returns empty!
	result2, err := stream.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, len(result2), "Second collection should return same results - BUG: Iterator not reset!")
}

// TestFromMapValuesDoubleCollection - THIS WILL FAIL!
func TestFromMapValuesDoubleCollection(t *testing.T) {
	m := map[string]int{
		"a": 1,
		"b": 2,
		"c": 3,
	}

	stream := FromMapValues(m)

	// First collection
	result1, err := stream.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, len(result1))

	// Second collection - BUG: Returns empty!
	result2, err := stream.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, len(result2), "Second collection should return same results - BUG: Iterator not reset!")
}

// requireRematerializesFresh materializes s fully, then stops a materialization early (with Limit
// and with an iterator break) and materializes it fully again after each: every full
// materialization must start from fresh state and yield the same elements.
func requireRematerializesFresh[T any](t *testing.T, s Stream[T]) {
	t.Helper()
	ctx := context.Background()
	want, err := s.Collect(ctx)
	require.NoError(t, err)
	require.Greater(t, len(want), 2, "the stream must outlive the early stop")

	partial, err := s.Limit(2).Collect(ctx)
	require.NoError(t, err)
	require.Equal(t, want[:2], partial)

	got, err := s.Collect(ctx)
	require.NoError(t, err)
	require.Equal(t, want, got, "full materialization after a Limit")

	seen := 0
	for range s.Iterator {
		seen++
		if seen == 2 {
			break
		}
	}

	got, err = s.Collect(ctx)
	require.NoError(t, err)
	require.Equal(t, want, got, "full materialization after an iterator break")
}

func TestRematerializeAfterEarlyStop(t *testing.T) {
	sum := func(values []int) int {
		total := 0
		for _, v := range values {
			total += v
		}
		return total
	}
	sumPresent := func(values []*int) int {
		total := 0
		for _, v := range values {
			if v != nil {
				total += *v
			}
		}
		return total
	}

	t.Run("Concat", func(t *testing.T) {
		requireRematerializesFresh(t, ConcatStreams(Just(1, 2, 3), Just(4, 5)))
	})
	t.Run("FlatMap", func(t *testing.T) {
		requireRematerializesFresh(t, FlatMap(Just(1, 2), func(x int) Stream[int] {
			return Just(x*10, x*10+1, x*10+2)
		}))
	})
	t.Run("MergeSortedStreams", func(t *testing.T) {
		requireRematerializesFresh(t, MergeSortedStreams(cmp.Compare, Just(1, 4, 7), Just(2, 5, 8)))
	})
	t.Run("JoinSortedStreams", func(t *testing.T) {
		requireRematerializesFresh(t, JoinSortedStreams(
			Just(1, 2, 3, 4), Just(1, 2, 3, 4), identity[int], identity[int], cmp.Compare[int],
		))
	})
	t.Run("LeftJoinSortedStreams", func(t *testing.T) {
		requireRematerializesFresh(t, LeftJoinSortedStreams(
			Just(1, 2, 3, 4), Just(1, 2, 4), identity[int], identity[int], cmp.Compare[int],
		))
	})
	t.Run("JoinMultipleSortedStreams", func(t *testing.T) {
		requireRematerializesFresh(t, JoinMultipleSortedStreams(
			[]Stream[int]{Just(1, 2, 3, 4), Just(1, 2, 3, 4, 5)}, cmp.Compare[int], sum,
		))
	})
	t.Run("LeftJoinMultipleSortedStreams", func(t *testing.T) {
		requireRematerializesFresh(t, LeftJoinMultipleSortedStreams(
			[]Stream[int]{Just(1, 2, 3, 4), Just(1, 2, 4)}, cmp.Compare[int],
			func(left int, others []*int) int { return left + sumPresent(others) },
		))
	})
	t.Run("FullJoinMultipleSortedStreams", func(t *testing.T) {
		requireRematerializesFresh(t, FullJoinMultipleSortedStreams(
			[]Stream[int]{Just(1, 2, 3, 4), Just(1, 3, 5)}, cmp.Compare[int], sumPresent,
		))
	})
	t.Run("ClusterSortedStream", func(t *testing.T) {
		// Each cluster reports the last item of the previous one (-1 for the first cluster)
		requireRematerializesFresh(t, ClusterSortedStreamOrdered(
			func(ctx context.Context, _ int, cluster Stream[int], lastItemOnPreviousCluster *int) (int, error) {
				if err := cluster.Consume(ctx, func(int) {}); err != nil {
					return 0, err
				}
				if lastItemOnPreviousCluster == nil {
					return -1, nil
				}
				return *lastItemOnPreviousCluster, nil
			},
			identity[int],
			Just(1, 1, 2, 2, 3, 4),
		))
	})
}

func identity[T any](v T) T { return v }
