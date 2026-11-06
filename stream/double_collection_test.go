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

	s1 := WithLockWhileMaterializing(Just(1, 3, 5), mu.RLocker())
	s2 := WithLockWhileMaterializing(Just(2, 4, 6), mu.RLocker())

	merged := MergeSortedStreams(cmp.Compare, s1, s2)

	// First collection should work
	result1, err := Collect(context.Background(), merged)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5, 6}, result1)

	// Second collection should also work
	result2, err := Collect(context.Background(), merged)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5, 6}, result2, "Second collection should return same results")
}

// TestConcatStreamDoubleCollection tests concat which dynamically adds streams
func TestConcatStreamDoubleCollection(t *testing.T) {
	mu := &sync.RWMutex{}

	innerStreams := []Stream[int]{
		WithLockWhileMaterializing(Just(1, 2, 3), mu.RLocker()),
		WithLockWhileMaterializing(Just(4, 5, 6), mu.RLocker()),
		WithLockWhileMaterializing(Just(7, 8, 9), mu.RLocker()),
	}
	outerStream := Just(innerStreams...)
	concatenated := Concat(outerStream)

	// First collection
	result1, err := Collect(context.Background(), concatenated)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, result1)

	// Second collection
	result2, err := Collect(context.Background(), concatenated)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}, result2, "Second collection should return same results")

	// Third collection to verify no memory leak
	result3, err := Collect(context.Background(), concatenated)
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
	result1, err := Collect(context.Background(), flatMapped)
	require.NoError(t, err)
	require.Equal(t, []int{10, 11, 12, 20, 21, 22, 30, 31, 32}, result1)

	// Second collection - THIS IS THE BUG!
	result2, err := Collect(context.Background(), flatMapped)
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
	result1, err := Collect(context.Background(), stream)
	require.NoError(t, err)
	require.Equal(t, 3, len(result1))

	// Second collection - BUG: Returns empty!
	result2, err := Collect(context.Background(), stream)
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
	result1, err := Collect(context.Background(), stream)
	require.NoError(t, err)
	require.Equal(t, 3, len(result1))

	// Second collection - BUG: Returns empty!
	result2, err := Collect(context.Background(), stream)
	require.NoError(t, err)
	require.Equal(t, 3, len(result2), "Second collection should return same results - BUG: Iterator not reset!")
}
