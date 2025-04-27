package stream

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStream_CollectRandomSample(t *testing.T) {
	streamWith10Elements := Just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	// Testing we're returning all elements if sample size is bigger than the stream
	require.Len(t, streamWith10Elements.RandomSample(20).MustCollect(), 10)

	// Testing we're returning all elements if sample size is equal to the stream
	require.Len(t, streamWith10Elements.RandomSample(10).MustCollect(), 10)

	// Testing we're returning correct number of elements if sample size is less than the stream
	require.Len(t, streamWith10Elements.RandomSample(5).MustCollect(), 5)

	// Testing we're returning correct number of elements if sample size is 0
	require.Len(t, streamWith10Elements.RandomSample(0).MustCollect(), 0)

}

func TestStream_CollectRandomSample_IsRandom(t *testing.T) {
	streamWith10Elements := Just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	// making sure it is random, theoretically it can fail, but it is very unlikely
	foundNum := make(map[int]bool)
	for i := 0; i < 100; i++ {
		rSample := streamWith10Elements.RandomSample(5).MustCollect()
		require.Len(t, rSample, 5)

		for _, v := range rSample {
			foundNum[v] = true
		}

		if len(foundNum) == 10 {
			break
		}
	}
	require.Len(t, foundNum, 10)

}
