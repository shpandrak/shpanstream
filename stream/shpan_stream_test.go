package stream

import (
	"context"
	"testing"

	"github.com/shpandrak/shpanstream/lazy"
	"github.com/stretchr/testify/require"
)

func TestStream_FindFirst(t *testing.T) {
	addOne := func(i int) int {
		return i + 1
	}

	require.Equal(t, 1, Just(1, 2, 3, 4, 5).FindFirst().MustGet())
	require.Equal(t, 2, lazy.Map(Just(1, 2, 3, 4, 5).FindFirst(), addOne).MustGet())
	require.Equal(t, 2, Just(1, 2, 3, 4, 5).Skip(1).FindFirst().MustGet())
	require.Nil(t, Empty[int]().FindFirst().MustGetOptional())
	require.Nil(t, Just[int]().FindFirst().MustGetOptional())
}

func TestStream_Filter(t *testing.T) {
	addOne := func(i int) int {
		return i + 1
	}

	require.Len(
		t,
		Just(1, 2, 3, 4, 5).
			Filter(func(i int) bool {
				return i > 2
			}).
			MustCollect(),
		3,
	)

	require.Len(
		t,
		Map(Just(1, 2, 3, 4, 5), addOne).
			Filter(func(i int) bool {
				return i > 2
			}).
			MustCollect(),
		4,
	)

}

func TestWithAdditionalLifecycle_SiblingStreamsDoNotShareBackingArray(t *testing.T) {
	// Grow the lifecycle slice until it has spare capacity, then derive two sibling
	// streams from it: each sibling's appended element must land in its own backing
	// array, not overwrite the other's.
	base := Just(1).
		WithAdditionalLifecycle(NewLifecycle(nil, nil)).
		WithAdditionalLifecycle(NewLifecycle(nil, nil))

	var aOpened, bOpened bool
	sa := base.WithAdditionalLifecycle(NewLifecycle(
		func(context.Context) error { aOpened = true; return nil },
		nil,
	))
	sb := base.WithAdditionalLifecycle(NewLifecycle(
		func(context.Context) error { bOpened = true; return nil },
		nil,
	))

	require.Equal(t, []int{1}, sa.MustCollect())
	require.Equal(t, []int{1}, sb.MustCollect())
	require.True(t, aOpened, "sibling A's lifecycle was overwritten by sibling B's append")
	require.True(t, bOpened)
}
