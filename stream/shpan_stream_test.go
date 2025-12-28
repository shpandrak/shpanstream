package stream

import (
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
