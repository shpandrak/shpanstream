package shpanstream

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConcatenatedStream(t *testing.T) {
	// Assert the results are as expected
	require.EqualValues(
		t,
		[]int{1, 2, 3, 4, 5, 6, 7, 8},
		ConcatStreams(
			Just(1, 2, 3),
			Just(4, 5),
			EmptyStream[int](),
			Just(6),
			Just(7, 8),
		).MustCollect(),
	)
}

func TestEmptyConcatenatedStream(t *testing.T) {

	require.Len(t, Concat(Just(EmptyStream[int](), EmptyStream[int](), EmptyStream[int]())).MustCollect(), 0)

	require.Len(t, Concat(EmptyStream[Stream[int]]()).MustCollect(), 0)

	require.Len(t, Concat(Just(EmptyStream[int]())).MustCollect(), 0)
}

func TestErrorConcatenatedStream(t *testing.T) {
	ctx := context.Background()

	// Check if the error is propagated correctly
	_, err := Concat(Just(EmptyStream[int](), NewErrorStream[int](fmt.Errorf("hi")), EmptyStream[int]())).Collect(ctx)
	require.Error(t, err)

	_, err = Concat(NewErrorStream[Stream[Stream[int]]](fmt.Errorf("hi"))).Collect(ctx)
	require.Error(t, err)

	_, err = Concat(Just(NewErrorStream[int](fmt.Errorf("hi")))).Collect(ctx)
	require.Error(t, err)

	_, err = Concat(Just(Just(1), NewErrorStream[int](fmt.Errorf("hi")))).Collect(ctx)
	require.Error(t, err)
	_, err = Concat(Just(NewErrorStream[int](fmt.Errorf("hi")), Just(1))).Collect(ctx)
	require.Error(t, err)

}
