package stream

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
			Empty[int](),
			Just(6),
			Just(7, 8),
		).MustCollect(),
	)
}

func TestEmptyConcatenatedStream(t *testing.T) {

	require.Len(t, Concat(Just(Empty[int](), Empty[int](), Empty[int]())).MustCollect(), 0)

	require.Len(t, Concat(Empty[Stream[int]]()).MustCollect(), 0)

	require.Len(t, Concat(Just(Empty[int]())).MustCollect(), 0)
}

func TestErrorConcatenatedStream(t *testing.T) {
	ctx := context.Background()

	// Check if the error is propagated correctly
	_, err := Concat(Just(Empty[int](), Error[int](fmt.Errorf("hi")), Empty[int]())).Collect(ctx)
	require.Error(t, err)

	_, err = Concat(Error[Stream[Stream[int]]](fmt.Errorf("hi"))).Collect(ctx)
	require.Error(t, err)

	_, err = Concat(Just(Error[int](fmt.Errorf("hi")))).Collect(ctx)
	require.Error(t, err)

	_, err = Concat(Just(Just(1), Error[int](fmt.Errorf("hi")))).Collect(ctx)
	require.Error(t, err)
	_, err = Concat(Just(Error[int](fmt.Errorf("hi")), Just(1))).Collect(ctx)
	require.Error(t, err)

}
