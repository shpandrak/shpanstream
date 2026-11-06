package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConcatStreams(t *testing.T) {
	// Assert the results are as expected
	require.EqualValues(
		t,
		[]int{1, 2, 3, 4, 5, 6, 7, 8},
		MustCollect(ConcatStreams(
			Just(1, 2, 3),
			Just(4, 5),
			Empty[int](),
			Just(6),
			Just(7, 8),
		)),
	)
}

func TestConcatenatedStreamEmpty(t *testing.T) {

	require.Len(t, MustCollect(Concat(Just(Empty[int](), Empty[int](), Empty[int]()))), 0)

	require.Len(t, MustCollect(Concat(Empty[Stream[int]]())), 0)

	require.Len(t, MustCollect(Concat(Just(Empty[int]()))), 0)
}

func TestConcatStreamsError(t *testing.T) {
	ctx := context.Background()

	// Check if the error is propagated correctly
	_, err := Collect(ctx, Concat(Just(Empty[int](), Error[int](fmt.Errorf("hi")), Empty[int]())))
	require.Error(t, err)

	_, err = Collect(ctx, Concat(Error[Stream[Stream[int]]](fmt.Errorf("hi"))))
	require.Error(t, err)

	_, err = Collect(ctx, Concat(Just(Error[int](fmt.Errorf("hi")))))
	require.Error(t, err)

	_, err = Collect(ctx, Concat(Just(Just(1), Error[int](fmt.Errorf("hi")))))
	require.Error(t, err)
	_, err = Collect(ctx, Concat(Just(Error[int](fmt.Errorf("hi")), Just(1))))
	require.Error(t, err)

}

func TestConcatResourceMgmt(t *testing.T) {

	tu := newTestStreamUtil(t)

	// Assert the results are as expected
	MustCollect(Concat(
		WithAdditionalLifecycle(Just(
			WithAdditionalLifecycle(Just(1, 2, 3), tu.AddLifecycleWatcher("1")),
			WithAdditionalLifecycle(Just(4, 5), tu.AddLifecycleWatcher("2")),
			WithAdditionalLifecycle(Empty[int](), tu.AddLifecycleWatcher("3")),
			WithAdditionalLifecycle(Just(6), tu.AddLifecycleWatcher("4")),
			WithAdditionalLifecycle(Just(7, 8), tu.AddLifecycleWatcher("5")),
		), tu.AddLifecycleWatcher("6")),
	))
	// Assert all lifecycle watchers were visited
	tu.requireAllWatchersVisited()
}

func TestConcatResourceMgmtErrorStream(t *testing.T) {

	tu := newTestStreamUtil(t)

	// Assert the results are as expected
	_, err := Collect(context.Background(), Concat(
		WithAdditionalLifecycle(Just(
			WithAdditionalLifecycle(Just(1, 2, 3), tu.AddLifecycleWatcher("1")),
			WithAdditionalLifecycle(Just(4, 5), tu.AddLifecycleWatcher("2")),
			WithAdditionalLifecycle(Error[int](errors.New("test error")), tu.AddLifecycleWatcher("3")),
			WithAdditionalLifecycle(Just(6), tu.AddLifecycleWatcher("4")),
			WithAdditionalLifecycle(Just(7, 8), tu.AddLifecycleWatcher("5")),
		), tu.AddLifecycleWatcher("6")),
	))
	require.Error(t, err)

	// Assert lifecycle watchers were visited
	tu.requireOnlyWatchersVisited("1", "2", "6")

}

func TestConcatResourceMgmtStreamWithErr(t *testing.T) {

	tu := newTestStreamUtil(t)

	// Assert the results are as expected
	_, err := Collect(context.Background(), Concat(
		WithAdditionalLifecycle(Just(
			WithAdditionalLifecycle(Just(1, 2, 3), tu.AddLifecycleWatcher("1")),
			WithAdditionalLifecycle(Just(4, 5), tu.AddLifecycleWatcher("2")),
			WithAdditionalLifecycle(MapWithErr(Just[int](1, 2, 3, 4), func(i int) (int, error) {
				if i == 3 {
					return 0, errors.New("test error")
				}
				return i, nil
			}), tu.AddLifecycleWatcher("3")),
			WithAdditionalLifecycle(Just(6), tu.AddLifecycleWatcher("4")),
			WithAdditionalLifecycle(Just(7, 8), tu.AddLifecycleWatcher("5")),
		), tu.AddLifecycleWatcher("6")),
	))
	require.Error(t, err)

	// Assert lifecycle watchers were visited
	tu.requireOnlyWatchersVisited("1", "2", "3", "6")

}
