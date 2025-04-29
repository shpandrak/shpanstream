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
		ConcatStreams(
			Just(1, 2, 3),
			Just(4, 5),
			Empty[int](),
			Just(6),
			Just(7, 8),
		).MustCollect(),
	)
}

func TestConcatenatedStreamEmpty(t *testing.T) {

	require.Len(t, Concat(Just(Empty[int](), Empty[int](), Empty[int]())).MustCollect(), 0)

	require.Len(t, Concat(Empty[Stream[int]]()).MustCollect(), 0)

	require.Len(t, Concat(Just(Empty[int]())).MustCollect(), 0)
}

func TestConcatStreamsError(t *testing.T) {
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

func TestConcatResourceMgmt(t *testing.T) {

	tu := newTestStreamUtil(t)

	// Assert the results are as expected
	Concat(
		Just(
			Just(1, 2, 3).WithAdditionalLifecycle(tu.AddLifecycleWatcher("1")),
			Just(4, 5).WithAdditionalLifecycle(tu.AddLifecycleWatcher("2")),
			Empty[int]().WithAdditionalLifecycle(tu.AddLifecycleWatcher("3")),
			Just(6).WithAdditionalLifecycle(tu.AddLifecycleWatcher("4")),
			Just(7, 8).WithAdditionalLifecycle(tu.AddLifecycleWatcher("5")),
		).WithAdditionalLifecycle(tu.AddLifecycleWatcher("6")),
	).MustCollect()
	// Assert all lifecycle watchers were visited
	tu.requireAllWatchersVisited()
}

func TestConcatResourceMgmtErrorStream(t *testing.T) {

	tu := newTestStreamUtil(t)

	// Assert the results are as expected
	_, err := Concat(
		Just(
			Just(1, 2, 3).WithAdditionalLifecycle(tu.AddLifecycleWatcher("1")),
			Just(4, 5).WithAdditionalLifecycle(tu.AddLifecycleWatcher("2")),
			Error[int](errors.New("test error")).WithAdditionalLifecycle(tu.AddLifecycleWatcher("3")),
			Just(6).WithAdditionalLifecycle(tu.AddLifecycleWatcher("4")),
			Just(7, 8).WithAdditionalLifecycle(tu.AddLifecycleWatcher("5")),
		).WithAdditionalLifecycle(tu.AddLifecycleWatcher("6")),
	).Collect(context.Background())
	require.Error(t, err)

	// Assert lifecycle watchers were visited
	tu.requireOnlyWatchersVisited("1", "2", "6")

}

func TestConcatResourceMgmtStreamWithErr(t *testing.T) {

	tu := newTestStreamUtil(t)

	// Assert the results are as expected
	_, err := Concat(
		Just(
			Just(1, 2, 3).WithAdditionalLifecycle(tu.AddLifecycleWatcher("1")),
			Just(4, 5).WithAdditionalLifecycle(tu.AddLifecycleWatcher("2")),
			MapWithErr(Just[int](1, 2, 3, 4), func(i int) (int, error) {
				if i == 3 {
					return 0, errors.New("test error")
				}
				return i, nil
			}).WithAdditionalLifecycle(tu.AddLifecycleWatcher("3")),
			Just(6).WithAdditionalLifecycle(tu.AddLifecycleWatcher("4")),
			Just(7, 8).WithAdditionalLifecycle(tu.AddLifecycleWatcher("5")),
		).WithAdditionalLifecycle(tu.AddLifecycleWatcher("6")),
	).Collect(context.Background())
	require.Error(t, err)

	// Assert lifecycle watchers were visited
	tu.requireOnlyWatchersVisited("1", "2", "3", "6")

}
