package stream

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

type testStreamUtil struct {
	watchers map[string]*testLifecycleWatcher
	t        *testing.T
}

func newTestStreamUtil(t *testing.T) *testStreamUtil {
	return &testStreamUtil{
		t:        t,
		watchers: make(map[string]*testLifecycleWatcher),
	}
}

func (t *testStreamUtil) requireAllWatchersVisited() {
	for name, w := range t.watchers {
		require.Equal(t.t, 1, w.openCalled, "%s open called", name)
		require.Equal(t.t, 1, w.closeCalled, "%s close called", name)
	}
}

func (t *testStreamUtil) requireOnlyWatchersVisited(w ...string) {
	expectedWatchersSet := MustCollectToSet(Just(w...))

	for name, w := range t.watchers {
		if _, ok := expectedWatchersSet[name]; !ok {
			require.Equal(t.t, 0, w.openCalled, "%s open called", name)
			require.Equal(t.t, 0, w.closeCalled, "%s close called", name)
		} else {
			require.Equal(t.t, 1, w.openCalled, "%s open called", name)
			require.Equal(t.t, 1, w.closeCalled, "%s close called", name)
		}
	}
}

func (t *testStreamUtil) AddLifecycleWatcher(name string) Lifecycle {
	w := &testLifecycleWatcher{}
	t.watchers[name] = w
	return w
}

type testLifecycleWatcher struct {
	openCalled  int
	closeCalled int
}

func (t *testLifecycleWatcher) Open(_ context.Context) error {
	t.openCalled++
	return nil
}

func (t *testLifecycleWatcher) Close() {
	t.closeCalled++
}

func TestZipN(t *testing.T) {
	require.Equal(
		t,
		[][]int{
			{1, 2, 3},
			{4, 5, 6},
			{7, 8, 9},
			{10, 11, 12},
			{13, 14, 15},
		},
		ZipN(
			Just(1, 4, 7, 10, 13),
			Just(2, 5, 8, 11, 14),
			Just(3, 6, 9, 12, 15),
		).MustCollect(),
	)
}

func TestZipNMissingData(t *testing.T) {

	require.Equal(
		t,
		[][]int{
			{1, 2, 3},
			{4, 5, 6},
			{7, 8, 9},
		},
		ZipN(
			Just(1, 4, 7, 10, 13),
			Just(2, 5, 8),
			Just(3, 6, 9, 12, 15),
		).MustCollect(),
	)

	require.Len(
		t,
		ZipN(
			Just(1, 4, 7, 10, 13),
			Just(2, 5, 8),
			Empty[int](),
			Just(3, 6, 9, 12, 15),
		).MustCollect(),
		0,
	)
}

func TestZipN_ResourceMgmt(t *testing.T) {

	tu := newTestStreamUtil(t)

	ZipN(
		Just(1, 4, 7, 10, 13).WithAdditionalLifecycle(tu.AddLifecycleWatcher("1")),
		Just(2, 5, 8, 11, 14).WithAdditionalLifecycle(tu.AddLifecycleWatcher("2")),
		Just(3, 6, 9, 12, 15).WithAdditionalLifecycle(tu.AddLifecycleWatcher("3")),
	).WithAdditionalLifecycle(tu.AddLifecycleWatcher("4")).MustCollect()

	// Assert all lifecycle watchers were visited
	tu.requireAllWatchersVisited()

}

func TestZipN_ErrorStream(t *testing.T) {
	// Test with stream that fails to open
	tu := newTestStreamUtil(t)
	_, err := ZipN(
		Just(1, 4, 7, 10, 13).WithAdditionalLifecycle(tu.AddLifecycleWatcher("1")),
		Just(2, 5, 8, 11, 14).WithAdditionalLifecycle(tu.AddLifecycleWatcher("2")),
		Error[int](errors.New("test error")).WithAdditionalLifecycle(tu.AddLifecycleWatcher("3")),
		Just(3, 6, 9, 12, 15).WithAdditionalLifecycle(tu.AddLifecycleWatcher("4")),
	).WithAdditionalLifecycle(tu.AddLifecycleWatcher("5")).Collect(context.Background())
	require.Error(t, err)

	tu.requireOnlyWatchersVisited("1", "2")

}

func TestZipN_StreamWithError(t *testing.T) {
	// Test with stream that fails to emit
	tu := newTestStreamUtil(t)
	_, err := ZipN(
		Just(1, 4, 7, 10, 13).WithAdditionalLifecycle(tu.AddLifecycleWatcher("1")),
		Just(2, 5, 8, 11, 14).WithAdditionalLifecycle(tu.AddLifecycleWatcher("2")),
		MapWithErr(Just[int](1, 2, 3, 4), func(i int) (int, error) {
			if i == 3 {
				return 0, errors.New("test error")
			}
			return i, nil
		}).WithAdditionalLifecycle(tu.AddLifecycleWatcher("3")),
		Just(3, 6, 9, 12, 15).WithAdditionalLifecycle(tu.AddLifecycleWatcher("4")),
	).WithAdditionalLifecycle(tu.AddLifecycleWatcher("5")).Collect(context.Background())
	require.Error(t, err)

	tu.requireAllWatchersVisited()
}
