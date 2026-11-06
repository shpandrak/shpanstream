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
		MustCollect(ZipN(
			Just(1, 4, 7, 10, 13),
			Just(2, 5, 8, 11, 14),
			Just(3, 6, 9, 12, 15),
		)),
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
		MustCollect(ZipN(
			Just(1, 4, 7, 10, 13),
			Just(2, 5, 8),
			Just(3, 6, 9, 12, 15),
		)),
	)

	require.Len(
		t,
		MustCollect(ZipN(
			Just(1, 4, 7, 10, 13),
			Just(2, 5, 8),
			Empty[int](),
			Just(3, 6, 9, 12, 15),
		)),
		0,
	)
}

func TestZipN_ResourceMgmt(t *testing.T) {

	tu := newTestStreamUtil(t)

	MustCollect(WithAdditionalLifecycle(ZipN(
		WithAdditionalLifecycle(Just(1, 4, 7, 10, 13), tu.AddLifecycleWatcher("1")),
		WithAdditionalLifecycle(Just(2, 5, 8, 11, 14), tu.AddLifecycleWatcher("2")),
		WithAdditionalLifecycle(Just(3, 6, 9, 12, 15), tu.AddLifecycleWatcher("3")),
	), tu.AddLifecycleWatcher("4")))

	// Assert all lifecycle watchers were visited
	tu.requireAllWatchersVisited()

}

func TestZipN_ErrorStream(t *testing.T) {
	// Test with stream that fails to open
	tu := newTestStreamUtil(t)
	_, err := Collect(context.Background(), WithAdditionalLifecycle(ZipN(
		WithAdditionalLifecycle(Just(1, 4, 7, 10, 13), tu.AddLifecycleWatcher("1")),
		WithAdditionalLifecycle(Just(2, 5, 8, 11, 14), tu.AddLifecycleWatcher("2")),
		WithAdditionalLifecycle(Error[int](errors.New("test error")), tu.AddLifecycleWatcher("3")),
		WithAdditionalLifecycle(Just(3, 6, 9, 12, 15), tu.AddLifecycleWatcher("4")),
	), tu.AddLifecycleWatcher("5")))
	require.Error(t, err)

	tu.requireOnlyWatchersVisited("1", "2")

}

func TestZipN_StreamWithError(t *testing.T) {
	// Test with stream that fails to emit
	tu := newTestStreamUtil(t)
	_, err := Collect(context.Background(), WithAdditionalLifecycle(ZipN(
		WithAdditionalLifecycle(Just(1, 4, 7, 10, 13), tu.AddLifecycleWatcher("1")),
		WithAdditionalLifecycle(Just(2, 5, 8, 11, 14), tu.AddLifecycleWatcher("2")),
		WithAdditionalLifecycle(MapWithErr(Just[int](1, 2, 3, 4), func(i int) (int, error) {
			if i == 3 {
				return 0, errors.New("test error")
			}
			return i, nil
		}), tu.AddLifecycleWatcher("3")),
		WithAdditionalLifecycle(Just(3, 6, 9, 12, 15), tu.AddLifecycleWatcher("4")),
	), tu.AddLifecycleWatcher("5")))
	require.Error(t, err)

	tu.requireAllWatchersVisited()
}
