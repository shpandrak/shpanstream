package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream"
	"github.com/stretchr/testify/require"
)

// recordingProvider emits 1..n (or fails with failAt's error at element failAt) and records its
// lifecycle events, so tests can assert what was pulled and when it was closed.
type recordingProvider struct {
	n       int
	failAt  int
	failErr error
	next    int
	events  *[]string
}

func (p *recordingProvider) Open(context.Context) error {
	p.next = 0
	*p.events = append(*p.events, "open")
	return nil
}

func (p *recordingProvider) Close() {
	*p.events = append(*p.events, "close")
}

func (p *recordingProvider) Emit(context.Context) (int, error) {
	if p.next >= p.n {
		*p.events = append(*p.events, "eof")
		return 0, io.EOF
	}
	p.next++
	*p.events = append(*p.events, fmt.Sprintf("emit %d", p.next))
	if p.next == p.failAt {
		return 0, p.failErr
	}
	return p.next, nil
}

func recorded(n, failAt int, failErr error) (Stream[int], *[]string) {
	events := &[]string{}
	s := NewStream[int](&recordingProvider{n: n, failAt: failAt, failErr: failErr, events: events}).
		DoFinally(func(err error) {
			*events = append(*events, fmt.Sprintf("finally %v", err))
		})
	return s, events
}

func TestStream_FindFirst(t *testing.T) {
	ctx := context.Background()

	v, found, err := FindFirst(ctx, Just(1, 2, 3, 4, 5))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, v)

	v, found, err = FindFirst(ctx, Skip(Just(1, 2, 3, 4, 5), 1))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 2, v)

	v, found, err = FindFirst(ctx, Just(7))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 7, v)

	for _, empty := range []Stream[int]{Empty[int](), Just[int]()} {
		v, found, err = FindFirst(ctx, empty)
		require.NoError(t, err)
		require.False(t, found)
		require.Zero(t, v)
	}

	boom := errors.New("boom")
	v, found, err = FindFirst(ctx, Error[int](boom))
	require.ErrorIs(t, err, boom)
	require.False(t, found)
	require.Zero(t, v)
}

func TestStream_FindFirst_StopsAfterFirstElement(t *testing.T) {
	s, events := recorded(5, 0, nil)
	v, found, err := FindFirst(context.Background(), s)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, v)
	// Exactly one pull, then the provider is closed and DoFinally sees a clean (truncated) end.
	require.Equal(t, []string{"open", "emit 1", "close", "finally <nil>"}, *events)

	// Re-materialization starts over.
	*events = nil
	v, found, err = FindFirst(context.Background(), s)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, v)
	require.Equal(t, []string{"open", "emit 1", "close", "finally <nil>"}, *events)
}

func TestStream_FindFirst_EmptyLifecycle(t *testing.T) {
	s, events := recorded(0, 0, nil)
	_, found, err := FindFirst(context.Background(), s)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, []string{"open", "eof", "close", "finally <nil>"}, *events)
}

func TestStream_FindFirst_ErrorOnFirstPull(t *testing.T) {
	boom := errors.New("boom")
	s, events := recorded(5, 1, boom)
	v, found, err := FindFirst(context.Background(), s)
	require.ErrorIs(t, err, boom)
	require.False(t, found)
	require.Zero(t, v)
	require.Equal(t, []string{"open", "emit 1", "close", "finally boom"}, *events)
}

func TestStream_FindFirst_ErrorAfterFirstIsNotReached(t *testing.T) {
	// The failing element is never pulled, so FindFirst succeeds.
	s, events := recorded(5, 2, errors.New("boom"))
	v, found, err := FindFirst(context.Background(), s)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, v)
	require.Equal(t, []string{"open", "emit 1", "close", "finally <nil>"}, *events)
}

func TestStream_FindFirst_FilterPullsUntilMatch(t *testing.T) {
	s, events := recorded(5, 0, nil)
	v, found, err := FindFirst(context.Background(), s.Filter(func(i int) bool { return i == 3 }))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 3, v)
	require.Equal(t, []string{"open", "emit 1", "emit 2", "emit 3", "close", "finally <nil>"}, *events)
}

func TestStream_FindFirst_OpenFailure(t *testing.T) {
	boom := errors.New("cannot open")
	closed := false
	s := NewSimpleStream(func(ctx context.Context) (int, error) {
		t.Fatal("must not pull when open failed")
		return 0, nil
	}, WithOpenFuncOption(func(ctx context.Context) error { return boom }), WithCloseFuncOption(func() { closed = true }))
	_, found, err := FindFirst(context.Background(), s)
	require.ErrorIs(t, err, boom)
	require.False(t, found)
	require.False(t, closed, "a lifecycle element that failed to open is not closed")
}

func TestStream_FindFirst_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, found, err := FindFirst(ctx, Just(1, 2, 3))
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, found)
}

func TestStream_FindFirst_PanicInPipeline(t *testing.T) {
	v, found, err := FindFirst(context.Background(), Map(Just(1, 2), func(int) int { panic("kaboom") }))
	require.Error(t, err)
	require.Contains(t, err.Error(), "kaboom")
	require.False(t, found)
	require.Zero(t, v)
}

func TestStream_FindLast(t *testing.T) {
	ctx := context.Background()

	v, found, err := FindLast(ctx, Just(1, 2, 3, 4, 5))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 5, v)

	v, found, err = FindLast(ctx, Just(7))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 7, v)

	v, found, err = FindLast(ctx, Empty[int]())
	require.NoError(t, err)
	require.False(t, found)
	require.Zero(t, v)

	s, events := recorded(3, 0, nil)
	v, found, err = FindLast(ctx, s)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 3, v)
	require.Equal(t, []string{"open", "emit 1", "emit 2", "emit 3", "eof", "close", "finally <nil>"}, *events)

	boom := errors.New("boom")
	s, events = recorded(5, 3, boom)
	v, found, err = FindLast(ctx, s)
	require.ErrorIs(t, err, boom)
	require.False(t, found, "a failed stream has no last element, even if earlier ones were read")
	require.Zero(t, v)
	require.Equal(t, []string{"open", "emit 1", "emit 2", "emit 3", "close", "finally boom"}, *events)
}

func TestFindFirstAndLast(t *testing.T) {
	ctx := context.Background()

	v, found, err := FindFirstAndLast(ctx, Just(1, 2, 3))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, shpanstream.Tuple2[int, int]{A: 1, B: 3}, v)

	v, found, err = FindFirstAndLast(ctx, Just(4))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, shpanstream.Tuple2[int, int]{A: 4, B: 4}, v)

	v, found, err = FindFirstAndLast(ctx, Empty[int]())
	require.NoError(t, err)
	require.False(t, found)
	require.Zero(t, v)

	boom := errors.New("boom")
	s, _ := recorded(5, 2, boom)
	v, found, err = FindFirstAndLast(ctx, s)
	require.ErrorIs(t, err, boom)
	require.False(t, found)
	require.Zero(t, v)
}

func TestStream_IsEmpty(t *testing.T) {
	ctx := context.Background()

	empty, err := IsEmpty(ctx, Empty[int]())
	require.NoError(t, err)
	require.True(t, empty)

	s, events := recorded(5, 0, nil)
	empty, err = IsEmpty(ctx, s)
	require.NoError(t, err)
	require.False(t, empty)
	require.Equal(t, []string{"open", "emit 1", "close", "finally <nil>"}, *events)

	boom := errors.New("boom")
	empty, err = IsEmpty(ctx, Error[int](boom))
	require.ErrorIs(t, err, boom)
	require.False(t, empty)
}

func TestStream_MustValues_BreakClosesWithoutFurtherPulls(t *testing.T) {
	s, events := recorded(5, 0, nil)
	var got []int
	for v := range MustValues(s) {
		got = append(got, v)
		if v == 2 {
			break
		}
	}
	require.Equal(t, []int{1, 2}, got)
	require.Equal(t, []string{"open", "emit 1", "emit 2", "close", "finally <nil>"}, *events)

	*events = nil
	var idx []int
	for i, v := range MustAll(s) {
		idx = append(idx, i)
		got = append(got, v)
		if i == 1 {
			break
		}
	}
	require.Equal(t, []int{0, 1}, idx)
	require.Equal(t, []string{"open", "emit 1", "emit 2", "close", "finally <nil>"}, *events)

	*events = nil
	n := 0
	for range MustValues(s) {
		n++
	}
	require.Equal(t, 5, n)
	require.Equal(t, []string{"open", "emit 1", "emit 2", "emit 3", "emit 4", "emit 5", "eof", "close", "finally <nil>"}, *events)
}

func TestStream_MustValues_PanicsOnStreamError(t *testing.T) {
	boom := errors.New("boom")
	recoverErr := func(f func()) (err error) {
		defer func() { err, _ = recover().(error) }()
		f()
		return nil
	}
	require.ErrorIs(t, recoverErr(func() {
		for range MustValues(Error[int](boom)) {
		}
	}), boom)
	require.ErrorIs(t, recoverErr(func() {
		for range MustAll(Error[int](boom)) {
		}
	}), boom)
}

func TestFromValueProvider(t *testing.T) {
	ctx := context.Background()
	calls := 0
	s := FromValueProvider(func(ctx context.Context) (int, error) {
		calls++
		return 42, nil
	})
	require.Equal(t, 0, calls, "the provider is deferred until materialization")
	require.Equal(t, []int{42}, MustCollect(s))
	require.Equal(t, []int{42}, MustCollect(s), "each materialization fetches again")
	require.Equal(t, 2, calls)

	boom := errors.New("boom")
	_, err := Collect(ctx, FromValueProvider(func(ctx context.Context) (int, error) { return 0, boom }))
	require.Equal(t, boom, err, "the provider's error is the stream's error, unwrapped")
}

func TestFromOptionalProvider(t *testing.T) {
	ctx := context.Background()
	present := true
	calls := 0
	s := FromOptionalProvider(func(ctx context.Context) (string, bool, error) {
		calls++
		return "v", present, nil
	})
	require.Equal(t, 0, calls)
	require.Equal(t, []string{"v"}, MustCollect(s))
	present = false
	require.Empty(t, MustCollect(s))
	require.Equal(t, 2, calls)

	boom := errors.New("boom")
	_, err := Collect(ctx, FromOptionalProvider(func(ctx context.Context) (string, bool, error) { return "", false, boom }))
	require.Equal(t, boom, err)

	// FindFirst bound to a stream has the provider shape.
	findFirst := func(s Stream[int]) func(context.Context) (int, bool, error) {
		return func(ctx context.Context) (int, bool, error) { return FindFirst(ctx, s) }
	}
	require.Equal(t, []int{3}, MustCollect(FromOptionalProvider(findFirst(Just(3, 4)))))
	require.Empty(t, MustCollect(FromOptionalProvider(findFirst(Empty[int]()))))
}

func TestFromStreamProvider(t *testing.T) {
	ctx := context.Background()
	calls := 0
	var events *[]string
	s := FromStreamProvider(func(ctx context.Context) (Stream[int], error) {
		calls++
		inner, ev := recorded(3, 0, nil)
		events = ev
		return inner, nil
	})
	require.Equal(t, 0, calls)
	require.Equal(t, []int{1, 2, 3}, MustCollect(s))
	require.Equal(t, []string{"open", "emit 1", "emit 2", "emit 3", "eof", "close", "finally <nil>"}, *events)

	v, found, err := FindFirst(ctx, s)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, v)
	require.Equal(t, []string{"open", "emit 1", "close", "finally <nil>"}, *events, "the inner stream is closed on early stop")
	require.Equal(t, 2, calls)

	boom := errors.New("boom")
	_, err = Collect(ctx, FromStreamProvider(func(ctx context.Context) (Stream[int], error) { return Empty[int](), boom }))
	require.ErrorIs(t, err, boom)
}

func TestStream_FindFirst_CancelAfterFirstElementReturnsIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	v, found, err := FindFirst(ctx, NewSimpleStream(func(ctx context.Context) (int, error) {
		cancel()
		return 7, nil
	}))
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 7, v)
}

func TestStream_MustValues_LoopBodyPanicClosesStream(t *testing.T) {
	s, events := recorded(5, 0, nil)
	require.Panics(t, func() {
		for v := range MustValues(s) {
			if v == 2 {
				panic("body")
			}
		}
	})
	require.Equal(t, []string{"open", "emit 1", "emit 2", "close", "finally <nil>"}, *events)
}

// infiniteClosing is an infinite source recording whether it was closed.
func infiniteClosing(closed *atomic.Bool) Stream[int] {
	i := 0
	return NewSimpleStream(func(ctx context.Context) (int, error) {
		i++
		return i, nil
	}, WithOpenFuncOption(func(context.Context) error { i = 0; closed.Store(false); return nil }),
		WithCloseFuncOption(func() { closed.Store(true) }))
}

func TestStream_EarlyStopTearsDownConcurrentPipelines(t *testing.T) {
	before := runtime.NumGoroutine()
	var closedA, closedB atomic.Bool
	pipelines := map[string]func() Stream[int]{
		"concurrent map": func() Stream[int] {
			return Map(infiniteClosing(&closedA), func(i int) int { return i * 2 }, WithConcurrentMapOption(4))
		},
		"buffered": func() Stream[int] { return Buffered(infiniteClosing(&closedA), 8) },
		"merge sorted, buffered, concurrent map": func() Stream[int] {
			return Map(
				Buffered(MergeSortedStreams(func(a, b int) int { return a - b }, infiniteClosing(&closedA), infiniteClosing(&closedB)), 4),
				func(i int) int { return i },
				WithConcurrentMapOption(3),
			)
		},
	}
	for name, build := range pipelines {
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 20; i++ {
				_, found, err := FindFirst(context.Background(), build())
				require.NoError(t, err)
				require.True(t, found)
				require.True(t, closedA.Load(), "source closed after FindFirst")

				n := 0
				for range MustValues(build()) {
					if n++; n == 3 {
						break
					}
				}
				require.True(t, closedA.Load(), "source closed after MustValues break")

				empty, err := IsEmpty(context.Background(), build())
				require.NoError(t, err)
				require.False(t, empty)
			}
		})
	}
	// Polled inline: require.Eventually runs its condition on a goroutine of its own.
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.LessOrEqual(t, runtime.NumGoroutine(), before, "goroutines leaked")
}
