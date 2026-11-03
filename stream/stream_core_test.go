package stream

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests pin the behaviour of the sequential drain after its type-independent half moved into
// stream_core.go (consumeLoop, lifecycle helpers, the DoFinally guard).

// A consumer returning io.EOF is a consumer error, not the end of the stream: it must surface.
func TestConsumeLoop_ConsumerEOFIsAnError(t *testing.T) {
	src := Just(1, 2, 3)

	err := src.ConsumeWithErr(context.Background(), func(v int) error { return io.EOF })
	require.ErrorIs(t, err, io.EOF)

	err = src.ConsumeWithErrAndCtx(context.Background(), func(_ context.Context, v int) error { return io.EOF })
	require.ErrorIs(t, err, io.EOF)
}

// A provider that fails with a real error stops the drain with that error; a provider EOF ends it cleanly.
func TestConsumeLoop_ProviderErrorAndEOF(t *testing.T) {
	boom := errors.New("boom")
	n := 0
	s := NewSimpleStream(func(ctx context.Context) (int, error) {
		n++
		if n == 3 {
			return 0, boom
		}
		return n, nil
	})
	var seen []int
	err := s.Consume(context.Background(), func(v int) { seen = append(seen, v) })
	require.ErrorIs(t, err, boom)
	require.Equal(t, []int{1, 2}, seen)

	out, err := Just(1, 2, 3).Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, out)
}

func TestConsumeLoop_LifecycleOrderAcrossOperators(t *testing.T) {
	var order []string
	lc := func(name string) Lifecycle {
		return NewLifecycle(
			func(ctx context.Context) error { order = append(order, "open:"+name); return nil },
			func() { order = append(order, "close:"+name) },
		)
	}
	expected := []string{"first", "open:a", "open:b", "close:a", "close:b", "finally"}

	s := Just(1, 2, 3).
		WithAdditionalLifecycle(lc("a")).
		Filter(func(int) bool { return true }).
		WithAdditionalLifecycle(lc("b")).
		DoFirst(func(context.Context) { order = append(order, "first") }).
		DoFinally(func(err error) { order = append(order, "finally") })
	for i := 0; i < 2; i++ {
		order = nil
		n, err := s.Count(context.Background())
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, expected, order, "same order on every consumption")
	}

	// Truncation by Limit terminates cleanly, through the same close sequence.
	order = nil
	n, err := s.Limit(2).Count(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, expected, order)
}

func TestConsumeLoop_ConsumerErrorStillClosesStream(t *testing.T) {
	boom := errors.New("boom")
	closed := 0
	s := Just(1, 2, 3).WithAdditionalLifecycle(NewLifecycle(nil, func() { closed++ }))
	err := s.ConsumeWithErr(context.Background(), func(v int) error {
		if v == 2 {
			return boom
		}
		return nil
	})
	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, closed)
}

func TestConsumeLoop_ProviderPanicRecoveredAndClosed(t *testing.T) {
	closed := 0
	s := Map(Just(1, 2, 3), func(v int) int {
		if v == 2 {
			panic(errors.New("mapper boom"))
		}
		return v
	}).WithAdditionalLifecycle(NewLifecycle(nil, func() { closed++ }))

	out, err := s.Collect(context.Background())
	require.Nil(t, out)
	require.Error(t, err)
	require.ErrorContains(t, err, "mapper boom")
	require.Equal(t, 1, closed)
}

func TestConsumeLoop_ConsumerPanicRecoveredAndClosed(t *testing.T) {
	closed := 0
	s := Just(1, 2, 3).WithAdditionalLifecycle(NewLifecycle(nil, func() { closed++ }))
	err := s.Consume(context.Background(), func(v int) {
		if v == 2 {
			panic("consumer boom")
		}
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "consumer boom")
	require.Equal(t, 1, closed)
}

func TestConsumeLoop_CancelledContextBeforeFirstPull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	opened, closed := 0, 0
	s := Just(1, 2, 3).WithAdditionalLifecycle(NewLifecycle(
		func(ctx context.Context) error { opened++; return nil },
		func() { closed++ },
	))
	err := s.Consume(ctx, func(int) { t.Fatal("must not be called") })
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, opened)
	require.Equal(t, 1, closed)
}

func TestConsumeLoop_OpenFailureWrapped(t *testing.T) {
	boom := errors.New("open boom")
	_, err := Error[int](boom).Collect(context.Background())
	require.ErrorIs(t, err, boom)
	require.ErrorContains(t, err, "failed to open stream")
}

func TestPeek_SeesElementsOnlyWhenMaterialized(t *testing.T) {
	var peeked []int
	s := Just(1, 2, 3).Peek(func(v int) { peeked = append(peeked, v) })
	require.Empty(t, peeked, "Peek must not materialize the stream")

	out, err := s.Filter(func(v int) bool { return v != 2 }).Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 3}, out)
	require.Equal(t, []int{1, 2, 3}, peeked, "Peek sees every element flowing through it")

	// Not invoked for an upstream error.
	boom := errors.New("boom")
	calls := 0
	_, err = Error[int](boom).Peek(func(int) { calls++ }).Collect(context.Background())
	require.ErrorIs(t, err, boom)
	require.Equal(t, 0, calls)
}

func TestRandomSample_ReConsumableAndSized(t *testing.T) {
	src := make([]int, 100)
	for i := range src {
		src[i] = i
	}
	s := FromSlice(src).RandomSample(10)
	for i := 0; i < 3; i++ {
		out, err := s.Collect(context.Background())
		require.NoError(t, err)
		require.Len(t, out, 10)
		seen := map[int]bool{}
		for _, v := range out {
			require.False(t, seen[v], "a reservoir sample has no duplicates")
			seen[v] = true
		}
	}

	out, err := FromSlice(src).RandomSample(0).Collect(context.Background())
	require.NoError(t, err)
	require.Empty(t, out)
}
