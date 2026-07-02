package stream

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

// Regression: Result.Unpack / UnpackResult used to drop the error, which made Buffered append a
// spurious trailing zero (from the io.EOF sentinel mapped to (zero, nil)) and swallow upstream
// errors.
func TestBuffered_NoSpuriousTrailingValue(t *testing.T) {
	out, err := Buffered(Just(1, 2, 3, 4, 5), 3).Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5}, out)
}

func TestBuffered_SurfacesUpstreamError(t *testing.T) {
	boom := errors.New("boom")
	_, err := Buffered(
		MapWithErr(Just(1, 2, 3, 4), func(i int) (int, error) {
			if i == 3 {
				return 0, boom
			}
			return i, nil
		}),
		2,
	).Collect(context.Background())
	require.ErrorIs(t, err, boom)
}

func TestBuffered_EmptyStream(t *testing.T) {
	out, err := Buffered(Empty[int](), 3).Collect(context.Background())
	require.NoError(t, err)
	require.Empty(t, out)
}

// Buffered must be re-consumable (double collection): channel state is created per Open, so a
// second consumption drains the source again rather than returning an empty result.
func TestBuffered_ReConsumable(t *testing.T) {
	s := Buffered(Just(1, 2, 3, 4, 5), 3)
	for i := 0; i < 3; i++ {
		out, err := s.Collect(context.Background())
		require.NoError(t, err)
		require.Equal(t, []int{1, 2, 3, 4, 5}, out)
	}
}

// Re-consumption must also hold after an early-terminated consumption (teardown joins the goroutine
// and resets state cleanly).
func TestBuffered_ReConsumableAfterEarlyStop(t *testing.T) {
	s := Buffered(Just(1, 2, 3, 4, 5), 2)
	partial, err := s.Limit(2).Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, partial)

	full, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5}, full)
}

// Regression: Buffered's teardown must cancel and join the buffering goroutine, so the source is
// fully closed by the time Consume returns. Without the join, the goroutine (and the source's
// Close) outlives the stream, racing with callers that release resources right after Consume.
func TestBuffered_SourceClosedBeforeConsumeReturns(t *testing.T) {
	for i := 0; i < 100; i++ {
		var sourceClosed atomic.Bool
		src := Just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).
			WithAdditionalLifecycle(NewLifecycle(
				func(ctx context.Context) error { return nil },
				func() { sourceClosed.Store(true) },
			))

		// Stop early so the buffering goroutine is still in flight (blocked on a full buffer)
		// when teardown runs.
		out, err := Buffered(src, 3).Limit(2).Collect(context.Background())
		require.NoError(t, err)
		require.Equal(t, []int{1, 2}, out)
		require.True(t, sourceClosed.Load(), "source must be closed by the time Consume returns")
	}
}

// Same invariant when the source is blocked inside its provider and the consumer's ctx expires.
// The source's Close is deliberately slow: only a teardown that joins the buffering goroutine
// (which owns the source lifecycle) observes the flag as set.
func TestBuffered_TeardownReleasesBlockedSource(t *testing.T) {
	var sourceClosed atomic.Bool
	neverWritten := make(chan int)
	src := FromChannel(neverWritten).
		WithAdditionalLifecycle(NewLifecycle(
			func(ctx context.Context) error { return nil },
			func() {
				time.Sleep(20 * time.Millisecond)
				sourceClosed.Store(true)
			},
		))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := Buffered(src, 3).Collect(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.True(t, sourceClosed.Load(), "source must be closed by the time Consume returns")
}

func TestStream_Buffered(t *testing.T) {
	iterationCounter := atomic.Int32{}
	actuallyReadCounter := atomic.Int32{}

	sourceChannel := Just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).Peek(func(v int) {
		// Counting how many items are actually read from the source channel
		actuallyReadCounter.Add(1)
	})

	buffered := Buffered(sourceChannel, 3).Peek(func(v int) {
		// Counting how many items are read from the buffered channel
		iterationCounter.Add(1)
	})

	for idx := range buffered.IndexedIterator {
		// Allowing for the buffer to be filled
		time.Sleep(time.Millisecond * 1)
		require.EqualValues(t, idx+1, iterationCounter.Load())
		require.EqualValues(t, min(idx+4, 10), actuallyReadCounter.Load())
	}
}
