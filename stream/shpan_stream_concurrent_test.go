package stream

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStream_ConsumeConcurrently(t *testing.T) {
	t.Run("basic concurrent consumption", func(t *testing.T) {
		var count atomic.Int32
		err := Just(1, 2, 3, 4, 5).Consume(context.Background(), func(v int) {
			count.Add(1)
		}, WithConcurrentConsumeOption(3))

		require.NoError(t, err)
		require.Equal(t, int32(5), count.Load())
	})

	t.Run("concurrent consume with error", func(t *testing.T) {
		expectedErr := errors.New("test error")
		var count atomic.Int32

		err := Just(1, 2, 3, 4, 5).ConsumeWithErr(context.Background(), func(v int) error {
			if v == 3 {
				return expectedErr
			}
			count.Add(1)
			return nil
		}, WithConcurrentConsumeOption(1)) // Use concurrency 1 for deterministic error

		require.ErrorIs(t, err, expectedErr)
	})

	t.Run("concurrent consume with context and error", func(t *testing.T) {
		var count atomic.Int32
		err := Just(1, 2, 3, 4, 5).ConsumeWithErrAndCtx(context.Background(), func(ctx context.Context, v int) error {
			count.Add(1)
			return nil
		}, WithConcurrentConsumeOption(2))

		require.NoError(t, err)
		require.Equal(t, int32(5), count.Load())
	})

	t.Run("empty stream", func(t *testing.T) {
		var count atomic.Int32
		err := Empty[int]().Consume(context.Background(), func(v int) {
			count.Add(1)
		}, WithConcurrentConsumeOption(3))

		require.NoError(t, err)
		require.Equal(t, int32(0), count.Load())
	})

	t.Run("concurrency must be positive", func(t *testing.T) {
		err := Just(1, 2, 3).Consume(context.Background(), func(v int) {
		}, WithConcurrentConsumeOption(0))

		require.Error(t, err)
		require.Contains(t, err.Error(), "concurrency must be > 0")
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// Create a stream that will block
		s := NewSimpleStream(func(ctx context.Context) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(10 * time.Second):
				return 1, nil
			}
		})

		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		err := s.Consume(ctx, func(v int) {
		}, WithConcurrentConsumeOption(2))

		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("actual concurrency", func(t *testing.T) {
		// Test that work is actually done concurrently
		var maxConcurrent atomic.Int32
		var current atomic.Int32

		err := Just(1, 2, 3, 4, 5, 6, 7, 8).Consume(context.Background(), func(v int) {
			curr := current.Add(1)
			// Track max concurrent
			for {
				max := maxConcurrent.Load()
				if curr <= max || maxConcurrent.CompareAndSwap(max, curr) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond) // Simulate work
			current.Add(-1)
		}, WithConcurrentConsumeOption(4))

		require.NoError(t, err)
		// With concurrency 4 and 8 items with 50ms work, we should consistently see 3-4 concurrent workers
		require.GreaterOrEqual(t, maxConcurrent.Load(), int32(3))
	})
}
