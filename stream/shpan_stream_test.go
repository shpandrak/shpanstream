package stream

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStream_Filter(t *testing.T) {
	addOne := func(i int) int {
		return i + 1
	}

	require.Len(
		t,
		MustCollect(Just(1, 2, 3, 4, 5).
			Filter(func(i int) bool {
				return i > 2
			})),
		3,
	)

	require.Len(
		t,
		MustCollect(Map(Just(1, 2, 3, 4, 5), addOne).
			Filter(func(i int) bool {
				return i > 2
			})),
		4,
	)

}

func TestWithAdditionalLifecycle_SiblingStreamsDoNotShareBackingArray(t *testing.T) {
	// Grow the lifecycle slice until it has spare capacity, then derive two sibling
	// streams from it: each sibling's appended element must land in its own backing
	// array, not overwrite the other's.
	base := WithAdditionalLifecycle(WithAdditionalLifecycle(Just(1), NewLifecycle(nil, nil)), NewLifecycle(nil, nil))

	var aOpened, bOpened bool
	sa := WithAdditionalLifecycle(base, NewLifecycle(
		func(context.Context) error { aOpened = true; return nil },
		nil,
	))
	sb := WithAdditionalLifecycle(base, NewLifecycle(
		func(context.Context) error { bOpened = true; return nil },
		nil,
	))

	require.Equal(t, []int{1}, MustCollect(sa))
	require.Equal(t, []int{1}, MustCollect(sb))
	require.True(t, aOpened, "sibling A's lifecycle was overwritten by sibling B's append")
	require.True(t, bOpened)
}

func TestStream_MustConsumeMethod(t *testing.T) {
	var got []int
	Just(1, 2, 3).MustConsume(func(v int) { got = append(got, v) })
	require.Equal(t, []int{1, 2, 3}, got)

	// A failing stream panics with its error, after the elements before the failure were consumed.
	boom := errors.New("boom")
	got = nil
	failing := ConcatStreams(Just(1, 2), Error[int](boom))
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		failing.MustConsume(func(v int) { got = append(got, v) })
	}()
	err, isErr := recovered.(error)
	require.True(t, isErr, "MustConsume should panic with the stream's error, got %v", recovered)
	require.ErrorIs(t, err, boom)
	require.Equal(t, []int{1, 2}, got)
}

func TestStream_FilterWithErrAndCtxMethod(t *testing.T) {
	type ctxKey struct{}

	t.Run("keeps the elements the predicate accepts, with the materialization context", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), ctxKey{}, 2)
		res, err := Collect(ctx, Just(1, 2, 3, 4, 5, 6).FilterWithErrAndCtx(func(ctx context.Context, v int) (bool, error) {
			return v%ctx.Value(ctxKey{}).(int) == 0, nil
		}))
		require.NoError(t, err)
		require.Equal(t, []int{2, 4, 6}, res)
	})

	t.Run("a predicate error fails the stream", func(t *testing.T) {
		boom := errors.New("boom")
		var seen []int
		_, err := Collect(context.Background(), Just(1, 2, 3).FilterWithErrAndCtx(func(_ context.Context, v int) (bool, error) {
			seen = append(seen, v)
			if v == 2 {
				return false, boom
			}
			return true, nil
		}))
		require.ErrorIs(t, err, boom)
		require.ErrorContains(t, err, "filter failed")
		require.Equal(t, []int{1, 2}, seen, "no element is pulled after the predicate fails")
	})

	t.Run("a predicate error of io.EOF does not end the stream as a normal completion", func(t *testing.T) {
		_, err := Collect(context.Background(), Just(1, 2).FilterWithErrAndCtx(func(context.Context, int) (bool, error) {
			return false, io.EOF
		}))
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("the predicate observes the context's cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var seen []int
		_, err := Collect(ctx, Just(1, 2, 3).FilterWithErrAndCtx(func(ctx context.Context, v int) (bool, error) {
			seen = append(seen, v)
			if v == 1 {
				cancel()
				return true, nil
			}
			return true, ctx.Err()
		}))
		require.ErrorIs(t, err, context.Canceled)
		require.NotContains(t, seen, 3)
	})
}
