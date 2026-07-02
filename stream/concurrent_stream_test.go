package stream

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConcurrentMap_EarlyStopNoRace is a regression test for a data race where the background
// producer goroutine kept driving the source's provider while teardown closed that same source.
// It only stops racing once the concurrent map joins its goroutines before closing the source.
// Run with -race; without the fix this fails, with the fix it is clean.
func TestConcurrentMap_EarlyStopViaLimitNoRace(t *testing.T) {
	for i := 0; i < 300; i++ {
		out, err := Map(
			Just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			func(v int) int { return v * 2 },
			WithConcurrentMapOption(4),
		).
			Limit(2).
			Collect(context.Background())
		require.NoError(t, err)
		require.Len(t, out, 2)
	}
}

// Early stop via context cancellation (the client-disconnect shape).
func TestConcurrentMap_EarlyStopViaCtxCancelNoRace(t *testing.T) {
	for i := 0; i < 300; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		got := 0
		err := Map(
			Just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			func(v int) int { return v * 2 },
			WithConcurrentMapOption(4),
		).
			Consume(ctx, func(v int) {
				got++
				if got == 2 {
					cancel()
				}
			})
		require.ErrorIs(t, err, context.Canceled)
		cancel()
	}
}

// Full drain must still produce every mapped element (order not guaranteed for concurrent map).
func TestConcurrentMap_FullDrainCorrect(t *testing.T) {
	out, err := Map(
		Just(1, 2, 3, 4, 5, 6, 7, 8),
		func(v int) int { return v * 10 },
		WithConcurrentMapOption(3),
	).Collect(context.Background())
	require.NoError(t, err)
	sort.Ints(out)
	require.Equal(t, []int{10, 20, 30, 40, 50, 60, 70, 80}, out)
}

// The provider now holds per-consumption state; re-consuming the same stream value must work.
func TestConcurrentMap_ReConsumable(t *testing.T) {
	s := Map(Just(1, 2, 3, 4), func(v int) int { return v * 10 }, WithConcurrentMapOption(2))
	for i := 0; i < 3; i++ {
		out, err := s.Collect(context.Background())
		require.NoError(t, err)
		sort.Ints(out)
		require.Equal(t, []int{10, 20, 30, 40}, out)
	}
}

// A mapper error is surfaced.
func TestConcurrentMap_MapperErrorSurfaced(t *testing.T) {
	boom := errors.New("boom")
	_, err := MapWithErr(
		Just(1, 2, 3, 4, 5, 6),
		func(v int) (int, error) {
			if v == 4 {
				return 0, boom
			}
			return v, nil
		},
		WithConcurrentMapOption(3),
	).Collect(context.Background())
	require.ErrorIs(t, err, boom)
}
