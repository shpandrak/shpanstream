package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDoFirst_RunsBeforeSourceOpen(t *testing.T) {
	var order []string

	out, err := Just(1, 2, 3).
		WithAdditionalLifecycle(NewLifecycle(
			func(ctx context.Context) error { order = append(order, "source-open"); return nil },
			func() { order = append(order, "source-close") },
		)).
		DoFirst(func(context.Context) { order = append(order, "first") }).
		Collect(context.Background())

	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, out)
	require.Equal(t, []string{"first", "source-open", "source-close"}, order)
}

func TestDoFirst_OpenFailureHookRanAndDoFinallyGetsError(t *testing.T) {
	boom := errors.New("boom")
	var firstCalls, finallyCalls int
	var got error

	_, err := Error[int](boom).
		DoFirst(func(context.Context) { firstCalls++ }).
		DoFinally(func(e error) { finallyCalls++; got = e }).
		Collect(context.Background())

	require.Error(t, err)
	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, firstCalls, "timer must be armed even when source Open fails")
	require.Equal(t, 1, finallyCalls)
	require.ErrorIs(t, got, boom)
}

func TestDoFirst_FiresPerConsumption(t *testing.T) {
	var calls int
	s := Just(1).DoFirst(func(context.Context) { calls++ })

	_, err := s.Collect(context.Background())
	require.NoError(t, err)
	_, err = s.Collect(context.Background())
	require.NoError(t, err)

	require.Equal(t, 2, calls)
}

func TestDoFirst_PanickingHookDoesNotFailDrain(t *testing.T) {
	out, err := Just(1, 2).
		DoFirst(func(context.Context) { panic("kaboom") }).
		Collect(context.Background())

	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, out)
}

func TestDoFirst_MultipleHooksLastDeclaredRunsFirst(t *testing.T) {
	var order []string

	_, err := Just(1).
		DoFirst(func(context.Context) { order = append(order, "a") }).
		DoFirst(func(context.Context) { order = append(order, "b") }).
		Collect(context.Background())

	require.NoError(t, err)
	require.Equal(t, []string{"b", "a"}, order)
}

func TestDoFirst_StaysFirstUnderDownstreamOperators(t *testing.T) {
	var order []string

	s := Just(1, 2, 3).
		DoFirst(func(context.Context) { order = append(order, "first") }).
		WithAdditionalLifecycle(NewLifecycle(
			func(ctx context.Context) error { order = append(order, "later-open"); return nil },
			nil,
		))
	out, err := s.Filter(func(i int) bool { return i > 1 }).Collect(context.Background())

	require.NoError(t, err)
	require.Equal(t, []int{2, 3}, out)
	require.Equal(t, []string{"first", "later-open"}, order)
}
