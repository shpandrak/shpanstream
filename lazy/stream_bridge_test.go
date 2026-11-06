package lazy

import (
	"context"
	"errors"
	"testing"

	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/stretchr/testify/require"
)

func TestFindFirstBridge(t *testing.T) {
	addOne := func(i int) int { return i + 1 }
	require.Equal(t, 1, FindFirst(stream.Just(1, 2, 3, 4, 5)).MustGet())
	require.Equal(t, 2, Map(FindFirst(stream.Just(1, 2, 3, 4, 5)), addOne).MustGet())
	require.Nil(t, FindFirst(stream.Empty[int]()).MustGetOptional())
	_, err := FindFirst(stream.Empty[int]()).Get(context.Background())
	require.EqualError(t, err, `no "first element" in an empty stream`)

	boom := errors.New("boom")
	_, err = FindFirst(stream.Error[int](boom)).GetOptional(context.Background())
	require.ErrorIs(t, err, boom)
}

func TestFindLastBridge(t *testing.T) {
	require.Equal(t, 5, FindLast(stream.Just(1, 2, 3, 4, 5)).MustGet())
	require.Nil(t, FindLast(stream.Empty[int]()).MustGetOptional())
	_, err := FindLast(stream.Empty[int]()).Get(context.Background())
	require.EqualError(t, err, `no "last element" in an empty stream`)
}

func TestFindFirstAndLastBridge(t *testing.T) {
	require.Equal(t, shpanstream.Tuple2[int, int]{A: 1, B: 3}, FindFirstAndLast(stream.Just(1, 2, 3)).MustGet())
	_, err := FindFirstAndLast(stream.Empty[int]()).Get(context.Background())
	require.EqualError(t, err, `no "first and last element" in an empty stream`)
}

func TestToStream(t *testing.T) {
	require.Equal(t, []int{7}, stream.MustCollect(ToStream(Just(7))))
	require.Empty(t, stream.MustCollect(ToStream(Empty[int]())))
	// An OrElseThrow Lazy that is empty is still an empty stream, as with stream.FromLazy.
	require.Empty(t, stream.MustCollect(ToStream(Empty[int]().OrElseThrow(func() error { return errors.New("x") }))))
	boom := errors.New("boom")
	_, err := stream.Collect(context.Background(), ToStream(Error[int](boom)))
	require.Equal(t, boom, err)

	// Each materialization fetches the Lazy again.
	calls := 0
	s := ToStream(NewLazy(func(ctx context.Context) (int, error) { calls++; return calls, nil }))
	require.Equal(t, []int{1}, stream.MustCollect(s))
	require.Equal(t, []int{2}, stream.MustCollect(s))
}

func TestReduceBridges(t *testing.T) {
	sum := func(acc, v int) int { return acc + v }
	require.Equal(t, 15, Reduce(stream.Just(1, 2, 3, 4, 5), 0, sum).MustGet())
	require.Equal(t, 15, ReduceWithErr(stream.Just(1, 2, 3, 4, 5), 0, func(acc, v int) (int, error) { return acc + v, nil }).MustGet())
	require.Equal(t, 15, ReduceWithErrAndCtx(stream.Just(1, 2, 3, 4, 5), 0, func(_ context.Context, acc, v int) (int, error) { return acc + v, nil }).MustGet())
	require.Equal(t, 5, Max(stream.Just(3, 5, 1)).MustGet())
	// stream.Min starts from the zero value, so it is only meaningful with negatives here.
	require.Equal(t, -3, Min(stream.Just(-1, 5, -3)).MustGet())

	boom := errors.New("boom")
	_, err := ReduceWithErr(stream.Just(1, 2), 0, func(acc, v int) (int, error) { return 0, boom }).Get(context.Background())
	require.ErrorIs(t, err, boom)
}
