package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStream_FlatMap(t *testing.T) {

	// Define a mapper function that transforms each integer into a Stream of integers
	mapper := func(src int) Stream[int] {
		switch src {
		case 1:
			return Just(10, 11)
		case 2:
			return Just(20, 21)
		case 3:
			return Just(30, 31)
		default:
			return Empty[int]()
		}
	}

	// Assert the results
	require.EqualValues(
		t,
		[]int{10, 11, 20, 21, 30, 31},
		MustCollect(FlatMap(Just(1, 5, 2, 3, 4), mapper)),
	)
}

func TestStream_FlatMap_ErrorPropagation(t *testing.T) {

	var elementsProduced []int

	addToElementsProduced := func(src int) {
		elementsProduced = append(elementsProduced, src)
	}

	// Define a mapper function that transforms each integer into a Stream of integers
	mapper := func(src int) Stream[int] {
		switch src {
		case 1:
			return Just(11).Peek(addToElementsProduced)
		case 2:
			return Just(22).Peek(addToElementsProduced)
		case 3:
			return Error[int](fmt.Errorf("propagate this please"))
		case 4:
			return Just(44).Peek(addToElementsProduced)
		default:
			return Empty[int]()
		}
	}

	// Create a source Stream using Just
	// Create the FlatMap
	_, err := Collect(context.Background(), FlatMap(Just(1, 5, 2, 3, 4), mapper))
	require.ErrorContains(t, err, "propagate this please")

	// Verify that the elements produced before the error, and not after
	require.EqualValues(t, []int{11, 22}, elementsProduced)

}

func TestStream_MapMethod(t *testing.T) {
	require.Equal(t, []string{"1", "2", "3"}, MustCollect(Just(1, 2, 3).Map(func(v int) string { return fmt.Sprint(v) })))

	// Options pass through to the package-level Map.
	require.ElementsMatch(t, []int{2, 4, 6}, MustCollect(Just(1, 2, 3).Map(func(v int) int { return v * 2 }, WithConcurrentMapOption(2))))
}

func TestStream_MapWithErrMethod(t *testing.T) {
	require.Equal(t, []string{"1", "2"}, MustCollect(Just(1, 2).MapWithErr(func(v int) (string, error) { return fmt.Sprint(v), nil })))

	boom := fmt.Errorf("boom")
	_, err := Collect(context.Background(), Just(1, 2).MapWithErr(func(v int) (string, error) { return "", boom }))
	require.ErrorIs(t, err, boom)
}

func TestStream_MapWithErrAndCtxMethod(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, 10)
	res, err := Collect(ctx, Just(1, 2).MapWithErrAndCtx(func(ctx context.Context, v int) (int, error) {
		return v * ctx.Value(ctxKey{}).(int), nil
	}))
	require.NoError(t, err)
	require.Equal(t, []int{10, 20}, res)
}

func TestStream_FlatMapMethod(t *testing.T) {
	require.Equal(t, []int{1, 1, 2, 2}, MustCollect(Just(1, 2).FlatMap(func(v int) Stream[int] { return Just(v, v) })))
}

func TestStream_MapWithErrMethod_ForwardsConcurrentOption(t *testing.T) {
	// Both mapper calls must be in flight at once: only a concurrency of 2 lets them meet.
	meet := newRendezvous(2)
	res, err := Collect(context.Background(), Just(1, 2).MapWithErr(func(v int) (string, error) {
		if !meet() {
			return "", errors.New("mapper calls did not overlap")
		}
		return fmt.Sprint(v), nil
	}, WithConcurrentMapOption(2)))
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"1", "2"}, res)
}

func TestStream_MapWithErrAndCtxMethod_ForwardsConcurrentOption(t *testing.T) {
	meet := newRendezvous(2)
	res, err := Collect(context.Background(), Just(1, 2).MapWithErrAndCtx(func(ctx context.Context, v int) (int, error) {
		if !meet() {
			return 0, errors.New("mapper calls did not overlap")
		}
		return v * 10, nil
	}, WithConcurrentMapOption(2)))
	require.NoError(t, err)
	require.ElementsMatch(t, []int{10, 20}, res)
}

func TestStream_MapMethods_ForwardOptions(t *testing.T) {
	// An option the methods dropped would leave these streams succeeding.
	_, err := Collect(context.Background(), Just(1).MapWithErr(func(v int) (int, error) { return v, nil }, unsupportedMapOption{}))
	require.ErrorContains(t, err, "unsupported map stream option type")

	_, err = Collect(context.Background(), Just(1).MapWithErrAndCtx(func(_ context.Context, v int) (int, error) { return v, nil }, unsupportedMapOption{}))
	require.ErrorContains(t, err, "unsupported map stream option type")
}
