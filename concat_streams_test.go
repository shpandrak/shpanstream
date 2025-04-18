package shpanstream

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestConcatenatedStream(t *testing.T) {
	ctx := context.Background()

	// Create constant streams using Just
	stream1 := Just(1, 2, 3)
	stream2 := Just(4, 5)
	stream3 := EmptyStream[int]()
	stream4 := Just(6)
	stream5 := Just(7, 8)

	cStream := ConcatStreams(stream1, stream2, stream3, stream4, stream5)

	// Assert the results directly
	expected := []int{1, 2, 3, 4, 5, 6, 7, 8}

	// Collect results from the concatenated Stream
	results, err := cStream.Collect(ctx)
	require.NoError(t, err)
	require.EqualValues(t, expected, results)
}

func TestEmptyConcatenatedStream(t *testing.T) {
	ctx := context.Background()

	// Create constant streams using Just
	// Assert the results directly
	var expected []int

	results, err := Concat(Just(EmptyStream[int](), EmptyStream[int](), EmptyStream[int]())).Collect(ctx)
	require.NoError(t, err)
	require.EqualValues(t, expected, results)

	results, err = Concat(EmptyStream[Stream[int]]()).Collect(ctx)
	require.NoError(t, err)
	require.EqualValues(t, expected, results)

	results, err = Concat(Just(EmptyStream[int]())).Collect(ctx)
	require.NoError(t, err)
	require.EqualValues(t, expected, results)
}

func TestErrorConcatenatedStream(t *testing.T) {
	ctx := context.Background()

	_, err := Concat(Just(EmptyStream[int](), NewErrorStream[int](fmt.Errorf("hi")), EmptyStream[int]())).Collect(ctx)
	require.Error(t, err)

	_, err = Concat(NewErrorStream[Stream[Stream[int]]](fmt.Errorf("hi"))).Collect(ctx)
	require.Error(t, err)

	_, err = Concat(Just(NewErrorStream[int](fmt.Errorf("hi")))).Collect(ctx)
	require.Error(t, err)

	_, err = Concat(Just(Just(1), NewErrorStream[int](fmt.Errorf("hi")))).Collect(ctx)
	require.Error(t, err)
	_, err = Concat(Just(NewErrorStream[int](fmt.Errorf("hi")), Just(1))).Collect(ctx)
	require.Error(t, err)

}
