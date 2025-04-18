package shpanstream

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestFlatMapStream(t *testing.T) {
	ctx := context.Background()

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
			return EmptyStream[int]()
		}
	}

	// Create a source Stream using Just
	srcStream := Just(1, 5, 2, 3, 4)

	// Create the FlatMapStream
	flatMapStream := FlatMapStream(srcStream, mapper)

	// Assert the results directly
	expected := []int{10, 11, 20, 21, 30, 31}

	// Collect results from the flat-mapped Stream
	results, err := flatMapStream.Collect(ctx)
	require.NoError(t, err)
	require.EqualValues(t, expected, results)
}
