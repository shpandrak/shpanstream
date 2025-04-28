package stream

import (
	"context"
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
		FlatMap(Just(1, 5, 2, 3, 4), mapper).MustCollect(),
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
	_, err := FlatMap(Just(1, 5, 2, 3, 4), mapper).Collect(context.Background())
	require.ErrorContains(t, err, "propagate this please")

	// Verify that the elements produced before the error, and not after
	require.EqualValues(t, []int{11, 22}, elementsProduced)

}
