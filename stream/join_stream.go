package stream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
)

func JoinSortedStreams[L any, R any, KEY any](
	leftStream Stream[L],
	rightStream Stream[R],
	leftKeyFunc func(L) KEY,
	rightKeyFunc func(R) KEY,
	comparator shpanstream.Comparator[KEY],
) Stream[shpanstream.Tuple2[L, R]] {
	b := &unsafeProviderBuilder{}
	addStreamUnsafe(b, leftStream)
	addStreamUnsafe(b, rightStream)
	var leftStreamProvider ProviderFunc[L]
	var rightStreamProvider ProviderFunc[R]
	var err error
	var lastLeftKey KEY
	var lastRightKey KEY
	var lastRightValue R
	firstElement := true

	return newUnsafeStream[shpanstream.Tuple2[L, R]](
		b,
		func(ctx context.Context, b *unsafeProviderBuilder) error {
			// Open the left stream
			leftStreamProvider, err = openSubStreamUnsafe[L](ctx, b, 0)
			if err != nil {
				return err
			}
			// Open the right stream
			rightStreamProvider, err = openSubStreamUnsafe[R](ctx, b, 1)
			if err != nil {
				return err
			}

			return nil
		},
		func(ctx context.Context, b *unsafeProviderBuilder) (shpanstream.Tuple2[L, R], error) {
			// Check if the context is done
			if ctx.Err() != nil {
				return shpanstream.Tuple2[L, R]{}, ctx.Err()
			}

			// Get the next element from the left stream
			leftValue, err := leftStreamProvider(ctx)
			if err != nil {
				// This works for EOF among other errors... if we're done return the EOF
				return shpanstream.Tuple2[L, R]{}, err
			}
			leftKey := leftKeyFunc(leftValue)

			// If this is the first element, we need to pull from the right stream
			if firstElement {
				// Always Check if the context is done before trying to pull elements from the right stream
				if ctx.Err() != nil {
					return shpanstream.Tuple2[L, R]{}, ctx.Err()
				}

				rightValue, err := rightStreamProvider(ctx)
				if err != nil {
					// This works for EOF among other errors... if we're done return the EOF
					return shpanstream.Tuple2[L, R]{}, err
				}

				// assert stream is sorted
				lastRightValue = rightValue
				lastRightKey = rightKeyFunc(rightValue)
				firstElement = false
			} else {
				// assert stream is sorted if it is not the first element
				if comparator(leftKey, lastLeftKey) < 0 {
					return shpanstream.Tuple2[L, R]{}, fmt.Errorf("left stream is not sorted %v < %v", leftKey, lastLeftKey)
				}

			}
			lastLeftKey = leftKey

			// We need to keep pulling from both until we have a match
			for {
				// keep pulling from the right stream until we find a key that is greater or equal to the left key
				for comparator(leftKey, lastRightKey) > 0 {
					// Always Check if the context is done before trying to pull elements from the right stream
					if ctx.Err() != nil {
						return shpanstream.Tuple2[L, R]{}, ctx.Err()
					}

					rightValue, err := rightStreamProvider(ctx)
					if err != nil {
						// This works for EOF among other errors... if we're done return the EOF
						return shpanstream.Tuple2[L, R]{}, err
					}

					// assert stream is sorted
					rightKey := rightKeyFunc(rightValue)
					if comparator(rightKey, lastRightKey) < 0 {
						return shpanstream.Tuple2[L, R]{}, fmt.Errorf("right stream is not sorted %v < %v", rightKey, lastRightKey)
					}
					lastRightValue = rightValue
					lastRightKey = rightKey
				}

				// Compare the keys of the two elements
				if comparator(lastLeftKey, lastRightKey) == 0 {
					return shpanstream.Tuple2[L, R]{
						A: leftValue,
						B: lastRightValue,
					}, nil
				}

				// No match and right key is after left key, so we need to pull from the left stream again
				// always Check if the context is done before trying to pull elements from a stream
				if ctx.Err() != nil {
					return shpanstream.Tuple2[L, R]{}, ctx.Err()
				}
				leftValue, err = leftStreamProvider(ctx)
				if err != nil {
					// This works for EOF among other errors... if we're done return the EOF
					return shpanstream.Tuple2[L, R]{}, err
				}
				leftKey = leftKeyFunc(leftValue)
				// assert stream is sorted
				if comparator(leftKey, lastLeftKey) < 0 {
					return shpanstream.Tuple2[L, R]{}, fmt.Errorf("left stream is not sorted %v < %v", leftKey, lastLeftKey)
				}
				lastLeftKey = leftKey
			}
		},
		nil,
	)

}
