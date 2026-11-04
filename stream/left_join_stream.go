package stream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"io"
)

func LeftJoinSortedStreams[L any, R any, KEY any](
	leftStream Stream[L],
	rightStream Stream[R],
	leftKeyFunc func(L) KEY,
	rightKeyFunc func(R) KEY,
	comparator shpanstream.Comparator[KEY],
) Stream[shpanstream.Tuple2[L, *R]] {
	b := &unsafeProviderBuilder{}
	addStreamUnsafe(b, leftStream)
	addStreamUnsafe(b, rightStream)
	var run *sortedJoinRun[L, R, KEY]

	return newUnsafeStream[shpanstream.Tuple2[L, *R]](
		b,
		func(ctx context.Context, b *unsafeProviderBuilder) error {
			run = &sortedJoinRun[L, R, KEY]{firstElement: true}
			var err error
			// Open the left stream
			run.leftStreamProvider, err = openSubStreamUnsafe[L](ctx, b, 0)
			if err != nil {
				return err
			}
			// Open the right stream
			run.rightStreamProvider, err = openSubStreamUnsafe[R](ctx, b, 1)
			if err != nil {
				return err
			}

			return nil
		},
		func(ctx context.Context, b *unsafeProviderBuilder) (shpanstream.Tuple2[L, *R], error) {
			// Check if the context is done
			if ctx.Err() != nil {
				return shpanstream.Tuple2[L, *R]{}, ctx.Err()
			}

			// Get the next element from the left stream, in left join we always emit for every element in the left stream
			leftValue, err := run.leftStreamProvider(ctx)
			if err != nil {
				// This works for EOF among other errors... if we're done return the EOF
				return shpanstream.Tuple2[L, *R]{}, err
			}
			leftKey := leftKeyFunc(leftValue)

			// If this is the first element, we need to pull from the right stream
			if run.firstElement {
				run.firstElement = false

				// Always Check if the context is done before trying to pull elements from the right stream
				if ctx.Err() != nil {
					return shpanstream.Tuple2[L, *R]{}, ctx.Err()
				}

				rightValue, err := run.rightStreamProvider(ctx)
				if err != nil {
					// if EOF, we need to return the left value with a nil right value
					if err == io.EOF {
						run.rightStreamIsDone = true
					} else {
						return shpanstream.Tuple2[L, *R]{}, err
					}
				} else {
					run.lastRightValue = rightValue
					run.lastRightKey = rightKeyFunc(rightValue)
				}

			} else {
				// assert stream is sorted if it is not the first element
				if comparator(leftKey, run.lastLeftKey) < 0 {
					return shpanstream.Tuple2[L, *R]{}, fmt.Errorf("left stream is not sorted %v < %v", leftKey, run.lastLeftKey)
				}

			}
			run.lastLeftKey = leftKey

			// If the right stream is done, we need to return the left value with a nil right value
			if run.rightStreamIsDone {
				return shpanstream.Tuple2[L, *R]{
					A: leftValue,
					B: nil,
				}, nil
			}

			// We need to keep pulling from the right stream until we have a match
			for {
				// keep pulling from the right stream until we find a key that is greater or equal to the left key
				for comparator(leftKey, run.lastRightKey) > 0 {
					// Always Check if the context is done before trying to pull elements from the right stream
					if ctx.Err() != nil {
						return shpanstream.Tuple2[L, *R]{}, ctx.Err()
					}

					rightValue, err := run.rightStreamProvider(ctx)
					if err != nil {
						if err == io.EOF {
							// If EOF, we need to return the left value with a nil right value
							run.rightStreamIsDone = true
							return shpanstream.Tuple2[L, *R]{
								A: leftValue,
								B: nil,
							}, nil
						} else {
							return shpanstream.Tuple2[L, *R]{}, err
						}
					}

					// assert stream is sorted
					rightKey := rightKeyFunc(rightValue)
					if comparator(rightKey, run.lastRightKey) < 0 {
						return shpanstream.Tuple2[L, *R]{}, fmt.Errorf("right stream is not sorted %v < %v", rightKey, run.lastRightKey)
					}
					run.lastRightValue = rightValue
					run.lastRightKey = rightKey
				}

				// Compare the keys of the two elements
				if comparator(run.lastLeftKey, run.lastRightKey) == 0 {
					cpyRightValue := run.lastRightValue // Copy the right value to avoid issues with the pointer being reused
					return shpanstream.Tuple2[L, *R]{
						A: leftValue,
						B: &cpyRightValue,
					}, nil
				}

				// No match and right key is after left key, so we emit the left value with a nil right value
				return shpanstream.Tuple2[L, *R]{
					A: leftValue,
					B: nil,
				}, nil
			}
		},
		nil,
	)

}
