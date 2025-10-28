package stream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

type leftJoinMultipleSortedStreamsProvider[S any, T any] struct {
	comparator  shpanstream.Comparator[S]
	joiner      func(left S, others []*S) T
	nextBuffer  []*S
	lastLeftKey *S
}

func LeftJoinMultipleSortedStreams[S any, T any](
	s []Stream[S],
	comparator shpanstream.Comparator[S],
	joiner func(left S, others []*S) T,
) Stream[T] {
	if len(s) == 0 {
		return Empty[T]()
	}

	ljms := &leftJoinMultipleSortedStreamsProvider[S, T]{
		comparator: comparator,
		joiner:     joiner,
		nextBuffer: nil, // Initialize as nil to trigger initialization on first call
	}

	return NewDownMultiStreamSimple(
		s,
		ljms.emitLeftJoin,
	)
}

func (ljms *leftJoinMultipleSortedStreamsProvider[S, T]) emitLeftJoin(ctx context.Context, srcProviders []ProviderFunc[S]) (T, error) {
	// Initialize buffer on first call
	if ljms.nextBuffer == nil {
		ljms.nextBuffer = make([]*S, len(srcProviders))
		for i, currProvider := range srcProviders {
			if ctx.Err() != nil {
				return util.DefaultValue[T](), ctx.Err()
			}
			v, err := currProvider(ctx)
			if err != nil && err != io.EOF {
				return util.DefaultValue[T](), err
			}
			if err == nil {
				ljms.nextBuffer[i] = &v
			}
		}
	}

	// Get the next element from the left stream (index 0)
	// In a left join, we must have an element from the left stream
	if ljms.nextBuffer[0] == nil {
		if ctx.Err() != nil {
			return util.DefaultValue[T](), ctx.Err()
		}
		leftValue, err := srcProviders[0](ctx)
		if err != nil {
			// If left stream is done, we're done
			return util.DefaultValue[T](), err
		}
		ljms.nextBuffer[0] = &leftValue
	}

	leftValue := *ljms.nextBuffer[0]

	// Assert that the left stream is sorted
	if ljms.lastLeftKey != nil {
		if ljms.comparator(leftValue, *ljms.lastLeftKey) < 0 {
			return util.DefaultValue[T](), fmt.Errorf("left stream is not sorted")
		}
	}
	ljms.lastLeftKey = &leftValue

	// Advance other streams to catch up to the left stream's key
	others := make([]*S, len(srcProviders)-1)
	for i := 1; i < len(srcProviders); i++ {
		// Skip streams that are already done
		if ljms.nextBuffer[i] == nil {
			continue
		}

		// Advance this stream while its key is less than the left key
		for ljms.nextBuffer[i] != nil && ljms.comparator(*ljms.nextBuffer[i], leftValue) < 0 {
			if ctx.Err() != nil {
				return util.DefaultValue[T](), ctx.Err()
			}
			v, err := srcProviders[i](ctx)
			if err != nil && err != io.EOF {
				return util.DefaultValue[T](), err
			}
			if err == io.EOF {
				ljms.nextBuffer[i] = nil
			} else {
				ljms.nextBuffer[i] = &v
			}
		}

		// If we have a match, include it in the others slice
		if ljms.nextBuffer[i] != nil && ljms.comparator(*ljms.nextBuffer[i], leftValue) == 0 {
			others[i-1] = ljms.nextBuffer[i]
		}
	}

	// Clear the left buffer for the next iteration
	ljms.nextBuffer[0] = nil

	// Call the joiner function to create the result
	result := ljms.joiner(leftValue, others)
	return result, nil
}
