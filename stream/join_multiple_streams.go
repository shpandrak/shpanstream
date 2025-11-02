package stream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

type joinMultipleSortedStreamsProvider[S any, T any] struct {
	comparator shpanstream.Comparator[S]
	joiner     func(values []S) T
	nextBuffer []*S
	lastKeys   []*S
}

func JoinMultipleSortedStreams[S any, T any](
	s []Stream[S],
	comparator shpanstream.Comparator[S],
	joiner func(values []S) T,
) Stream[T] {
	if len(s) == 0 {
		return Empty[T]()
	}

	jms := &joinMultipleSortedStreamsProvider[S, T]{
		comparator: comparator,
		joiner:     joiner,
		nextBuffer: nil, // Initialize as nil to trigger initialization on first call
		lastKeys:   nil,
	}

	return NewDownMultiStreamSimple(
		s,
		jms.emitJoin,
	)
}

func (jms *joinMultipleSortedStreamsProvider[S, T]) emitJoin(ctx context.Context, srcProviders []ProviderFunc[S]) (T, error) {
	// Initialize buffer on first call
	if jms.nextBuffer == nil {
		jms.nextBuffer = make([]*S, len(srcProviders))
		jms.lastKeys = make([]*S, len(srcProviders))
		for i, currProvider := range srcProviders {
			if ctx.Err() != nil {
				return util.DefaultValue[T](), ctx.Err()
			}
			v, err := currProvider(ctx)
			if err != nil && err != io.EOF {
				return util.DefaultValue[T](), err
			}
			if err == nil {
				jms.nextBuffer[i] = &v
			}
		}
	}

	// For inner join, we need all streams to have values
	// Keep advancing streams until we find a common key across all streams
	for {
		// Pull new values for any streams that have nil buffers
		for i := 0; i < len(jms.nextBuffer); i++ {
			if jms.nextBuffer[i] == nil {
				if ctx.Err() != nil {
					return util.DefaultValue[T](), ctx.Err()
				}
				v, err := srcProviders[i](ctx)
				if err != nil && err != io.EOF {
					return util.DefaultValue[T](), err
				}
				if err == io.EOF {
					// If any stream is done, the join is done
					return util.DefaultValue[T](), io.EOF
				}
				jms.nextBuffer[i] = &v
			}
		}

		// Check if context is done
		if ctx.Err() != nil {
			return util.DefaultValue[T](), ctx.Err()
		}

		// Assert all streams are sorted
		for i := 0; i < len(jms.nextBuffer); i++ {
			if jms.lastKeys[i] != nil {
				if jms.comparator(*jms.nextBuffer[i], *jms.lastKeys[i]) < 0 {
					return util.DefaultValue[T](), fmt.Errorf("stream %d is not sorted", i)
				}
			}
		}

		// Find the maximum key among all current values
		maxKey := jms.nextBuffer[0]
		for i := 1; i < len(jms.nextBuffer); i++ {
			if jms.comparator(*jms.nextBuffer[i], *maxKey) > 0 {
				maxKey = jms.nextBuffer[i]
			}
		}

		// Check if all streams have the same key (all equal to maxKey)
		allMatch := true
		for i := 0; i < len(jms.nextBuffer); i++ {
			if jms.comparator(*jms.nextBuffer[i], *maxKey) != 0 {
				allMatch = false
				break
			}
		}

		if allMatch {
			// We have a match! Collect all values
			values := make([]S, len(jms.nextBuffer))
			for i := 0; i < len(jms.nextBuffer); i++ {
				values[i] = *jms.nextBuffer[i]
				jms.lastKeys[i] = jms.nextBuffer[i]
				jms.nextBuffer[i] = nil // Clear buffer to fetch next value on next iteration
			}

			// Call the joiner function to create the result
			result := jms.joiner(values)
			return result, nil
		}

		// Not all streams match, advance streams that are behind maxKey
		for i := 0; i < len(jms.nextBuffer); i++ {
			if jms.comparator(*jms.nextBuffer[i], *maxKey) < 0 {
				// This stream is behind, advance it
				if ctx.Err() != nil {
					return util.DefaultValue[T](), ctx.Err()
				}
				v, err := srcProviders[i](ctx)
				if err != nil && err != io.EOF {
					return util.DefaultValue[T](), err
				}
				if err == io.EOF {
					// Stream is done, join is done
					return util.DefaultValue[T](), io.EOF
				}
				jms.lastKeys[i] = jms.nextBuffer[i]
				jms.nextBuffer[i] = &v
			}
		}
	}
}
