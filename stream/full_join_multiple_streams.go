package stream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

type fullJoinMultipleSortedStreamsProvider[S any, T any] struct {
	comparator shpanstream.Comparator[S]
	joiner     func(values []*S) T
	fullJoinMultipleRun[S]
}

// fullJoinMultipleRun is the state of one materialization; Open replaces it with a fresh one.
type fullJoinMultipleRun[S any] struct {
	nextBuffer []*S
	lastKeys   []*S
}

func FullJoinMultipleSortedStreams[S any, T any](
	s []Stream[S],
	comparator shpanstream.Comparator[S],
	joiner func(values []*S) T,
) Stream[T] {
	if len(s) == 0 {
		return Empty[T]()
	}

	return NewDownMultiStream[S, T](
		s,
		&fullJoinMultipleSortedStreamsProvider[S, T]{comparator: comparator, joiner: joiner},
	)
}

// Open starts each materialization from a fresh run, so nothing an earlier one left behind, whether
// it ran to EOF or was stopped early (FindFirst, Limit, an iterator break), carries over.
func (fjms *fullJoinMultipleSortedStreamsProvider[S, T]) Open(_ context.Context, _ []ProviderFunc[S]) error {
	fjms.fullJoinMultipleRun = fullJoinMultipleRun[S]{}
	return nil
}

func (fjms *fullJoinMultipleSortedStreamsProvider[S, T]) Close() {
}

func (fjms *fullJoinMultipleSortedStreamsProvider[S, T]) Emit(ctx context.Context, srcProviders []ProviderFunc[S]) (T, error) {
	// Initialize buffer on first call
	if fjms.nextBuffer == nil {
		fjms.nextBuffer = make([]*S, len(srcProviders))
		fjms.lastKeys = make([]*S, len(srcProviders))
		for i, currProvider := range srcProviders {
			if ctx.Err() != nil {
				return util.DefaultValue[T](), ctx.Err()
			}
			v, err := currProvider(ctx)
			if err != nil && err != io.EOF {
				return util.DefaultValue[T](), err
			}
			if err == nil {
				fjms.nextBuffer[i] = &v
			}
		}
	}

	// For full join, we emit values until ALL streams are exhausted
	// Keep advancing streams until we find the next value to emit
	for {
		// Pull new values for any streams that have nil buffers and are not done
		for i := 0; i < len(fjms.nextBuffer); i++ {
			if fjms.nextBuffer[i] == nil {
				if ctx.Err() != nil {
					return util.DefaultValue[T](), ctx.Err()
				}
				v, err := srcProviders[i](ctx)
				if err != nil && err != io.EOF {
					return util.DefaultValue[T](), err
				}
				if err == nil {
					fjms.nextBuffer[i] = &v
				}
				// If err == io.EOF, buffer remains nil (stream is done)
			}
		}

		// Check if all streams are done
		allDone := true
		for i := 0; i < len(fjms.nextBuffer); i++ {
			if fjms.nextBuffer[i] != nil {
				allDone = false
				break
			}
		}
		if allDone {
			// All streams are exhausted
			return util.DefaultValue[T](), io.EOF
		}

		// Check if context is done
		if ctx.Err() != nil {
			return util.DefaultValue[T](), ctx.Err()
		}

		// Assert all streams are sorted
		for i := 0; i < len(fjms.nextBuffer); i++ {
			if fjms.nextBuffer[i] != nil && fjms.lastKeys[i] != nil {
				if fjms.comparator(*fjms.nextBuffer[i], *fjms.lastKeys[i]) < 0 {
					return util.DefaultValue[T](), fmt.Errorf("stream %d is not sorted", i)
				}
			}
		}

		// Find the minimum key among all current non-nil values
		var minKey *S
		for i := 0; i < len(fjms.nextBuffer); i++ {
			if fjms.nextBuffer[i] != nil {
				if minKey == nil || fjms.comparator(*fjms.nextBuffer[i], *minKey) < 0 {
					minKey = fjms.nextBuffer[i]
				}
			}
		}

		// Collect all values that match the minimum key
		values := make([]*S, len(fjms.nextBuffer))
		for i := 0; i < len(fjms.nextBuffer); i++ {
			if fjms.nextBuffer[i] != nil && fjms.comparator(*fjms.nextBuffer[i], *minKey) == 0 {
				// This stream has a value matching the minimum key
				values[i] = fjms.nextBuffer[i]
				fjms.lastKeys[i] = fjms.nextBuffer[i]
				fjms.nextBuffer[i] = nil // Clear buffer to fetch next value on next iteration
			}
			// If the stream doesn't have the minimum key, values[i] remains nil
		}

		// Call the joiner function to create the result
		result := fjms.joiner(values)
		return result, nil
	}
}
