package shpanstream

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

type mergeSortedStreamsProvider[T any] struct {
	comparator func(a, b T) int
	nextBuffer []*T
}

// MergeSortedStreams merges multiple sorted streams into a single sorted stream.
// The streams must be sorted according to the provided comparator function.
func MergeSortedStreams[T any](comparator func(a, b T) int, streams ...Stream[T]) Stream[T] {
	if len(streams) == 0 {
		return EmptyStream[T]()
	}
	ms := &mergeSortedStreamsProvider[T]{
		comparator: comparator,
		nextBuffer: make([]*T, len(streams)),
	}
	return NewDownMultiStreamSimple(
		streams,
		ms.emitMerged,
	)
}

func (ms *mergeSortedStreamsProvider[T]) emitMerged(ctx context.Context, srcProviders []StreamProviderFunc[T]) (T, error) {
	if ms.nextBuffer == nil {
		ms.nextBuffer = make([]*T, len(srcProviders))
		for i, currProvider := range srcProviders {
			// Always check if the context is done before trying to pull elements from a stream
			if ctx.Err() != nil {
				return util.DefaultValue[T](), ctx.Err()
			}
			v, err := currProvider(ctx)
			if err != nil && err != io.EOF {
				return v, err
			}
			if err == nil {
				ms.nextBuffer[i] = &v
			}
		}
	} else {
		for i, currProvider := range srcProviders {
			if ms.nextBuffer[i] == nil {
				// Always check if the context is done before trying to pull elements from a stream
				if ctx.Err() != nil {
					return util.DefaultValue[T](), ctx.Err()
				}
				v, err := currProvider(ctx)
				if err != nil && err != io.EOF {
					return v, err
				}
				if err == nil {
					ms.nextBuffer[i] = &v
				}
			}
		}
	}

	minIndex := -1
	var minVal *T

	for i, v := range ms.nextBuffer {
		if v != nil {
			if minVal == nil || ms.comparator(*v, *minVal) < 0 {
				minVal = v
				minIndex = i
			}
		}
	}

	if minVal == nil {
		return util.DefaultValue[T](), io.EOF
	}

	ms.nextBuffer[minIndex] = nil
	return *minVal, nil
}
