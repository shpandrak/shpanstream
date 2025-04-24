package shpanstream

import (
	"context"
	"errors"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

type mergeSortedStreamsProvider[T any] struct {
	streams    []Stream[T]
	comparator func(a, b T) int
	nextBuffer []*T
}

// MergeSortedStreams merges multiple sorted streams into a single sorted stream.
// The streams must be sorted according to the provided comparator function.
func MergeSortedStreams[T any](comparator func(a, b T) int, streams ...Stream[T]) Stream[T] {
	if len(streams) == 0 {
		return EmptyStream[T]()
	}

	return NewStream(&mergeSortedStreamsProvider[T]{
		streams:    streams,
		comparator: comparator,
		nextBuffer: make([]*T, len(streams)),
	})
}

func (ms *mergeSortedStreamsProvider[T]) Open(ctx context.Context) error {
	var allErrors []error

	for _, s := range ms.streams {
		for _, l := range s.allLifecycleElement {
			if err := l.Open(ctx); err != nil {
				allErrors = append(allErrors, err)
			}
		}
	}
	return errors.Join(allErrors...)
}

func (ms *mergeSortedStreamsProvider[T]) Close() {
	for _, s := range ms.streams {
		for _, l := range s.allLifecycleElement {
			l.Close()
		}
	}
}

func (ms *mergeSortedStreamsProvider[T]) Emit(ctx context.Context) (T, error) {
	if ms.nextBuffer == nil {
		ms.nextBuffer = make([]*T, len(ms.streams))
		for i, s := range ms.streams {
			// Always check if the context is done before trying to pull elements from a stream
			if ctx.Err() != nil {
				return util.DefaultValue[T](), ctx.Err()
			}
			v, err := s.provider(ctx)
			if err != nil && err != io.EOF {
				return v, err
			}
			if err == nil {
				ms.nextBuffer[i] = &v
			}
		}
	} else {
		for i, s := range ms.streams {
			if ms.nextBuffer[i] == nil {
				// Always check if the context is done before trying to pull elements from a stream
				if ctx.Err() != nil {
					return util.DefaultValue[T](), ctx.Err()
				}
				v, err := s.provider(ctx)
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
