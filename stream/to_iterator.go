package stream

import (
	"context"
	"io"
)

// Iterator is a range-over-func iterator over the stream: for v := range s.Iterator { ... }.
// Breaking out of the loop stops the stream and closes it; no further element is pulled. It
// panics if the stream fails.
//
//go:noinline
func (s Stream[T]) Iterator(yield func(T) bool) {
	err := consumeLoop(context.Background(), s.allLifecycleElement, func(ctx context.Context) (bool, error) {
		v, err := s.provider(ctx)
		if err != nil {
			return false, err
		}
		if !yield(v) {
			// The loop body broke out: end the drain as a normal completion.
			return false, io.EOF
		}
		return true, nil
	})
	if err != nil {
		panic(err)
	}
}

// IndexedIterator is Iterator with the element's zero-based index:
// for i, v := range s.IndexedIterator { ... }.
//
//go:noinline
func (s Stream[T]) IndexedIterator(yield func(int, T) bool) {
	index := -1
	err := consumeLoop(context.Background(), s.allLifecycleElement, func(ctx context.Context) (bool, error) {
		v, err := s.provider(ctx)
		if err != nil {
			return false, err
		}
		index++
		if !yield(index, v) {
			return false, io.EOF
		}
		return true, nil
	})
	if err != nil {
		panic(err)
	}
}
