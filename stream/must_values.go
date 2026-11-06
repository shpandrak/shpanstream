package stream

import (
	"context"
	"io"
	"iter"
)

// MustValues returns a range-over-func iterator over the stream's elements:
// for v := range stream.MustValues(s) { ... }. Breaking out of the loop stops the stream and closes
// it; no further element is pulled. It panics if the stream fails, hence the Must prefix.
func MustValues[T any](s Stream[T]) iter.Seq[T] {
	return func(yield func(T) bool) { iterate(s, yield) }
}

//go:noinline
func iterate[T any](s Stream[T], yield func(T) bool) {
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

// MustAll is MustValues with each element's zero-based index:
// for i, v := range stream.MustAll(s) { ... }. Like MustValues, it panics if the stream fails.
func MustAll[T any](s Stream[T]) iter.Seq2[int, T] {
	return func(yield func(int, T) bool) { indexedIterate(s, yield) }
}

//go:noinline
func indexedIterate[T any](s Stream[T], yield func(int, T) bool) {
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
