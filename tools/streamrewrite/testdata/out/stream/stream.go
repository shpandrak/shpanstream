// Package stream is a stub of the v0.5 API: just the signatures the rewriter looks at.
package stream

import (
	"context"
	"sync"
)

type Stream[T any] struct{ items []T }

type Lifecycle interface{ Close() }

func FromSlice[T any](items []T) Stream[T] { return Stream[T]{items: items} }

func (s Stream[T]) Collect(ctx context.Context) ([]T, error)                       { return s.items, nil }
func (s Stream[T]) MustCollect() []T                                               { return s.items }
func (s Stream[T]) Count(ctx context.Context) (int, error)                         { return len(s.items), nil }
func (s Stream[T]) MustCount() int                                                 { return len(s.items) }
func (s Stream[T]) IsEmpty(ctx context.Context) (bool, error)                      { return len(s.items) == 0, nil }
func (s Stream[T]) FindFirst(ctx context.Context) (first T, found bool, err error) { return }
func (s Stream[T]) FindLast(ctx context.Context) (last T, found bool, err error)   { return }
func (s Stream[T]) CollectRandomSample(ctx context.Context, n int) ([]T, error)    { return nil, nil }
func (s Stream[T]) Limit(limit int) Stream[T]                                      { return s }
func (s Stream[T]) Skip(skip int) Stream[T]                                        { return s }
func (s Stream[T]) RandomSample(n int) Stream[T]                                   { return s }
func (s Stream[T]) WithAdditionalLifecycle(l Lifecycle) Stream[T]                  { return s }
func (s Stream[T]) WithLockWhileMaterializing(mu sync.Locker) Stream[T]            { return s }
func (s Stream[T]) Untyped() Stream[any]                                           { return Stream[any]{} }
func (s Stream[T]) Iterator(yield func(T) bool)                                    {}
func (s Stream[T]) IndexedIterator(yield func(int, T) bool)                        {}
func (s Stream[T]) Filter(p func(T) bool) Stream[T]                                { return s }
func (s Stream[T]) FilterWithErAndCtx(p func(context.Context, T) (bool, error)) Stream[T] {
	return s
}

// Page shows a call site inside package stream itself: no qualifier.
func (s Stream[T]) Page(pageNum int, pageSize int) Stream[T] {
	return Limit(Skip(s, pageNum*pageSize), pageSize)
}
