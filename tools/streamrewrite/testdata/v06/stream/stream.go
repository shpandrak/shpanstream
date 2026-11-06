// Package stream is a stub of the v0.6 API: the golden output must compile against it.
package stream

import (
	"context"
	"iter"
	"sync"
)

type Stream[T any] struct{ items []T }

type Lifecycle interface{ Close() }

func FromSlice[T any](items []T) Stream[T] { return Stream[T]{items: items} }

func Collect[T any](ctx context.Context, s Stream[T]) ([]T, error)                       { return s.items, nil }
func MustCollect[T any](s Stream[T]) []T                                                 { return s.items }
func Count[T any](ctx context.Context, s Stream[T]) (int, error)                         { return len(s.items), nil }
func MustCount[T any](s Stream[T]) int                                                   { return len(s.items) }
func IsEmpty[T any](ctx context.Context, s Stream[T]) (bool, error)                      { return len(s.items) == 0, nil }
func FindFirst[T any](ctx context.Context, s Stream[T]) (first T, found bool, err error) { return }
func FindLast[T any](ctx context.Context, s Stream[T]) (last T, found bool, err error)   { return }
func CollectRandomSample[T any](ctx context.Context, s Stream[T], n int) ([]T, error) {
	return nil, nil
}
func Limit[T any](s Stream[T], limit int) Stream[T]                           { return s }
func Skip[T any](s Stream[T], skip int) Stream[T]                             { return s }
func Page[T any](s Stream[T], pageNum, pageSize int) Stream[T]                { return s }
func RandomSample[T any](s Stream[T], n int) Stream[T]                        { return s }
func WithAdditionalLifecycle[T any](s Stream[T], l Lifecycle) Stream[T]       { return s }
func WithLockWhileMaterializing[T any](s Stream[T], mu sync.Locker) Stream[T] { return s }
func Untyped[T any](s Stream[T]) Stream[any]                                  { return Stream[any]{} }
func MustValues[T any](s Stream[T]) iter.Seq[T]                               { return func(func(T) bool) {} }
func MustAll[T any](s Stream[T]) iter.Seq2[int, T]                            { return func(func(int, T) bool) {} }

func (s Stream[T]) Filter[P ~func(T) bool](p P) Stream[T] { return s }
func (s Stream[T]) FilterWithErrAndCtx[P ~func(context.Context, T) (bool, error)](p P) Stream[T] {
	return s
}
