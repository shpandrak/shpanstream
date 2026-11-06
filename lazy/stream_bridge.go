package lazy

import (
	"cmp"
	"context"
	"errors"

	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/stream"
)

// This file bridges package stream and Lazy. Package stream does not depend on lazy (a Stream
// element type would otherwise compile all of Lazy's methods, see
// docs/stream-api-generic-methods.md), so the Lazy-returning helpers over streams live here.
// Each is a thin wrapper over the stream API; code that does not otherwise need a Lazy should call
// that API directly.

// FindFirst returns a Lazy of the stream's first element, empty when the stream is empty (Get then
// fails). stream.FindFirst(ctx, s) is the direct form.
func FindFirst[T any](s stream.Stream[T]) Lazy[T] {
	return NewLazyOptional(func(ctx context.Context) (*T, error) {
		return optional(stream.FindFirst(ctx, s))
	}).OrElseThrow(errNoFirstElement)
}

// FindLast returns a Lazy of the stream's last element, empty when the stream is empty (Get then
// fails). stream.FindLast(ctx, s) is the direct form.
func FindLast[T any](s stream.Stream[T]) Lazy[T] {
	return NewLazyOptional(func(ctx context.Context) (*T, error) {
		return optional(stream.FindLast(ctx, s))
	}).OrElseThrow(errNoLastElement)
}

// FindFirstAndLast returns a Lazy of the stream's first and last elements, empty when the stream is
// empty (Get then fails). stream.FindFirstAndLast(ctx, s) is the direct form.
func FindFirstAndLast[T any](s stream.Stream[T]) Lazy[shpanstream.Tuple2[T, T]] {
	return NewLazyOptional(func(ctx context.Context) (*shpanstream.Tuple2[T, T], error) {
		return optional(stream.FindFirstAndLast(ctx, s))
	}).OrElseThrow(errNoFirstAndLastElement)
}

// ToStream converts the Lazy to a Stream of at most one element: the value, nothing when the Lazy
// is empty, or the Lazy's error. It re-reads the Lazy on every materialization of the stream.
func ToStream[T any](l Lazy[T]) stream.Stream[T] {
	return stream.FromOptionalProvider(func(ctx context.Context) (T, bool, error) {
		p, err := l.GetOptional(ctx)
		if err != nil || p == nil {
			var zero T
			return zero, false, err
		}
		return *p, true, nil
	})
}

// Reduce returns a Lazy of stream.Reduce over s. It replaces stream.ReduceLazy.
func Reduce[T any, R any](s stream.Stream[T], initialValue R, reduceFunc func(acc R, v T) R) Lazy[R] {
	return NewLazy(func(ctx context.Context) (R, error) {
		return stream.Reduce(ctx, s, initialValue, reduceFunc)
	})
}

// ReduceWithErr returns a Lazy of stream.ReduceWithErr over s. It replaces stream.ReduceLazyWithErr.
func ReduceWithErr[T any, R any](s stream.Stream[T], initialValue R, reduceFunc func(acc R, v T) (R, error)) Lazy[R] {
	return NewLazy(func(ctx context.Context) (R, error) {
		return stream.ReduceWithErr(ctx, s, initialValue, reduceFunc)
	})
}

// ReduceWithErrAndCtx returns a Lazy of stream.ReduceWithErrAndCtx over s. It replaces
// stream.ReduceLazyWithErrAndCtx.
func ReduceWithErrAndCtx[T any, R any](
	s stream.Stream[T],
	initialValue R,
	reduceFunc func(ctx context.Context, acc R, v T) (R, error),
) Lazy[R] {
	return NewLazy(func(ctx context.Context) (R, error) {
		return stream.ReduceWithErrAndCtx(ctx, s, initialValue, reduceFunc)
	})
}

// Max returns a Lazy of stream.Max over s. It replaces stream.MaxLazy.
func Max[O cmp.Ordered](s stream.Stream[O]) Lazy[O] {
	return NewLazy(func(ctx context.Context) (O, error) {
		return stream.Max(ctx, s)
	})
}

// Min returns a Lazy of stream.Min over s. It replaces stream.MinLazy.
func Min[O cmp.Ordered](s stream.Stream[O]) Lazy[O] {
	return NewLazy(func(ctx context.Context) (O, error) {
		return stream.Min(ctx, s)
	})
}

// optional adapts a (value, found, error) result to the (*T, error) shape of NewLazyOptional.
func optional[T any](v T, found bool, err error) (*T, error) {
	if err != nil || !found {
		return nil, err
	}
	return &v, nil
}

// The empty-value errors the bridges' Lazies fail with.

func errNoFirstElement() error {
	return errors.New("no \"first element\" in an empty stream")
}

func errNoLastElement() error {
	return errors.New("no \"last element\" in an empty stream")
}

func errNoFirstAndLastElement() error {
	return errors.New("no \"first and last element\" in an empty stream")
}
