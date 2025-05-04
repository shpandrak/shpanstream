package shpanstream

import (
	"cmp"
	"context"
)

// Entry defines a key/value pairs.
type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

type Result[T any] struct {
	Value T
	Err   error
}

type Comparable[T any] interface {
	Compare(other T) int
}

type Comparator[T any] func(one, other T) int

func ComparatorForComparable[T Comparable[T]]() Comparator[T] {
	return func(one, other T) int {
		return one.Compare(other)
	}
}

func ComparatorForOrdered[T cmp.Ordered]() Comparator[T] {
	return func(one, other T) int {
		return cmp.Compare(one, other)
	}
}

func (r Result[T]) Unpack() (T, error) {
	return r.Value, nil
}

func UnpackResult[T any](r Result[T]) (T, error) {
	return r.Value, nil
}

func (em MapperWithErr[SRC, TGT]) ToErrCtx() MapperWithErrAndCtx[SRC, TGT] {
	return func(_ context.Context, src SRC) (TGT, error) {
		return em(src)
	}
}

func (m Mapper[SRC, TGT]) ToErrCtx() MapperWithErrAndCtx[SRC, TGT] {
	return func(_ context.Context, src SRC) (TGT, error) {
		return m(src), nil
	}
}

func (p Predicate[SRC]) ToErrCtx() PredicateWithErrAndCtx[SRC] {
	return func(_ context.Context, src SRC) (bool, error) {
		return p(src), nil
	}
}

func (p PredicateWithErr[SRC]) ToErrCtx() PredicateWithErrAndCtx[SRC] {
	return func(_ context.Context, src SRC) (bool, error) {
		return p(src)
	}
}

type Mapper[SRC any, TGT any] func(src SRC) TGT
type MapperWithErr[SRC any, TGT any] func(src SRC) (TGT, error)
type MapperWithErrAndCtx[SRC any, TGT any] func(context.Context, SRC) (TGT, error)

type Predicate[SRC any] Mapper[SRC, bool]
type PredicateWithErr[SRC any] MapperWithErr[SRC, bool]
type PredicateWithErrAndCtx[SRC any] MapperWithErrAndCtx[SRC, bool]
