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
	return r.Value, r.Err
}

func UnpackResult[T any](r Result[T]) (T, error) {
	return r.Value, r.Err
}

// The ToErrCtx adapters are marked noinline so that each adapter closure is compiled once per
// type (pair): inlined, the compiler emits a copy of the closure body at every call site — and in
// every compiler-generated pointer-receiver wrapper of the generic callers in the stream and lazy
// packages. They run at pipeline construction, never per element.

//go:noinline
func (em MapperWithErr[SRC, TGT]) ToErrCtx() MapperWithErrAndCtx[SRC, TGT] {
	return func(_ context.Context, src SRC) (TGT, error) {
		return em(src)
	}
}

//go:noinline
func (m Mapper[SRC, TGT]) ToErrCtx() MapperWithErrAndCtx[SRC, TGT] {
	return func(_ context.Context, src SRC) (TGT, error) {
		return m(src), nil
	}
}

//go:noinline
func (p Predicate[SRC]) ToErrCtx() PredicateWithErrAndCtx[SRC] {
	return func(_ context.Context, src SRC) (bool, error) {
		return p(src), nil
	}
}

//go:noinline
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
