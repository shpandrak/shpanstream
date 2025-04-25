package shpanstream

import "context"

// Entry defines a key/value pairs.
type Entry[K comparable, V any] struct {
	Key   K
	Value V
}

type Result[T any] struct {
	Value T
	Err   error
}

func (r Result[T]) Unpack() (T, error) {
	return r.Value, nil

}
func mapperErrToErrCtx[SRC any, TGT any](errMapper MapperWithErr[SRC, TGT]) MapperWithErrAndCtx[SRC, TGT] {
	return func(_ context.Context, src SRC) (TGT, error) {
		return errMapper(src)
	}
}

func mapperToErrCtx[SRC any, TGT any](mapper Mapper[SRC, TGT]) MapperWithErrAndCtx[SRC, TGT] {
	return func(_ context.Context, src SRC) (TGT, error) {
		return mapper(src), nil
	}
}

func predicateToErrCtx[SRC any](predicate Predicate[SRC]) PredicateWithErrAndCtx[SRC] {
	return func(_ context.Context, src SRC) (bool, error) {
		return predicate(src), nil
	}
}

func predicateErrToErrCtx[SRC any](predicate PredicateWithErr[SRC]) PredicateWithErrAndCtx[SRC] {
	return func(_ context.Context, src SRC) (bool, error) {
		return predicate(src)
	}
}

type Mapper[SRC any, TGT any] func(src SRC) TGT
type MapperWithErr[SRC any, TGT any] func(src SRC) (TGT, error)
type MapperWithErrAndCtx[SRC any, TGT any] func(context.Context, SRC) (TGT, error)

type Predicate[SRC any] Mapper[SRC, bool]
type PredicateWithErr[SRC any] MapperWithErr[SRC, bool]
type PredicateWithErrAndCtx[SRC any] MapperWithErrAndCtx[SRC, bool]
