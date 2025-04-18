package shpanstream

import "context"

func defaultValue[T any]() T {
	var ret T
	return ret
}

type Entry[K any, V any] struct {
	Key   K
	Value V
}

func errMapperToErrCtxMapper[SRC any, TGT any](errMapper func(SRC) (TGT, error)) func(context.Context, SRC) (TGT, error) {
	return func(_ context.Context, src SRC) (TGT, error) {
		return errMapper(src)
	}
}

func mapperToErrCtxMapper[SRC any, TGT any](mapper func(SRC) TGT) func(context.Context, SRC) (TGT, error) {
	return func(_ context.Context, src SRC) (TGT, error) {
		return mapper(src), nil
	}
}
