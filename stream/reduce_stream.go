package stream

import (
	"cmp"
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/lazy"
)

type Reducer[S any, T any] func(Stream[S]) lazy.Lazy[T]

// Reduce consumes the entire stream and combines values using the given reduceFunc,
// starting from the provided initialValue. It returns the final accumulated result.
func Reduce[T any, R any](
	ctx context.Context,
	s Stream[T],
	initialValue R,
	reduceFunc func(acc R, v T) R,
) (R, error) {
	return ReduceWithErr(ctx, s, initialValue, func(acc R, v T) (R, error) {
		return reduceFunc(acc, v), nil
	})
}

// ReduceWithErr consumes the entire stream and combines values using the given reduceFunc,
// starting from the provided initialValue. It returns the final accumulated result.
func ReduceWithErr[T any, R any](
	ctx context.Context,
	s Stream[T],
	initialValue R,
	reduceFunc func(acc R, v T) (R, error),
) (R, error) {
	return ReduceWithErrAndCtx(ctx, s, initialValue, func(_ context.Context, acc R, v T) (R, error) {
		return reduceFunc(acc, v)
	})

}

// ReduceWithErrAndCtx consumes the entire stream and combines values using the given reduceFunc,
// starting from the provided initialValue. It returns the final accumulated result.
func ReduceWithErrAndCtx[T any, R any](
	ctx context.Context,
	s Stream[T],
	initialValue R,
	reduceFunc func(ctx context.Context, acc R, v T) (R, error),
) (R, error) {
	ret := initialValue
	err := s.ConsumeWithErr(ctx, func(v T) error {
		var err error
		ret, err = reduceFunc(ctx, ret, v)
		return err
	})
	if err != nil {
		return util.DefaultValue[R](), err
	}
	return ret, nil
}

func Max[O cmp.Ordered](ctx context.Context, o Stream[O]) (O, error) {
	return Reduce[O](ctx, o, util.DefaultValue[O](), func(acc, v O) O {
		return max(acc, v)
	})
}
func MaxLazy[O cmp.Ordered](o Stream[O]) lazy.Lazy[O] {
	return ReduceLazy[O](o, util.DefaultValue[O](), func(acc, v O) O {
		return max(acc, v)
	})
}
func MinLazy[O cmp.Ordered](o Stream[O]) lazy.Lazy[O] {
	return ReduceLazy[O](o, util.DefaultValue[O](), func(acc, v O) O {
		return min(acc, v)
	})
}

func MustMax[O cmp.Ordered](o Stream[O]) O {
	v, err := Max(context.Background(), o)
	if err != nil {
		panic(err)
	}
	return v
}

func Min[O cmp.Ordered](ctx context.Context, o Stream[O]) (O, error) {
	return Reduce[O](ctx, o, util.DefaultValue[O](), func(acc, v O) O {
		return min(acc, v)
	})
}

func MustMin[O cmp.Ordered](o Stream[O]) O {
	v, err := Min(context.Background(), o)
	if err != nil {
		panic(err)
	}
	return v
}

func ReduceLazy[T any, R any](
	s Stream[T],
	initialValue R,
	reduceFunc func(acc R, v T) R,
) lazy.Lazy[R] {
	return ReduceLazyWithErrAndCtx(s, initialValue, func(_ context.Context, acc R, v T) (R, error) {
		return reduceFunc(acc, v), nil
	})
}

func ReduceLazyWithErr[T any, R any](
	s Stream[T],
	initialValue R,
	reduceFunc func(acc R, v T) (R, error),
) lazy.Lazy[R] {
	return ReduceLazyWithErrAndCtx(s, initialValue, func(_ context.Context, acc R, v T) (R, error) {
		return reduceFunc(acc, v)
	})
}

func ReduceLazyWithErrAndCtx[T any, R any](
	s Stream[T],
	initialValue R,
	reduceFunc func(ctx context.Context, acc R, v T) (R, error),
) lazy.Lazy[R] {
	return lazy.NewLazy(func(ctx context.Context) (R, error) {
		return ReduceWithErrAndCtx(ctx, s, initialValue, reduceFunc)
	})
}

// MustReduce consumes the entire stream and combines values using the given reduceFunc,
// starting from the provided initialValue. It returns the final accumulated result.
// It panics if an error occurs during the reduction process. it should be used only for testing or when the stream is static.
func MustReduce[T any, R any](
	s Stream[T],
	initialValue R,
	reduceFunc func(acc R, v T) R,
) R {
	reduce, err := Reduce(context.Background(), s, initialValue, reduceFunc)
	if err != nil {
		panic(err)
	}
	return reduce
}
