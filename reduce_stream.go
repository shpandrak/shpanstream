package shpanstream

import (
	"cmp"
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
)

func ReduceStream[T any, R any](
	ctx context.Context,
	s Stream[T],
	initialValue R,
	reduceFunc func(acc R, v T) R,
) (R, error) {
	return ReduceStreamWithErr(ctx, s, initialValue, func(acc R, v T) (R, error) {
		return reduceFunc(acc, v), nil
	})
}

func ReduceStreamWithErr[T any, R any](
	ctx context.Context,
	s Stream[T],
	initialValue R,
	reduceFunc func(acc R, v T) (R, error),
) (R, error) {
	return ReduceStreamWithErrAndCtx(ctx, s, initialValue, func(_ context.Context, acc R, v T) (R, error) {
		return reduceFunc(acc, v)
	})

}

func ReduceStreamWithErrAndCtx[T any, R any](
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
	return ReduceStream[O](ctx, o, util.DefaultValue[O](), func(acc, v O) O {
		return max(acc, v)
	})
}

func MaxMust[O cmp.Ordered](o Stream[O]) O {
	v, err := Max(context.Background(), o)
	if err != nil {
		panic(err)
	}
	return v
}

func Min[O cmp.Ordered](ctx context.Context, o Stream[O]) (O, error) {
	return ReduceStream[O](ctx, o, util.DefaultValue[O](), func(acc, v O) O {
		return min(acc, v)
	})
}

func MinMust[O cmp.Ordered](o Stream[O]) O {
	v, err := Min(context.Background(), o)
	if err != nil {
		panic(err)
	}
	return v
}
