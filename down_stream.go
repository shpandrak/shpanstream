package shpanstream

import "context"

type downStreamProviderFunc[S any, T any] func(ctx context.Context, srcProviderFunc StreamProviderFunc[S]) (T, error)

func NewDownStream[S any, T any](
	src Stream[S],
	downStreamProviderFunc downStreamProviderFunc[S, T],
	optOpenFunc func(ctx context.Context, srcProviderFunc StreamProviderFunc[S]) error,
	optCloseFunc func(),
) Stream[T] {
	createStreamOptions := []CreateStreamOption{WithOpenFuncOption(func(ctx context.Context) error {
		err := openSubStream(ctx, src)
		if err != nil {
			return err
		}
		if optOpenFunc != nil {
			return optOpenFunc(ctx, src.provider)
		}
		return nil
	})}

	if optCloseFunc != nil {
		createStreamOptions = append(createStreamOptions, WithCloseFuncOption(optCloseFunc))
	}
	return NewSimpleStream[T](
		func(ctx context.Context) (T, error) {
			return downStreamProviderFunc(ctx, src.provider)
		},
		createStreamOptions...,
	)
}
