package stream

import "context"

type downStreamProviderFunc[S any, T any] func(ctx context.Context, srcProviderFunc StreamProviderFunc[S]) (T, error)

type DownStreamProvider[SRC any, TGT any] interface {
	Open(ctx context.Context, srcProviderFunc StreamProviderFunc[SRC]) error
	Emit(ctx context.Context, srcProviderFunc StreamProviderFunc[SRC]) (TGT, error)
	Close()
}

func NewDownStream[S any, T any](
	src Stream[S],
	downStreamProvider DownStreamProvider[S, T],
) Stream[T] {
	createStreamOptions := []CreateStreamOption{WithOpenFuncOption(func(ctx context.Context) error {
		ctx, _, err := doOpenStream(ctx, src)
		if err != nil {
			return err
		}
		return downStreamProvider.Open(ctx, src.provider)
	})}

	createStreamOptions = append(createStreamOptions, WithCloseFuncOption(downStreamProvider.Close))
	return NewSimpleStream[T](
		func(ctx context.Context) (T, error) {
			return downStreamProvider.Emit(ctx, src.provider)
		},
		createStreamOptions...,
	)
}

func NewDownStreamSimple[S any, T any](
	src Stream[S],
	downStreamProviderFunc downStreamProviderFunc[S, T],
) Stream[T] {
	return NewDownStream[S, T](
		src,
		simpleDownStreamProvider[S, T]{
			downStreamProviderFunc: downStreamProviderFunc,
		})
}

type simpleDownStreamProvider[S any, T any] struct {
	downStreamProviderFunc downStreamProviderFunc[S, T]
}

func (sd simpleDownStreamProvider[S, T]) Open(_ context.Context, _ StreamProviderFunc[S]) error {
	return nil
}

func (sd simpleDownStreamProvider[S, T]) Emit(ctx context.Context, srcProviderFunc StreamProviderFunc[S]) (T, error) {
	return sd.downStreamProviderFunc(ctx, srcProviderFunc)
}

func (sd simpleDownStreamProvider[S, T]) Close() {
}
