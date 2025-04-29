package stream

import "context"

type downStreamProviderFunc[S any, T any] func(ctx context.Context, srcProviderFunc ProviderFunc[S]) (T, error)

type DownStreamProvider[SRC any, TGT any] interface {
	Open(ctx context.Context, srcProviderFunc ProviderFunc[SRC]) error
	Emit(ctx context.Context, srcProviderFunc ProviderFunc[SRC]) (TGT, error)
	Close()
}

func NewDownStream[S any, T any](
	src Stream[S],
	downStreamProvider DownStreamProvider[S, T],
) Stream[T] {
	return NewDownStreamSimple(
		src,
		downStreamProvider.Emit,
		downStreamProvider.Open,
		downStreamProvider.Close,
	)

}

func NewDownStreamSimple[S any, T any](
	src Stream[S],
	emitFunc func(ctx context.Context, srcProviderFunc ProviderFunc[S]) (T, error),
	optOpenFunc func(ctx context.Context, srcProviderFunc ProviderFunc[S]) error,
	optCloseFunc func(),
) Stream[T] {

	b := &unsafeProviderBuilder{}
	addStreamUnsafe(b, src)
	var srcStreamProvider ProviderFunc[S]
	return newUnsafeStream[T](
		b,
		func(ctx context.Context, b *unsafeProviderBuilder) error {
			// Open the source
			var err error
			srcStreamProvider, err = openSubStreamUnsafe[S](ctx, b, 0)
			if err != nil {
				return err
			}

			// Open the open func if provided
			if optOpenFunc != nil {
				return optOpenFunc(ctx, srcStreamProvider)
			}
			return nil
		},
		func(ctx context.Context, _ *unsafeProviderBuilder) (T, error) {
			return emitFunc(ctx, srcStreamProvider)
		},
		optCloseFunc,
	)
}
