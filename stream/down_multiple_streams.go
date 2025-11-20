package stream

import "context"

type downMultiStreamProviderFunc[S any, T any] func(ctx context.Context, srcProviderFuncs []ProviderFunc[S]) (T, error)

type DownMultiStreamProvider[SRC any, TGT any] interface {
	Open(ctx context.Context, srcProviderFuncs []ProviderFunc[SRC]) error
	Emit(ctx context.Context, srcProviderFuncs []ProviderFunc[SRC]) (TGT, error)
	Close()
}

func NewDownMultiStream[S any, T any](
	srcs []Stream[S],
	downMultiStreamProvider DownMultiStreamProvider[S, T],
) Stream[T] {
	var srcProviders []ProviderFunc[S]
	b := &unsafeProviderBuilder{}
	for _, src := range srcs {
		addStreamUnsafe(b, src)
	}
	return newUnsafeStream[T](
		b,
		func(ctx context.Context, b *unsafeProviderBuilder) error {
			// Reset srcProviders to support reusability (double collection)
			srcProviders = srcProviders[:0]
			for i := range srcs {
				p, err := openSubStreamUnsafe[S](ctx, b, i)
				if err != nil {
					return err
				}
				srcProviders = append(srcProviders, p)
			}
			// Open the down stream provider
			return downMultiStreamProvider.Open(ctx, srcProviders)
		},
		func(ctx context.Context, _ *unsafeProviderBuilder) (T, error) {
			return downMultiStreamProvider.Emit(ctx, srcProviders)
		},
		downMultiStreamProvider.Close,
	)
}

func NewDownMultiStreamSimple[S any, T any](
	srcs []Stream[S],
	downMultiStreamProviderFunc downMultiStreamProviderFunc[S, T],
) Stream[T] {
	return NewDownMultiStream[S, T](
		srcs,
		simpleDownMultiStreamProvider[S, T]{
			downMultiStreamProviderFunc: downMultiStreamProviderFunc,
		})
}

type simpleDownMultiStreamProvider[S any, T any] struct {
	downMultiStreamProviderFunc downMultiStreamProviderFunc[S, T]
}

func (sd simpleDownMultiStreamProvider[S, T]) Open(_ context.Context, _ []ProviderFunc[S]) error {
	return nil
}

func (sd simpleDownMultiStreamProvider[S, T]) Emit(ctx context.Context, srcProviderFuncs []ProviderFunc[S]) (T, error) {
	return sd.downMultiStreamProviderFunc(ctx, srcProviderFuncs)
}

func (sd simpleDownMultiStreamProvider[S, T]) Close() {
}
