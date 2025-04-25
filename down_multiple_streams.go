package shpanstream

import "context"

type downMultiStreamProviderFunc[S any, T any] func(ctx context.Context, srcProviderFuncs []StreamProviderFunc[S]) (T, error)

type DownMultiStreamProvider[SRC any, TGT any] interface {
	Open(ctx context.Context, srcProviderFuncs []StreamProviderFunc[SRC]) error
	Emit(ctx context.Context, srcProviderFuncs []StreamProviderFunc[SRC]) (TGT, error)
	Close()
}

func NewDownMultiStream[S any, T any](
	srcs []Stream[S],
	downMultiStreamProvider DownMultiStreamProvider[S, T],
) Stream[T] {
	var srcProviders []StreamProviderFunc[S]
	var allLifeCycleEvents []StreamLifecycle

	// First add the lifecycle events of the source streams in order, this is important to separate the lifecycle events
	// So that when opening, if one of the source streams fails, it correctly closes the other streams
	for _, src := range srcs {
		allLifeCycleEvents = append(allLifeCycleEvents, src.allLifecycleElement...)
		srcProviders = append(srcProviders, src.provider)
	}

	// Adding the lifecycle events of the downMultiStreamProvider
	allLifeCycleEvents = append(allLifeCycleEvents,
		NewStreamLifecycle(
			func(ctx context.Context) error {
				return downMultiStreamProvider.Open(ctx, srcProviders)
			},
			downMultiStreamProvider.Close,
		),
	)

	return newStream[T](
		func(ctx context.Context) (T, error) {
			return downMultiStreamProvider.Emit(ctx, srcProviders)
		},
		allLifeCycleEvents,
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

func (sd simpleDownMultiStreamProvider[S, T]) Open(_ context.Context, _ []StreamProviderFunc[S]) error {
	return nil
}

func (sd simpleDownMultiStreamProvider[S, T]) Emit(ctx context.Context, srcProviderFuncs []StreamProviderFunc[S]) (T, error) {
	return sd.downMultiStreamProviderFunc(ctx, srcProviderFuncs)
}

func (sd simpleDownMultiStreamProvider[S, T]) Close() {
}
