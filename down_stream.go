package shpanstream

import "context"

type downStreamProviderFunc[S any, T any] func(ctx context.Context, srcProviderFunc StreamProviderFunc[S]) (T, error)

func NewDownStream[S any, T any](
	src Stream[S],
	downStreamProviderFunc downStreamProviderFunc[S, T],
	openFunc func(ctx context.Context, srcProviderFunc StreamProviderFunc[S]) error,
) Stream[T] {
	dsLifecycle := NewStreamLifecycle(
		func(ctx context.Context) error {
			err := openSubStream(ctx, src)
			if err != nil {
				return err
			}
			return openFunc(ctx, src.provider)
		}, func() {
			closeSubStream(src)
		})
	return newStream[T](
		func(ctx context.Context) (T, error) {
			return downStreamProviderFunc(ctx, src.provider)
		},
		[]StreamLifecycle{
			dsLifecycle,
		},
	)
}
