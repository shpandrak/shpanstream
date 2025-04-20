package shpanstream

import (
	"context"
)

func ErrorStream[T any](err error) Stream[T] {
	return newStream[T](func(ctx context.Context) (T, error) {
		return defaultValue[T](), err
	}, []StreamLifecycle{NewStreamLifecycle(func(_ context.Context) error {
		return err
	}, func() {
		// NOP
	})})
}
