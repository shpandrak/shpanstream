package shpanstream

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
)

func ErrorStream[T any](err error) Stream[T] {
	return newStream[T](func(ctx context.Context) (T, error) {
		return util.DefaultValue[T](), err
	}, []StreamLifecycle{NewStreamLifecycle(func(_ context.Context) error {
		return err
	}, func() {
		// NOP
	})})
}
