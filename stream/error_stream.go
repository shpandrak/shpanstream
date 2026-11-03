package stream

import (
	"context"

	"github.com/shpandrak/shpanstream/internal/util"
)

//go:noinline
func Error[T any](err error) Stream[T] {
	return newStream[T](func(ctx context.Context) (T, error) {
		return util.DefaultValue[T](), err
	}, []Lifecycle{errorLifecycle(err)})
}
