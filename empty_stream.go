package shpanstream

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

func EmptyStream[T any]() Stream[T] {
	return newStream(func(ctx context.Context) (T, error) {
		return util.DefaultValue[T](), io.EOF
	}, nil)
}
