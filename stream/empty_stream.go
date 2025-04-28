package stream

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

func Empty[T any]() Stream[T] {
	return newStream(func(ctx context.Context) (T, error) {
		return util.DefaultValue[T](), io.EOF
	}, nil)
}
