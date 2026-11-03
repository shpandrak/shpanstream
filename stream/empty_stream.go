package stream

import (
	"context"
	"io"

	"github.com/shpandrak/shpanstream/internal/util"
)

//go:noinline
func Empty[T any]() Stream[T] {
	return newStream(func(ctx context.Context) (T, error) {
		return util.DefaultValue[T](), io.EOF
	}, nil)
}
