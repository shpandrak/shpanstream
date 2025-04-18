package shpanstream

import (
	"context"
	"io"
)

func EmptyStream[T any]() Stream[T] {
	return newStream(func(ctx context.Context) (T, error) {
		return defaultValue[T](), io.EOF
	}, nil)
}
