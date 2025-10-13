package store

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
)

type JsonStreamStore[T any] interface {
	Put(ctx context.Context, value T) error
	PutAll(ctx context.Context, values stream.Stream[T]) error
	ReadStream(reverse bool) stream.Stream[T]
}
