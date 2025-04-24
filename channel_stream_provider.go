package shpanstream

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
	"log/slog"
)

type chanelStreamProvider[T any] struct {
	originalChannel <-chan T
}

func (cp chanelStreamProvider[T]) Open(_ context.Context) error {
	return nil
}

func (cp chanelStreamProvider[T]) Close() {
}

func (cp chanelStreamProvider[T]) Emit(ctx context.Context) (T, error) {
	select {
	case <-ctx.Done():
		return util.DefaultValue[T](), ctx.Err()
	case msg, stillGood := <-cp.originalChannel:
		if !stillGood {
			slog.Debug("Stream channel closed externally")
			return util.DefaultValue[T](), io.EOF
		}
		return msg, nil
	}
}

func StreamFromChannel[T any](ch <-chan T) Stream[T] {
	return NewStream[T](&chanelStreamProvider[T]{
		originalChannel: ch,
	})
}
