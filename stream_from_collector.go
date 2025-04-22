package shpanstream

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

// newStreamFromCollector this allows to create a stream from a collector function. resulting in a delayed materialization of the stream.
func newStreamFromCollector[S any, T any](
	src Stream[S],
	collector func(ctx context.Context, src Stream[S]) ([]T, error),
) Stream[T] {
	return NewStream[T](
		&manipulatedStream[S, T]{src: src, collector: collector},
	)
}

type manipulatedStream[S any, T any] struct {
	src       Stream[S]
	collector func(ctx context.Context, src Stream[S]) ([]T, error)
	collected *[]T
}

func (m *manipulatedStream[S, T]) Open(ctx context.Context) error {
	collected, err := m.collector(ctx, m.src)
	if err != nil {
		return err
	}
	m.collected = &collected
	return nil
}

func (m *manipulatedStream[S, T]) Close() {
	m.collected = nil
}

func (m *manipulatedStream[S, T]) Emit(ctx context.Context) (T, error) {
	if ctx.Err() != nil {
		return util.DefaultValue[T](), ctx.Err()
	}
	if len(*m.collected) == 0 {
		return util.DefaultValue[T](), io.EOF
	}
	v := (*m.collected)[0]
	*m.collected = (*m.collected)[1:]
	return v, nil
}
