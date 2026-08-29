package stream

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
	"slices"
)

func Just[T any](slice ...T) Stream[T] {
	return NewStream(&justStream[T]{slc: slices.Clone(slice)})
}

func FromSlice[T any](slice []T) Stream[T] {
	return NewStream(&justStream[T]{slc: slices.Clone(slice)})
}

type justStream[T any] struct {
	slc []T
	idx int
}

func (j *justStream[T]) Open(_ context.Context) error {
	j.idx = 0
	return nil
}

func (j *justStream[T]) Close() {
	j.idx = 0
}

func (j *justStream[T]) Emit(ctx context.Context) (T, error) {
	if ctx.Err() != nil {
		return util.DefaultValue[T](), ctx.Err()
	}
	if j.idx >= len(j.slc) {
		return util.DefaultValue[T](), io.EOF
	}
	v := j.slc[j.idx]
	j.idx++
	return v, nil
}

// FromSliceProvider creates a Stream from a function providing the slice to stream.
// The provider function is invoked lazily, only when the stream is materialized (and once per materialization),
// making it useful for streaming a slice that is expensive to fetch, e.g. a database query or an API call.
// The provider function receives the materialization context, and an error it returns is propagated to the stream consumer.
// Unlike FromSlice, the provided slice is not cloned, since the provider function is expected
// to hand over the ownership of the returned slice to the stream.
// A provider function returning a cached slice must therefore not mutate it while the stream is being consumed.
func FromSliceProvider[T any](provider func(ctx context.Context) ([]T, error)) Stream[T] {
	return NewStream(&sliceProviderStream[T]{provider: provider})
}

type sliceProviderStream[T any] struct {
	// justStream is embedded to share the slice emitting logic
	justStream[T]
	provider func(ctx context.Context) ([]T, error)
}

func (s *sliceProviderStream[T]) Open(ctx context.Context) error {
	slc, err := s.provider(ctx)
	if err != nil {
		return err
	}
	s.slc = slc
	s.idx = 0
	return nil
}

func (s *sliceProviderStream[T]) Close() {
	s.slc = nil
	s.idx = 0
}
