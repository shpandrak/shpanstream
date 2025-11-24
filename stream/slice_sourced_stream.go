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
