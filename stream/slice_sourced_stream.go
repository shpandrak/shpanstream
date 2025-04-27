package stream

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
	"slices"
)

func Just[T any](slice ...T) Stream[T] {
	return NewStream(&justStream[T]{slcOrig: slice})
}

type justStream[T any] struct {
	slcOrig []T
	slc     []T
}

func (j *justStream[T]) Open(_ context.Context) error {
	if j.slcOrig != nil {
		j.slc = slices.Clone(j.slcOrig)
	}
	return nil
}

func (j *justStream[T]) Close() {
	j.slc = nil
}

func (j *justStream[T]) Emit(ctx context.Context) (T, error) {
	if ctx.Err() != nil {
		return util.DefaultValue[T](), ctx.Err()
	}
	if len(j.slc) == 0 {
		return util.DefaultValue[T](), io.EOF
	}
	v := j.slc[0]
	j.slc = j.slc[1:]
	return v, nil
}
