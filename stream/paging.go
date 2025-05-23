package stream

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

func (s Stream[T]) Limit(limit int) Stream[T] {
	if limit <= 0 {
		return Empty[T]()
	}
	alreadyConsumed := 1
	return newStream[T](func(ctx context.Context) (T, error) {
		if alreadyConsumed > limit {
			return util.DefaultValue[T](), io.EOF
		}

		v, err := s.provider(ctx)
		if err != nil {
			// this covers for both EOF and any other error
			return util.DefaultValue[T](), err
		}
		alreadyConsumed++
		return v, nil
	}, s.allLifecycleElement)
}

func (s Stream[T]) Skip(skip int) Stream[T] {
	alreadySkipped := false
	return newStream[T](func(ctx context.Context) (T, error) {
		if ctx.Err() != nil {
			return util.DefaultValue[T](), ctx.Err()
		}
		if !alreadySkipped {
			alreadySkipped = true
			for i := 0; i < skip; i++ {
				v, err := s.provider(ctx)
				if err != nil {
					return v, err
				}
			}
		}
		return s.provider(ctx)

	}, s.allLifecycleElement)
}

func (s Stream[T]) Page(pageNum int, pageSize int) Stream[T] {
	if pageNum < 0 || pageSize <= 0 {
		return Empty[T]()
	}
	skipped := pageNum * pageSize
	return s.Skip(skipped).Limit(pageSize)
}
