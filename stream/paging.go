package stream

import (
	"context"
	"io"

	"github.com/shpandrak/shpanstream/internal/util"
)

// Limit returns a stream of at most the first limit elements of s. Once limit elements have been
// emitted, no further element is pulled from s.
//
//go:noinline
func Limit[T any](s Stream[T], limit int) Stream[T] {
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

// Skip returns a stream of the elements of s after the first skip ones.
//
//go:noinline
func Skip[T any](s Stream[T], skip int) Stream[T] {
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

// Page returns page pageNum (counted from 0) of s, in pages of pageSize elements:
// Limit(Skip(s, pageNum*pageSize), pageSize). An invalid page gives an empty stream.
func Page[T any](s Stream[T], pageNum int, pageSize int) Stream[T] {
	if pageNum < 0 || pageSize <= 0 {
		return Empty[T]()
	}
	skipped := pageNum * pageSize
	return Limit(Skip(s, skipped), pageSize)
}
