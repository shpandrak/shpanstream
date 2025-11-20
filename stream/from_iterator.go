package stream

import (
	"context"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
	"iter"
)

func FromIterator[E any](seq iter.Seq[E]) Stream[E] {
	var next func() (E, bool)
	var stop func()

	return NewSimpleStream(
		func(ctx context.Context) (E, error) {
			if ctx.Err() != nil {
				return util.DefaultValue[E](), ctx.Err()
			}
			e, b := next()
			if !b {
				return util.DefaultValue[E](), io.EOF
			}
			return e, nil
		},
		WithOpenFuncOption(func(ctx context.Context) error {
			// Pull the iterator on each open to support double collection
			next, stop = iter.Pull(seq)
			return nil
		}),
		WithCloseFuncOption(func() {
			if stop != nil {
				stop()
			}
		}),
	)
}

func FromIterator2[K comparable, V any](seq iter.Seq2[K, V]) Stream[shpanstream.Entry[K, V]] {
	var next func() (K, V, bool)
	var stop func()

	return NewSimpleStream(
		func(ctx context.Context) (shpanstream.Entry[K, V], error) {
			if ctx.Err() != nil {
				return util.DefaultValue[shpanstream.Entry[K, V]](), ctx.Err()
			}
			k, v, b := next()
			if !b {
				return util.DefaultValue[shpanstream.Entry[K, V]](), io.EOF
			}
			return shpanstream.Entry[K, V]{Key: k, Value: v}, nil
		},
		WithOpenFuncOption(func(ctx context.Context) error {
			// Pull the iterator on each open to support double collection
			next, stop = iter.Pull2(seq)
			return nil
		}),
		WithCloseFuncOption(func() {
			if stop != nil {
				stop()
			}
		}),
	)
}
