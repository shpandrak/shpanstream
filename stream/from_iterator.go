package stream

import (
	"context"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
	"iter"
)

func FromIterator[E any](seq iter.Seq[E]) Stream[E] {
	next, stop := iter.Pull(seq)
	return NewSimpleStream(func(ctx context.Context) (E, error) {
		if ctx.Err() != nil {
			return util.DefaultValue[E](), ctx.Err()
		}
		e, b := next()
		if !b {
			return util.DefaultValue[E](), io.EOF
		}
		return e, nil
	}, WithCloseFuncOption(func() {
		stop()
	}))

}

func FromIterator2[K comparable, V any](seq iter.Seq2[K, V]) Stream[shpanstream.Entry[K, V]] {
	next, stop := iter.Pull2(seq)
	return NewSimpleStream(func(ctx context.Context) (shpanstream.Entry[K, V], error) {
		if ctx.Err() != nil {
			return util.DefaultValue[shpanstream.Entry[K, V]](), ctx.Err()
		}
		k, v, b := next()
		if !b {
			return util.DefaultValue[shpanstream.Entry[K, V]](), io.EOF
		}
		return shpanstream.Entry[K, V]{Key: k, Value: v}, nil
	}, WithCloseFuncOption(func() {
		stop()
	}))

}
