package stream

import (
	"context"
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
