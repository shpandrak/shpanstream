package user

import (
	"context"

	"github.com/shpandrak/shpanstream/stream"
)

// Counter is satisfied by a v0.5 Stream, not by a v0.6 one.
type Counter interface {
	Count(ctx context.Context) (int, error)
}

// Query only shares a method name with Stream, not its signature.
type Query interface {
	Limit(n int) Query
}

type Wrapped struct{ stream.Stream[Person] }

func paging() (int, int) { return 1, 10 }

// Left for a hand edit: the context would be evaluated before the stream.
func ArgOrderHand(ch chan stream.Stream[Person], ctxCh chan context.Context, ctxList []context.Context) {
	_, _ = people().Collect(mkctx())
	_, _ = (<-ch).Count(mkctx())
	_, _ = people().Count(<-ctxCh)
	_, _ = people().IsEmpty(ctxList[0])
}

// Left for a hand edit: code that would not compile, or would behave differently.
func HandEdits(ctx context.Context, s stream.Stream[Person], v any) {
	_ = s.Page(paging())
	filter := s.FilterWithErAndCtx
	_ = filter
	collect := stream.Stream[Person].Collect
	_, _ = collect(people(), ctx)
	_ = Wrapped{people()}.MustCount()
	if c, ok := v.(Counter); ok {
		_, _ = c.Count(ctx)
	}
	switch v.(type) {
	case Query, interface{ MustCollect() []Person }:
	}
	stream := people()
	_, _ = stream.Count(ctx)
}
