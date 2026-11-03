package stream

import (
	"context"
	"fmt"

	"github.com/shpandrak/shpanstream"
)

// concurrentStreamMapperProvider is the typed shell of a concurrent map: it boxes the source
// provider and the mapper into the non-generic concurrentMapCore (concurrent_core.go) at open and
// unboxes emitted elements, so the goroutine fan-out is compiled once rather than per [SRC, TGT].
type concurrentStreamMapperProvider[SRC any, TGT any] struct {
	concurrency int
	mapper      func(context.Context, SRC) (TGT, error)
	src         Stream[SRC]

	// Per-consumption fan-out state, (re)initialised in open and torn down in close.
	concurrentMapCore
}

// mapStreamConcurrently returns a mapped stream using a mapper function. The mapping is done
// concurrently. Note that the resulting stream order is not guaranteed to match the source order.
func mapStreamConcurrently[SRC any, TGT any](
	src Stream[SRC],
	concurrency int,
	mapper shpanstream.MapperWithErrAndCtx[SRC, TGT],
) Stream[TGT] {
	if concurrency <= 0 {
		return Error[TGT](fmt.Errorf("concurrency must be > 0"))
	}
	c := &concurrentStreamMapperProvider[SRC, TGT]{
		concurrency: concurrency,
		mapper:      mapper,
		src:         src,
	}
	// The provider owns the source lifecycle itself (rather than delegating to the unsafe builder)
	// so that close can join the background reader goroutine BEFORE closing the source — otherwise
	// the reader keeps driving the source's provider while the source is being torn down (a data
	// race that surfaces on early termination: Limit, ctx cancellation, downstream error).
	return newStream(c.emit, []Lifecycle{NewLifecycle(c.open, c.close)})
}

func (c *concurrentStreamMapperProvider[SRC, TGT]) open(ctx context.Context) error {
	return c.concurrentMapCore.open(
		ctx,
		c.concurrency,
		c.src.allLifecycleElement,
		boxProvider(c.src.provider),
		boxMapper(c.mapper),
	)
}

func (c *concurrentStreamMapperProvider[SRC, TGT]) emit(ctx context.Context) (TGT, error) {
	return unboxResult[TGT](c.concurrentMapCore.emit(ctx))
}

func (c *concurrentStreamMapperProvider[SRC, TGT]) close() {
	c.concurrentMapCore.close()
}
