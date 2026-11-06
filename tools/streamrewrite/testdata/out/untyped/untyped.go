package untyped

import (
	"context"

	"github.com/shpandrak/shpanstream/stream"
)

func U(ctx context.Context) {
	_, _ = undefinedStream().Collect(ctx)
	_ = stream.MustCount(stream.FromSlice([]int{1}))
}
