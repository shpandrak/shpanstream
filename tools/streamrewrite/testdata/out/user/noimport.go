package user

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
)

// This file never names the stream package, so the rewrite has to import it.
func NoImport(ctx context.Context) (int, error) {
	return stream.Count(ctx, people())
}
