// Package v is in a vendor directory below the module root, which go list skips.
package v

import (
	"context"

	"github.com/shpandrak/shpanstream/stream"
)

func V(ctx context.Context, s stream.Stream[int]) (int, error) { return s.Count(ctx) }
