//go:build sometag

package user

import (
	"context"

	"github.com/shpandrak/shpanstream/stream"
)

// Tagged is only built with -tags sometag.
func Tagged(ctx context.Context, s stream.Stream[int]) (int, error) {
	return s.Count(ctx)
}
