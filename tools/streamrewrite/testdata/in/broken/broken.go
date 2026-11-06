package broken

import (
	"context"

	"github.com/shpandrak/shpanstream/stream"
)

func B(ctx context.Context, s stream.Stream[int]) (int, error) {
	return s.Count(ctx)
}

func oops( {
