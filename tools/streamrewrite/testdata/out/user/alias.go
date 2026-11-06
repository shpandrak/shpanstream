package user

import (
	"context"

	ss "github.com/shpandrak/shpanstream/stream"
)

func Aliased(ctx context.Context, s ss.Stream[int]) ([]int, error) {
	return ss.Collect(ctx, ss.Limit(s, 3))
}
