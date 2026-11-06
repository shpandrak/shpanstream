package user

import (
	"context"

	ss "github.com/shpandrak/shpanstream/stream"
)

func Aliased(ctx context.Context, s ss.Stream[int]) ([]int, error) {
	return s.Limit(3).Collect(ctx)
}
