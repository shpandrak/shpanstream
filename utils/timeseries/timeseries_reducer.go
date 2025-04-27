package timeseries

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/lazy"
	"github.com/shpandrak/shpanstream/stream"
	"time"
)

type Reducer[N Number] func(forTime time.Time, stream stream.Stream[TsRecord[N]]) lazy.Lazy[TsRecord[N]]

func Max[N Number](forTime time.Time, s stream.Stream[TsRecord[N]]) lazy.Lazy[TsRecord[N]] {
	return lazy.MapLazy(
		stream.MaxLazy(stream.MapStream(s, mapRecordValue)),
		recordMapper[N](forTime),
	)
}

func Avg[N Number](forTime time.Time, stream stream.Stream[TsRecord[N]]) lazy.Lazy[TsRecord[N]] {
	return lazy.MapLazy(
		lazy.NewLazy(func(ctx context.Context) (N, error) {
			var avg N
			var count uint64
			err := stream.Consume(ctx, func(currVal TsRecord[N]) {
				avg = avg*N(count)/N(count+1) + currVal.Value/N(count+1)
			})
			if err != nil {
				return util.DefaultValue[N](), err
			}
			return avg, nil
		}),
		recordMapper[N](forTime),
	)
}
