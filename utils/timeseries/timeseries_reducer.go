package timeseries

import (
	"context"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"time"
)

type Reducer[N Number] func(forTime time.Time, stream shpanstream.Stream[TsRecord[N]]) shpanstream.Lazy[TsRecord[N]]

func Max[N Number](forTime time.Time, stream shpanstream.Stream[TsRecord[N]]) shpanstream.Lazy[TsRecord[N]] {
	return shpanstream.MapLazy(
		shpanstream.MaxLazy(shpanstream.MapStream(stream, mapRecordValue)),
		recordMapper[N](forTime),
	)
}

func Avg[N Number](forTime time.Time, stream shpanstream.Stream[TsRecord[N]]) shpanstream.Lazy[TsRecord[N]] {
	return shpanstream.MapLazy(
		shpanstream.NewLazy(func(ctx context.Context) (N, error) {
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
