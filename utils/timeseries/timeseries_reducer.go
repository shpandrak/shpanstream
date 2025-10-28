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
	return lazy.Map(
		stream.MaxLazy(stream.Map(s, mapRecordValue)),
		recordMapper[N](forTime),
	)
}

func Min[N Number](forTime time.Time, s stream.Stream[TsRecord[N]]) lazy.Lazy[TsRecord[N]] {
	return lazy.Map(
		stream.MinLazy(stream.Map(s, mapRecordValue)),
		recordMapper[N](forTime),
	)
}

func Avg[N Number](forTime time.Time, stream stream.Stream[TsRecord[N]]) lazy.Lazy[TsRecord[N]] {
	return lazy.Map(
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

func Sum[N Number](forTime time.Time, stream stream.Stream[TsRecord[N]]) lazy.Lazy[TsRecord[N]] {
	return lazy.Map(
		lazy.NewLazy(func(ctx context.Context) (N, error) {
			var sum N
			err := stream.Consume(ctx, func(currVal TsRecord[N]) {
				sum += currVal.Value
			})
			if err != nil {
				return util.DefaultValue[N](), err
			}
			return sum, nil
		}),
		recordMapper[N](forTime),
	)
}
