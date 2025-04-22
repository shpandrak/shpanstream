package shpanstream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
)

type MapStreamOptions interface {
	mapStreamOptionName() string
}

type concurrentMapStreamOptions struct {
	concurrency int
}

// WithConcurrentMapStreamOption is causing the mapper function to be called concurrently in separate goroutines.
// note that the resulting stream order is not guaranteed to be the same as the source stream order.
// Note that source stream is being read sequentially, and only the mapper function is called concurrently.
// This is useful when the mapper function is expensive, and you want to parallelize the mapping process.
func WithConcurrentMapStreamOption(concurrency int) MapStreamOptions {
	return &concurrentMapStreamOptions{concurrency: concurrency}
}

func (c *concurrentMapStreamOptions) mapStreamOptionName() string {
	return "concurrent"
}

func MapStream[SRC any, TGT any](
	src Stream[SRC],
	mapper Mapper[SRC, TGT],
	options ...MapStreamOptions,
) Stream[TGT] {
	return MapStreamWithErrAndCtx(src, mapperToErrCtx(mapper), options...)
}

func MapStreamWithErr[SRC any, TGT any](
	src Stream[SRC],
	mapper MapperWithErr[SRC, TGT],
	options ...MapStreamOptions,
) Stream[TGT] {
	return MapStreamWithErrAndCtx(src, mapperErrToErrCtx(mapper), options...)
}

func MapStreamWithErrAndCtx[SRC any, TGT any](
	src Stream[SRC],
	mapper MapperWithErrAndCtx[SRC, TGT],
	options ...MapStreamOptions,
) Stream[TGT] {
	if len(options) > 0 {
		for _, opt := range options {
			switch cOpt := opt.(type) {
			case *concurrentMapStreamOptions:
				return mapStreamConcurrently[SRC, TGT](src, cOpt.concurrency, mapper)
			default:
				return ErrorStream[TGT](fmt.Errorf("unsupported map stream option type: %T", opt))
			}
		}
	}
	return newStream[TGT](
		func(ctx context.Context) (TGT, error) {
			v, err := src.provider(ctx)
			if err != nil {
				return util.DefaultValue[TGT](), err
			}
			return mapper(ctx, v)
		}, src.allLifecycleElement,
	)
}

// MapStreamWhileFiltering is a function that maps a Stream of SRC to a Stream of TGT while allowing to filter.
// filtering is done by returning nil from the mapper function.
func MapStreamWhileFiltering[SRC any, TGT any](
	src Stream[SRC],
	mapper Mapper[SRC, *TGT],
	options ...MapStreamOptions,
) Stream[TGT] {
	return MapStreamWhileFilteringWithErrAndCtx(src, mapperToErrCtx(mapper), options...)
}

// MapStreamWhileFilteringWithErr is a function that maps a Stream of SRC to a Stream of TGT while allowing to filter.
// filtering is done by returning nil from the mapper function.
func MapStreamWhileFilteringWithErr[SRC any, TGT any](
	src Stream[SRC],
	mapper MapperWithErr[SRC, *TGT],
	options ...MapStreamOptions,
) Stream[TGT] {
	return MapStreamWhileFilteringWithErrAndCtx(src, mapperErrToErrCtx[SRC, *TGT](mapper), options...)
}

// MapStreamWhileFilteringWithErrAndCtx is a function that maps a Stream of SRC to a Stream of TGT while allowing to filter while streaming.
// filtering is done by returning nil from the mapper function.
func MapStreamWhileFilteringWithErrAndCtx[SRC any, TGT any](
	src Stream[SRC],
	mapper MapperWithErrAndCtx[SRC, *TGT],
	options ...MapStreamOptions,
) Stream[TGT] {
	return MapStream(

		// First we map the stream to a stream of pointers to TGT using the mapper
		MapStreamWithErrAndCtx(src, mapper, options...).

			// Then we filter the stream to remove nil values
			Filter(func(tgt *TGT) bool {
				return tgt != nil
			}),

		// Finally we map the stream to a stream of TGT by dereferencing the pointers
		func(p *TGT) TGT {
			return *p
		},
	)
}

// FlatMapStream maps a single element of the source stream to a stream of elements and flattens the result to a single stream.
func FlatMapStream[SRC any, TGT any](src Stream[SRC], mapper Mapper[SRC, Stream[TGT]]) Stream[TGT] {
	collect, err := MapStreamWithErrAndCtx[SRC, Stream[TGT]](src, mapperToErrCtx(mapper)).
		Collect(context.Background())

	if err != nil {
		return ErrorStream[TGT](err)
	}
	return ConcatStreams[TGT](collect...)
}
