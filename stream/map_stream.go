package stream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
)

type MapOption interface {
	mapStreamOptionName() string
}

type concurrentMapOption struct {
	concurrency int
}

// WithConcurrentMapOption is causing the mapper function to be called concurrently in separate goroutines.
// note that the resulting stream order is not guaranteed to be the same as the source stream order.
// Note that source stream is being read sequentially, and only the mapper function is called concurrently.
// This is useful when the mapper function is expensive, and you want to parallelize the mapping process.
func WithConcurrentMapOption(concurrency int) MapOption {
	return &concurrentMapOption{concurrency: concurrency}
}

func (c *concurrentMapOption) mapStreamOptionName() string {
	return "concurrent"
}

// concurrentMapOptionOf interprets the map options: the first option decides, as it always did. It
// reports the concurrency if that option is a concurrent one, or an error for an unsupported option.
func concurrentMapOptionOf(options []MapOption) (concurrency int, concurrent bool, err error) {
	for _, opt := range options {
		switch cOpt := opt.(type) {
		case *concurrentMapOption:
			return cOpt.concurrency, true, nil
		default:
			return 0, false, fmt.Errorf("unsupported map stream option type: %T", opt)
		}
	}
	return 0, false, nil
}

// Map maps the source stream to a target stream using the provided mapper function.
func Map[SRC any, TGT any](
	src Stream[SRC],
	mapper shpanstream.Mapper[SRC, TGT],
	options ...MapOption,
) Stream[TGT] {
	return MapWithErrAndCtx(src, mapper.ToErrCtx(), options...)
}

// MapWithErr maps the source stream to a target stream using the provided mapper function.
func MapWithErr[SRC any, TGT any](
	src Stream[SRC],
	mapper shpanstream.MapperWithErr[SRC, TGT],
	options ...MapOption,
) Stream[TGT] {
	return MapWithErrAndCtx(src, mapper.ToErrCtx(), options...)
}

// MapWithErrAndCtx maps the source stream to a target stream using the provided mapper function.
func MapWithErrAndCtx[SRC any, TGT any](
	src Stream[SRC],
	mapper shpanstream.MapperWithErrAndCtx[SRC, TGT],
	options ...MapOption,
) Stream[TGT] {
	if concurrency, concurrent, err := concurrentMapOptionOf(options); err != nil {
		return Error[TGT](err)
	} else if concurrent {
		return mapStreamConcurrently[SRC, TGT](src, concurrency, mapper)
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

// MapWhileFiltering is a function that maps a Stream of SRC to a Stream of TGT while allowing to filter.
// filtering is done by returning nil from the mapper function.
// This is a convenience function to avoid chaining filter and the Map and do it in one go.
func MapWhileFiltering[SRC any, TGT any](
	src Stream[SRC],
	mapper shpanstream.Mapper[SRC, *TGT],
	options ...MapOption,
) Stream[TGT] {
	return MapWhileFilteringWithErrAndCtx(src, mapper.ToErrCtx(), options...)
}

// MapWhileFilteringWithErr is a function that maps a Stream of SRC to a Stream of TGT while allowing to filter.
// filtering is done by returning nil from the mapper function.
// This is a convenience function to avoid chaining filter and the Map and do it in one go.
func MapWhileFilteringWithErr[SRC any, TGT any](
	src Stream[SRC],
	mapper shpanstream.MapperWithErr[SRC, *TGT],
	options ...MapOption,
) Stream[TGT] {
	return MapWhileFilteringWithErrAndCtx(src, mapper.ToErrCtx(), options...)
}

// MapWhileFilteringWithErrAndCtx is a function that maps a Stream of SRC to a Stream of TGT while allowing to filter while streaming.
// filtering is done by returning nil from the mapper function.
// This is a convenience function to avoid chaining filter and the Map and do it in one go.
func MapWhileFilteringWithErrAndCtx[SRC any, TGT any](
	src Stream[SRC],
	mapper shpanstream.MapperWithErrAndCtx[SRC, *TGT],
	options ...MapOption,
) Stream[TGT] {
	return Map(

		// First we map the stream to a stream of pointers to TGT using the mapper
		MapWithErrAndCtx(src, mapper, options...).

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

// FlatMap maps a single element of the source stream to a stream of elements and flattens the result to a single stream.
func FlatMap[SRC any, TGT any](src Stream[SRC], mapper shpanstream.Mapper[SRC, Stream[TGT]]) Stream[TGT] {
	return Concat[TGT](MapWithErrAndCtx[SRC, Stream[TGT]](src, mapper.ToErrCtx()))
}
