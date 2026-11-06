package stream

import (
	"context"
	"math/rand"
)

// CollectRandomSample materializes the stream and returns a uniform random sample of sampleSize of
// its elements (all of them if the stream has fewer), drawn by reservoir sampling.
//
//go:noinline
func CollectRandomSample[T any](ctx context.Context, s Stream[T], sampleSize int) ([]T, error) {
	if sampleSize <= 0 {
		return nil, nil
	}

	reservoir := make([]T, 0, sampleSize)

	index := 0
	err := s.Consume(ctx, func(v T) {
		if index < sampleSize {
			reservoir = append(reservoir, v)
		} else {
			j := rand.Intn(index + 1)
			if j < sampleSize {
				reservoir[j] = v
			}
		}
		index++
	})

	if err != nil {
		return nil, err
	}
	return reservoir, nil
}

// RandomSample returns a stream of sampleSize elements sampled from the source. The sample is drawn
// when the returned stream is opened (once per materialization), so it is re-consumable.
//
//go:noinline
func RandomSample[T any](s Stream[T], sampleSize int) Stream[T] {
	return FromSliceProvider(func(ctx context.Context) ([]T, error) {
		return CollectRandomSample(ctx, s, sampleSize)
	})
}
