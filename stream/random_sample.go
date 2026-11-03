package stream

import (
	"context"
	"math/rand"
)

//go:noinline
func (s Stream[T]) CollectRandomSample(ctx context.Context, sampleSize int) ([]T, error) {
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
func (s Stream[T]) RandomSample(sampleSize int) Stream[T] {
	return FromSliceProvider(func(ctx context.Context) ([]T, error) {
		return s.CollectRandomSample(ctx, sampleSize)
	})
}
