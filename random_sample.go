package shpanstream

import (
	"context"
	"math/rand"
)

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

func (s Stream[T]) RandomSample(sampleSize int) Stream[T] {
	return newStreamFromCollector(s, func(ctx context.Context, src Stream[T]) ([]T, error) {
		return src.CollectRandomSample(ctx, sampleSize)
	})
}
