package shpanstream

import (
	"context"
	"fmt"
)

func CollectCountGroupedBy[K comparable, T any](ctx context.Context, s Stream[T], grouper func(T) K) (map[K]uint64, error) {
	result := make(map[K]uint64)
	err := s.Consume(ctx, func(v T) {
		result[grouper(v)]++
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func CollectToMapFilterNilOverrideDuplicates[K comparable, T any](ctx context.Context, s Stream[T], grouper func(T) K) (map[K]T, error) {
	result := make(map[K]T)
	err := s.Consume(ctx, func(v T) {
		result[grouper(v)] = v
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func CollectToMapFilterNilOverrideDuplicatesMapValue[K comparable, T any, V any](ctx context.Context, s Stream[T], grouper func(T) K, valueMapper func(T) V) (map[K]V, error) {
	result := make(map[K]V)
	err := s.Consume(ctx, func(v T) {
		result[grouper(v)] = valueMapper(v)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func CollectToMapOverrideDuplicates[K comparable, T any](ctx context.Context, s Stream[T], grouper func(T) K) (map[K]T, error) {
	result := make(map[K]T)
	err := s.Consume(ctx, func(v T) {
		result[grouper(v)] = v
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func CollectToMap[T any, K comparable, V any](ctx context.Context, s Stream[T], entryFactory func(T) (K, V)) (map[K]V, error) {
	result := make(map[K]V)
	err := s.ConsumeWithErr(ctx, func(src T) error {
		k, v := entryFactory(src)
		if existingValue, ok := result[k]; ok {
			return fmt.Errorf("duplicate key %v for source values %v and %v", k, v, existingValue)
		}
		result[k] = v
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
