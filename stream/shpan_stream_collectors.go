package stream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
)

// CollectToMap collects the elements of the stream into a map, using the provided entryFactory function to create the key-value pairs.
// If a duplicate key is found, an error is returned.
func CollectToMap[T any, K comparable, V any](
	ctx context.Context,
	s Stream[T],
	entryFactory func(T) (K, V),
) (map[K]V, error) {
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

// CollectCountGroupedBy collects the elements of the stream into a map of element groups, using the grouper mapper.
// to classify the elements.
func CollectCountGroupedBy[K comparable, T any](
	ctx context.Context,
	s Stream[T],
	grouper shpanstream.Mapper[T, K],
) (map[K]uint64, error) {
	result := make(map[K]uint64)
	err := s.Consume(ctx, func(v T) {
		result[grouper(v)]++
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// CollectToMapOverrideDuplicates collects the elements of the stream into a map,
// using the provided grouper function to create the key-value pairs.
// If a duplicate key is found, the value is overridden with the new value.
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
