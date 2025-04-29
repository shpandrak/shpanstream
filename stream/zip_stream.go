package stream

import (
	"context"
)

func ZipN[T any](s ...Stream[T]) Stream[[]T] {
	if len(s) == 0 {
		return Empty[[]T]()
	}
	return NewDownMultiStreamSimple(
		s,
		func(ctx context.Context, providers []ProviderFunc[T]) ([]T, error) {
			var result []T
			for i := range providers {
				s, err := providers[i](ctx)
				if err != nil {
					return nil, err
				}
				result = append(result, s)
			}
			return result, nil

		})
}
