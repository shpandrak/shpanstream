package stream

import (
	"context"
	"sync"
)

// WithLockWhileMaterializing helper function to lock the stream while materializing.
// returns a new stream that locks the provided mutex while materializing the stream
func (s Stream[T]) WithLockWhileMaterializing(mu sync.Locker) Stream[T] {
	return s.WithAdditionalLifecycle(NewLifecycle(
		func(ctx context.Context) error {
			mu.Lock()
			return nil
		},
		func() {
			mu.Unlock()
		},
	))
}
