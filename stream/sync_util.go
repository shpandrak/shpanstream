package stream

import (
	"sync"
)

// WithLockWhileMaterializing helper function to lock the stream while materializing.
// returns a new stream that locks the provided mutex while materializing the stream
func WithLockWhileMaterializing[T any](s Stream[T], mu sync.Locker) Stream[T] {
	return WithAdditionalLifecycle(s, lockLifecycle(mu))
}
