package stream

import (
	"sync"
)

// WithLockWhileMaterializing helper function to lock the stream while materializing.
// returns a new stream that locks the provided mutex while materializing the stream
func (s Stream[T]) WithLockWhileMaterializing(mu sync.Locker) Stream[T] {
	return s.WithAdditionalLifecycle(lockLifecycle(mu))
}
