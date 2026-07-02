package stream

import (
	"context"
)

// DoFirst registers f to run at the start of every drain, before any lifecycle element's Open
// (including the source's). It is purely observational: f cannot fail or abort the stream, and a
// panic in f is recovered and logged (like DoFinally). Multiple DoFirst hooks run in reverse
// declaration order (last declared, first run).
//
// Pairs with DoFinally to bracket a drain's full wall time: DoFirst fires before upstream open
// work (e.g. issuing a DB query), and DoFinally fires with the terminal outcome — including when
// an upstream Open fails, so a DoFirst-armed timer still gets its duration sample.
func (s Stream[T]) DoFirst(f func(ctx context.Context)) Stream[T] {
	return newStream(
		s.provider,
		append([]Lifecycle{NewLifecycle(
			func(ctx context.Context) error {
				invokeObservationalHook("DoFirst", func() { f(ctx) })
				return nil
			},
			nil,
		)}, s.allLifecycleElement...),
	)
}
