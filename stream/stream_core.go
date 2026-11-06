package stream

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"
)

// This file holds the type-independent half of Stream[T]: the drain loop, the lifecycle
// bookkeeping and the panic guards. Everything here is compiled once. The generic methods in
// shpan_stream.go and friends are thin shells over it, so that an element type costs the compiler
// only the closures that really touch T (see docs/stream-api-generic-methods.md).
//
// A note on `//go:noinline` across the package: a generic method or function that creates a
// closure is marked noinline. Otherwise the compiler inlines it into every call site (and into
// the pointer-receiver wrapper of a plain method) and emits a fresh copy of the closure body each
// time; nine copies of the Filter closure per element type were measured that way. These are stream-construction calls, never per-element ones, so the lost inlining
// costs nothing on the hot path.

// consumeStep is the typed part of a sequential drain: it pulls one element from the provider and
// hands it to the consumer. pulled reports whether an element came out of the provider; when it is
// false, err is the provider's error (io.EOF for normal completion). When it is true, err is the
// consumer's error — and is never read as io.EOF.
// A step may also return (false, io.EOF) after it has pulled an element, to end the drain early
// and cleanly (FindFirst, a break out of MustValues): the stream is closed without a further pull.
type consumeStep func(ctx context.Context) (pulled bool, err error)

// consumeLoop drives one sequential drain of a stream: it opens the lifecycle elements, runs step
// until the provider is exhausted, the consumer fails or ctx is done, and closes the lifecycle
// elements, all under the consumer-boundary panic guard.
//
// noinline, like the other closure-creating helpers here: inlined into the generic consume
// functions, its closure body would be stamped out once per element type and consume flavour.
//
//go:noinline
func consumeLoop(ctx context.Context, lifecycles []Lifecycle, step consumeStep) error {
	// The panic guard avoids leaking resources and allows returning an error via panic instead of returning it
	return consumeWithPanicGuard(func() error {
		cancelFunc, errVar := openStreamLifecycles(ctx, lifecycles)
		if errVar != nil {
			return fmt.Errorf("failed to open stream: %w", errVar)
		}

		// If we reach here, all lifecycle elements have been opened successfully
		// We can defer closing them until the end of the function
		defer func() {
			closeStreamLifecycles(lifecycles)
			cancelFunc()
		}()

		for {
			// Make sure to check if the context is done before trying to get the next item
			if ctx.Err() != nil {
				// If the context is cancelled, we need to close all lifecycle elements
				// return
				return ctx.Err()
			}
			pulled, errVar := step(ctx)
			if !pulled {
				if errVar == io.EOF {
					return nil
				}
				return errVar
			}
			if errVar != nil {
				return errVar
			}
		}
	})
}

// openStreamLifecycles opens a stream's lifecycle elements in order, under a cancellable child
// context, rolling back the already-opened ones if one fails. It is the single open chokepoint for
// every consumer (sequential and concurrent) and for sub-streams inside combinators.
func openStreamLifecycles(ctx context.Context, lifecycles []Lifecycle) (context.CancelFunc, error) {
	ctxWithCancel, cancelFunc := context.WithCancel(ctx)
	// Running all lifecycle elements
	for lcIdx, l := range lifecycles {
		err := l.Open(ctxWithCancel)
		if err != nil {
			// If we fail to open a lifecycle element, we need to close all previously opened elements
			// and return the error

			openErr := fmt.Errorf("failed to open stream lifecycle element %d: %w", lcIdx, err)

			// Notify DoFinally hooks of the open-stage terminal error before the rollback close,
			// so this error wins over any already-opened hook's Close (which would fire nil).
			fireFinallyHooksOnOpenFailure(lifecycles, openErr)

			// Close only the successfully opened lifecycle elements
			for i := 0; i < lcIdx; i++ {
				lifecycles[i].Close()
			}
			// Cancel the context to stop any ongoing operations
			cancelFunc()

			return nil, openErr
		}
	}
	return cancelFunc, nil
}

// closeStreamLifecycles closes a stream's lifecycle elements, in order.
func closeStreamLifecycles(lifecycles []Lifecycle) {
	for _, l := range lifecycles {
		l.Close()
	}
}

// appendLifecycle returns lifecycles with lch appended, always into a fresh backing array:
// lifecycles may have spare capacity shared with sibling streams derived from the same parent, and
// an in-place append would overwrite their appended element.
func appendLifecycle(lifecycles []Lifecycle, lch Lifecycle) []Lifecycle {
	return append(slices.Clip(lifecycles), lch)
}

// prependLifecycle returns a fresh slice with lch ahead of lifecycles.
func prependLifecycle(lch Lifecycle, lifecycles []Lifecycle) []Lifecycle {
	return append([]Lifecycle{lch}, lifecycles...)
}

// lifecyclesFromOptions folds the NewSimpleStream options into the stream's lifecycle list: the
// last open func and the last close func given win, and no options means no lifecycle element.
func lifecyclesFromOptions(options []CreateStreamOption) []Lifecycle {
	var openFunc func(ctx context.Context) error
	var closeFunc func()

	for _, option := range options {
		if option.openFunc != nil {
			openFunc = option.openFunc
		}
		if option.closeFunc != nil {
			closeFunc = option.closeFunc
		}
	}

	if openFunc == nil && closeFunc == nil {
		return nil
	}
	return []Lifecycle{NewLifecycle(openFunc, closeFunc)}
}

// lockLifecycle holds mu for the duration of a materialization (WithLockWhileMaterializing).
//
//go:noinline
func lockLifecycle(mu sync.Locker) Lifecycle {
	return NewLifecycle(
		func(ctx context.Context) error {
			mu.Lock()
			return nil
		},
		func() {
			mu.Unlock()
		},
	)
}

// doFirstLifecycle runs the DoFirst hook, under the observational-hook panic guard, as an Open.
//
//go:noinline
func doFirstLifecycle(f func(ctx context.Context)) Lifecycle {
	return NewLifecycle(
		func(ctx context.Context) error {
			invokeObservationalHook("DoFirst", func() { f(ctx) })
			return nil
		},
		nil,
	)
}

// errorLifecycle fails every Open with err (the Error stream).
//
//go:noinline
func errorLifecycle(err error) Lifecycle {
	return NewLifecycle(func(_ context.Context) error {
		return err
	}, func() {
		// NOP
	})
}

// recoveredPanicToError converts a recovered panic value into the error reported to callers and
// DoFinally hooks, preserving the error chain when the panic value is an error.
func recoveredPanicToError(rvr any) error {
	if asErr, ok := rvr.(error); ok {
		return fmt.Errorf("stream recovered error: %w", asErr)
	}
	return fmt.Errorf("stream recovered error value: %v", rvr)
}

// consumeWithPanicGuard is the consumer-boundary panic guard shared by the sequential and
// concurrent consume paths: a panic unwinding out of body is logged with its stack and converted
// into the error the consumer returns. The completed flag (rather than recover() != nil) is what
// detects unwinding, so a legacy panic(nil) under GODEBUG=panicnil=1 — where recover() returns nil
// and thereby cancels the panic — surfaces as an error instead of a clean, silently truncated
// drain. A runtime.Goexit trips the flag too, but the error it fabricates is only assigned to a
// return value Goexit discards, so it stays harmless.
func consumeWithPanicGuard(body func() error) (err error) {
	completed := false
	defer func() {
		if completed {
			return
		}
		if rvr := recover(); rvr != nil {
			slog.Error(fmt.Sprintf("Panic recovered: %v\n%s", rvr, debug.Stack()))
			err = recoveredPanicToError(rvr)
			return
		}
		err = recoveredPanicToError(nil)
	}()
	err = body()
	completed = true
	return err
}

// wrapFilterError wraps a predicate error so that e.g. an io.EOF returned by a predicate is not
// mistaken for the end of the stream.
func wrapFilterError(err error) error {
	return fmt.Errorf("filter failed for Stream: %w", err)
}
