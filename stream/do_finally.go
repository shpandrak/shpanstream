package stream

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"slices"
)

// DoFinally registers f to run exactly once when this stream node terminates, on the consuming
// goroutine, at Close (after the node stops producing). f runs after this node and its upstream have
// been closed, so their resources are already torn down — f observes the terminal outcome, it does
// not get a live view of upstream resources.
//
//	err == nil -> terminated without error (completed on io.EOF, or truncated early by e.g. Limit
//	              or a downstream break).
//	err != nil -> terminated with a pipeline error. This includes an open-stage failure (a lifecycle
//	              Open returning an error, e.g. a source that cannot connect); in that case f runs
//	              before the already-opened upstream siblings are torn down, since the open error
//	              must win over the rollback close. External context cancellation surfaces as the
//	              context error (use errors.Is(err, context.Canceled) / context.DeadlineExceeded to
//	              detect it).
//
// DoFinally is local and compositional (like the Lifecycle Open/Close hooks): it observes this node
// and everything upstream feeding it, wherever the node sits — including as a sub-stream inside a
// combinator (Concat/Join/Merge/...). It does NOT observe errors produced by operators applied
// *downstream* of it: such an error tears this node down cleanly, so f sees err == nil. To observe
// the whole pipeline's terminal error, attach DoFinally as the outermost operator.
//
// The terminal error is matched against io.EOF by identity (io.EOF is normal completion, not an
// error), consistent with the rest of the pipeline; an upstream that returns a %w-wrapped io.EOF
// will therefore be reported here as a real error.
//
// f is purely observational: it must not mutate the stream or drive further emission. A
// consumer-side failure (the consume callback erroring, or a consumer panic) is not the stream's
// terminal outcome and is reported here as err == nil — that error remains the return value of
// Consume/Collect/Reduce.
func (s Stream[T]) DoFinally(f func(err error)) Stream[T] {
	n := &finallyNode{f: f}
	return newStream(
		func(ctx context.Context) (T, error) {
			v, err := s.provider(ctx)
			// Record the terminal error the node's own subtree produced, so the Close hook can
			// report it locally. io.EOF is normal completion, not an error.
			if err != nil && err != io.EOF {
				n.recordedErr = err
			}
			return v, err
		},
		append(slices.Clone(s.allLifecycleElement), n),
	)
}

// finallyNode is the internal Lifecycle carrying a DoFinally callback. It captures its Open context
// (so cancellation can be reported at Close) and the terminal error observed at the provider
// boundary, then fires the callback exactly once per consumption when the node is closed.
type finallyNode struct {
	f           func(err error)
	capturedCtx context.Context
	recordedErr error
	fired       bool
}

func (n *finallyNode) Open(ctx context.Context) error {
	// Reset per-consumption state so re-consumption (double collection) fires again cleanly.
	n.capturedCtx = ctx
	n.recordedErr = nil
	n.fired = false
	return nil
}

func (n *finallyNode) Close() {
	if n.fired {
		return
	}
	n.fired = true

	err := n.recordedErr
	if err == nil && n.capturedCtx != nil {
		// No provider error: if the (external) context was cancelled, that is the terminal outcome.
		err = n.capturedCtx.Err()
	}
	n.invoke(err)
}

// fireOpenFailure fires the hook with an open-stage terminal error (a lifecycle Open returning an
// error). Unlike Close it always fires when called — open failures can recur across re-consumptions
// and the node's own Open may never have run — and it sets fired so the subsequent rollback Close
// cannot fire again with nil.
func (n *finallyNode) fireOpenFailure(err error) {
	n.fired = true
	n.invoke(err)
}

// invoke runs the observational callback under a panic guard so a buggy hook cannot crash the
// consumer or leak through Open/Close.
func (n *finallyNode) invoke(err error) {
	defer func() {
		if rvr := recover(); rvr != nil {
			slog.Error(fmt.Sprintf("DoFinally hook panicked: %v\n%s", rvr, debug.Stack()))
		}
	}()
	n.f(err)
}

// fireFinallyHooksOnOpenFailure notifies any DoFinally hooks among a stream's lifecycle elements
// that the stream failed to open, so they observe the open-stage terminal error. It is called from
// doOpenStream (the single open chokepoint for every consumer) before the rollback close, so this
// error wins over Close's nil.
func fireFinallyHooksOnOpenFailure(elements []Lifecycle, err error) {
	for _, l := range elements {
		if fn, ok := l.(*finallyNode); ok {
			fn.fireOpenFailure(err)
		}
	}
}
