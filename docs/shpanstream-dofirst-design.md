# Design: `DoFirst` — observational hook before all lifecycle Opens

*Response to a consumer request for a `WithPrependedLifecycle` API, motivated by a stream-metrics
wrapper library that needs to measure a drain's full wall time. Decision: meet the need with a
narrow observational hook instead of exposing lifecycle-order control.*

## Problem

An external wrapper cannot observe a drain's full wall time. `WithAdditionalLifecycle` appends
and `doOpenStream` opens lifecycles in list order (both in `stream/shpan_stream.go`), so a
wrapper-added Open always runs after every upstream Open.
For DB-backed streams the real work (pool acquire, query, time-to-first-row) happens in the
source's Open, so a metrics wrapper measures iteration time only. Additionally, when an upstream
Open fails, an appended Open never ran, so a timer never armed: the wrapper counts an error (via
`DoFinally`) but records no duration sample, breaking `count == successes + errors` invariants.

## Why not `WithPrependedLifecycle`

`doCloseSubStream` (`stream/shpan_stream.go`) closes in **forward** list order — same as open,
not reverse. A prepended lifecycle therefore opens first *and closes first*, before the source
closes. That is fine for a purely observational bracket, but the API cannot enforce
"observational only": the natural misuse — prepending a resource lifecycle (pool handle, span,
transaction) that the source depends on — looks correct at open time and is silently wrong at
close time. An API whose safe use is a doc caveat is a footgun, and once shipped the close order
cannot be fixed without breaking whoever relied on it. General prepend becomes legitimate only
with reverse-order close (true bracket semantics), which is a global behavioral change nothing in
the current use case requires.

Precedent: Reactor solved this exact gap (`doOnSubscribe` fires too late to observe source
subscription work) with the narrow `doFirst(Runnable)` — guaranteed to run before the source is
subscribed, deliberately not a resource-holding lifecycle — never with user-controlled operator
ordering.

## API

```go
// DoFirst registers f to run at the start of every drain, before any lifecycle
// element's Open (including the source's). Purely observational: f cannot fail
// or abort the stream, and panics are recovered and logged (like DoFinally).
// Multiple DoFirst hooks run in reverse declaration order (last declared, first run).
// Pairs with DoFinally to bracket a drain's full wall time, including source Open.
func (s Stream[T]) DoFirst(f func(ctx context.Context)) Stream[T]
```

Semantics:

- **Position.** Prepends an internal lifecycle node, so its Open runs before all lifecycle
  elements registered so far. Operators applied later append to the end, so the node stays first;
  a later `DoFirst` prepends again, giving last-declared-first order (matches Reactor `doFirst`).
- **Ctx.** `f` receives the drain's context (`ctxWithCancel` from `doOpenStream`), useful for
  tracing.
- **No error, no Close.** `f` returns nothing; the node's Close is a no-op. There is nothing to
  misorder at teardown.
- **Panic guard.** `f` runs under the same recover-and-`slog.Error` guard as `finallyNode.invoke`
  so a buggy hook cannot crash the consumer.
- **Open failure.** If a downstream (later-in-list) Open fails after `f` ran, existing rollback in
  `doOpenStream` closes the node (no-op) and `fireFinallyHooksOnOpenFailure` still delivers the
  error to `DoFinally` hooks — so a `DoFirst`-armed timer gets its duration sample on open
  failure, restoring `duration_seconds_count == successes + errors`.
- **Re-consumption.** Open fires per drain, so `f` runs again on each re-consumption.

## Implementation

One new method in `stream/do_first.go` (beside `do_finally.go`): prepend a panic-guarded
observational node via
`newStream(s.provider, append([]Lifecycle{node}, s.allLifecycleElement...))`.
Sibling backing-array aliasing (two derived streams appending into the same spare-capacity slot)
is guarded once at the chokepoint where the in-place append happens: `WithAdditionalLifecycle`
clips its slice (`slices.Clip`) before appending, so every producer of a lifecycle slice —
`DoFirst`, `DoFinally`, or a future operator — is covered without per-call-site defenses.
The panic guard is shared with `DoFinally` (`invokeObservationalHook` in
`stream/do_finally.go`). No changes to `doOpenStream`, `doCloseSubStream`, or
`fireFinallyHooksOnOpenFailure`; all already handle a prepended element correctly.

Customer usage (their `MeasureTimer` wrapper):

```go
func MeasureTimer[T any](s stream.Stream[T], t *obs.Timer, labels ...string) stream.Stream[T] {
	var stop func()
	return s.
		DoFirst(func(context.Context) { stop = t.Start(labels...) }).
		DoFinally(func(err error) {
			if stop != nil {
				stop()
				stop = nil
			}
			t.ObserveErr(err, labels...)
		})
}
```

Note: the shared `stop` variable is scoped per wrapped stream, not per drain, so this pattern
assumes the measured stream is drained at most once at a time. Streams are re-consumable values;
draining the same wrapped stream from multiple goroutines concurrently is a data race on `stop`
and misattributes timer samples. For concurrent drains, keep per-drain state instead (e.g. arm
the timer inside the drain's own scope and wrap each drain separately).

## Testing

- Order: `DoFirst` hook runs before an existing lifecycle's Open (and before the source provider's
  Open).
- Open failure: with a failing source Open, the `DoFirst` hook ran and `DoFinally` fired with the
  open error (the count-parity case).
- Re-consumption: hook runs again on each drain.
- Panic guard: a panicking hook does not fail the drain; stream completes normally.
- Multiple hooks: two `DoFirst` calls run in reverse declaration order.
- Downstream composition: `DoFirst` attached before further operators (e.g. `Filter`) still runs
  first.

## Alternatives rejected

1. **`WithPrependedLifecycle`** — exposes ordering control with unsafe close semantics (above).
2. **Prepend + reverse-order close** — clean bracket semantics but a global behavioral change to
   every existing stream; revisit only if a customer needs a true resource bracket
   (open-first/close-last).
3. **Full `Tap`/`Observe` instrumentation operator** (Reactor `SignalListener` analog) — bigger
   API surface; `DoFirst` + `DoFinally` + `Peek` already compose into it. YAGNI.
