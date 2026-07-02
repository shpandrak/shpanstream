# Follow-up: panics on goroutine-driven stream paths crash the process

*Found while fixing the DoFinally pipeline-panic reporting (hooks fired nil for a recovered
pipeline panic); deliberately split out of that fix. Reproduced on master, 2026-07-02.*

## Problem

Two options drive the pipeline from their own goroutines, and neither recovers panics there —
so a pipeline panic that the sequential consume paths would recover and return as an error
instead kills the whole process:

- `consumeConcurrently` (`stream/shpan_stream_concurrent.go`): the provider runs in a producer
  goroutine and the consumer callback runs in worker goroutines, neither with a `recover`.
- `MapConcurrently` / `WithConcurrentMapOption` (`stream/concurrent_stream.go`): the source
  provider runs in a reader goroutine and the mapper runs in worker goroutines, neither with a
  `recover`. Reproduced: a panicking upstream mapper consumed through
  `MapWithErrAndCtx(..., WithConcurrentMapOption(2))` crashes the process; a `DoFinally` node in
  between records the panic (its recover-record-repanic guard) but the re-raised panic escapes
  the reader goroutine before anything can observe it.

For `consumeConcurrently` specifically:

- **A pipeline panic (e.g. panicking mapper) kills the whole process.** Reproduced: unrecovered
  panic in the producer goroutine at `shpan_stream_concurrent.go:94`.
- **Worse, there is a race where the caller sees success first.** The panicking producer's deferred
  `close(itemChan)` / `close(producerDone)` run during unwinding, letting the workers drain, the
  `wg.Wait()` return, and `Consume` return `err == nil` — moments before the runtime crashes the
  process on the still-unwinding panic.
- A consumer-callback panic in a worker goroutine crashes the process the same way.

This is inconsistent with the sequential path, which recovers both kinds of panics and returns
them as errors.

## Planned fix

`consumeConcurrently`:

- Producer goroutine: deferred `recover` that converts the panic via `recoveredPanicToError`
  (added in the DoFinally fix, `stream/shpan_stream.go`) and sends it on `itemChan` as a
  `Result{Err: ...}`, so it flows through `setErr` and is returned as `firstErr`. The DoFinally
  provider wrapper already records the panic before it unwinds, so hooks report it consistently.
- Worker goroutines: deferred `recover` feeding `setErr` with the converted error (consumer-side
  failure — DoFinally hooks must still fire `nil`, matching the sequential boundary).

`MapConcurrently` / `WithConcurrentMapOption` (`stream/concurrent_stream.go`):

- Reader and mapper-worker goroutines: deferred `recover` converting the panic into an error
  forwarded through the existing error channel/first-error machinery, so it surfaces as the
  provider error of the concurrent map stage (a pipeline failure — DoFinally hooks downstream of
  the stage should observe it).

## Tests to add

- Pipeline panic under concurrent consume: process survives, caller gets the recovered error,
  DoFinally hook gets the equivalent error.
- Consumer-callback panic in a worker: process survives, caller gets the recovered error,
  DoFinally hook fires `nil`.
- No success-before-crash race: the caller must never observe `err == nil` for a panicking drain.
- Pipeline panic under `WithConcurrentMapOption`: process survives, caller gets the recovered
  error, DoFinally hooks (both upstream-of-the-stage-with-panic-downstream and downstream of the
  stage) observe their locality-correct outcome.
