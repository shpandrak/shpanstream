# Feature request: stateful operators should reset their state on every `Open`

*Filed by a consumer in production, pinned at `v0.4.57`. Reproduced on master `bc9c340` on
2026-09-18.*

## Ask

Every operator that keeps per-consumption state should reset it when the stream is opened. Then a
stream value behaves the same however many times it is consumed, and however far each earlier
consumption got.

Sources and plain `Map`/`FlatMap` already behave this way, and the library already has tests that
consume a stream twice for merge, concat, flatmap, map and `Buffered`. The operators below don't:
they keep their state in variables captured once, when the stream is built, and never reset them.

| Operator | State kept since the stream was built | Where |
|---|---|---|
| `FromLazy` | `alreadyFetched` | `stream/shpan_stream.go:373` |
| `Limit` (and `Page`) | `alreadyConsumed` | `stream/paging.go:13` |
| `Skip` (and `Page`) | `alreadySkipped` | `stream/paging.go:30` |
| `Window` | `buffer`, `done` (open function is `nil`) | `stream/window_stream.go:57-60` |
| `JoinSortedStreams` (and likely the other sorted joins) | `firstElement`, `lastLeftKey`, `lastRightKey`, `lastRightValue` | `stream/join_stream.go:16-25` |

## Current behaviour (probe)

Each line collects the same stream value twice over `Just(1, 2, 3, 4, 5)`:

```
FromSlice (control)   first=[1 2 3 4 5]         second=[1 2 3 4 5]
Map (control)         first=[10 20 30 40 50]    second=[10 20 30 40 50]
FromLazy              first=[7]                 second=[]
Limit(2)              first=[1 2]               second=[]
Skip(2)               first=[3 4 5]             second=[1 2 3 4 5]
Page(1,2)             first=[3 4]               second=[]
Window(2)             first=[[1 2] [3 4] [5]]   second=[]
JoinSortedStreams     first=[1 2 3 4 5]         second=(err: left stream is not sorted 1 < 5)
Limit(3): FindFirst then Collect  ->  FindFirst=1, then Collect=[1 2]
```

There are three kinds of failure here, not one:
- **Empty.** `FromLazy`, `Limit`, `Page` and `Window` return nothing on the second consumption.
- **Wrong elements.** `Skip` stops skipping. `Limit` gives a short result after a partial read, as
  in the last line: `FindFirst` took one element, so the following `Collect` got 2 elements
  instead of 3.
- **A false error.** `JoinSortedStreams` claims its input isn't sorted.

None of these is visible to the caller.

## Why a consumer needs it

The consumer uses `FromLazy` in 10 places and `Limit` in two live places, one of them a
newest-first feed. Today each of those streams happens to be consumed exactly once, so nothing is
broken yet. The trap is Firestore transactions:
- Firestore reruns a transaction's closure when there is contention.
- The consumer opens streams with the transaction carried in the consumer's context.
- So a stream built *outside* a retried closure and consumed *inside* it would come back empty on
  the retry, and the transaction would commit a decision based on "no rows".

The consumer's code builds its streams inside the closure, but only by convention; nothing enforces
it. Operators that reset on `Open` would remove the trap entirely.

## Proposed behaviour

Initialise per-consumption state in the operator's open step, not when the stream is built. For
example:
- move `Limit`'s counter into a lifecycle `Open`; or
- build the provider function inside `Open`, the way `sliceProviderStream.Open` resets `idx`.

`FromLazy` should fetch again on each consumption. That is what `Lazy` itself does, because it
isn't memoized.

## Acceptance tests

For each operator in the table:
1. Collect the same stream value twice: both results are equal.
2. Consume it partially (`FindFirst`, or `Limit` downstream), then collect: the collect sees the
   full result.
3. For `JoinSortedStreams` and the other sorted joins: a second consumption returns the same
   pairs, with no "not sorted" error.

## What the consumer does once it lands

Nothing at the call sites: every current use keeps working and the trap disappears. Its two
`FromLazy` sites that wrap a lazily loaded value into a stream also become safe to consume again.

## Workaround today

Rely on convention: always build a stream inside the transaction closure that consumes it, and
never consume a stream value twice.

## Size and priority

- Size: small per operator. Mostly moving variable initialisation into an open step, plus tests.
- Priority: high as a library correctness fix. For this consumer it is a latent trap, not an
  active bug.
