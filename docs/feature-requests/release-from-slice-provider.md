# Feature request: tag a release that contains `FromSliceProvider`

*Filed by a consumer in production, pinned at `v0.4.57`. Checked against master `bc9c340` on
2026-09-18.*

## Ask

Tag a release, for example `v0.4.58`, that includes `FromSliceProvider` (#128, `bc9c340`). It is
merged on master, but `git tag --contains bc9c340` returns nothing. The latest tag is `v0.4.57`,
one commit behind master, so no consumer can use `FromSliceProvider` yet.

## Why a consumer needs it

The consumer hand-builds the same source at eleven call sites. Each one lazily loads a slice, or a
value it turns into a slice, and streams it.

**The `FlatMap(FromLazy(NewLazy(load)), FromSlice)` shape, 8 sites.** These are repositories that
stream one slice field of a loaded document, or a loaded map's entries in sorted-key order.

**Hand-written `NewSimpleStream` state machines doing the same job, 3 sites:**
- collect, sort, re-emit (a generic `Sorted` helper);
- collect, read an ordering map, sort, re-emit;
- load a page of a feed, re-emit.

All eleven share the flaw `FromSliceProvider` was written to avoid: the stream works only once.
Probe results, collecting the same stream value twice:

```
FlatMap(FromLazy(NewLazy),FromSlice)     first=[1 2 3]  second=[]
FromSliceProvider                        first=[1 2 3]  second=[1 2 3]
```

`FromSliceProvider` fetches in `Open`, with the context the stream is consumed with, and fetches
again on each consumption. That is exactly what these sites need, with none of their per-site
state.

## Not in scope

Two more `FromLazy` sites in the consumer load a value and then produce a *stream*, not a slice.
`FromSliceProvider` doesn't fit them. The operator re-consumption fix, a separate request, covers
them instead.

## Caveat: panics in `Open`

`FromSliceProvider` calls the user's fetch function from `Open`, and a panic in `Open` skips
lifecycle cleanup and `DoFinally` (`docs/open-stage-panic-leak.md`: confirmed there, not fixed).

This does **not** make the consumer's current code any worse. Probe results with a panicking fetch:

```
FromSliceProvider panic: err=stream recovered error value: fetch panicked DoFinally fired=false
FlatMap(FromLazy) panic:  err=stream recovered error value: fetch panicked DoFinally fired=false
```

Both forms turn the panic into an error, and neither fires `DoFinally`, so switching is neutral on
this point. If the open-stage fix is small, it would be good to ship it in the same release, since
`FromSliceProvider` makes user code in `Open` the normal case.

## What the consumer does once it lands

1. Bump the pin: `go.mod` together with the strict-interface template URL in its oapi-codegen
   config.
2. Replace the eleven sites with `stream.FromSliceProvider(...)`.
3. Rewrite its `Sorted` helper in terms of it:
   `FromSliceProvider(func(ctx) { s, err := src.Collect(ctx); slices.SortFunc(s, cmp); return s, err })`.
4. Delete the per-site `collected` / `idx` / `loaded` state.

## Workaround today

None. Each site keeps its single-use state machine, or the consumer copies `sliceProviderStream`
into its own code.

## Size and priority

- Size: a tag and a release. No code.
- Priority: highest of this batch, because it unblocks the most call sites for the least effort.
