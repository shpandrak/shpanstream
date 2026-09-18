# Feature request: tag a release that contains `FromSliceProvider`

*Requested by shpankids, a consumer pinned at `v0.4.57`. Checked against master `bc9c340` on
2026-09-18.*

## Ask

Tag a release, `v0.4.58`, that includes `FromSliceProvider` (#128, `bc9c340`). It has been merged
to master, but `git tag --contains bc9c340` returns nothing. The latest tag is `v0.4.57`, one
commit behind master, so no consumer can use `FromSliceProvider` yet.

## Why shpankids needs it

shpankids builds the same source by hand in eleven places. Each one lazily loads a slice (or a
value it turns into a slice) and streams it.

**The `FlatMap(FromLazy(NewLazy(load)), FromSlice)` shape, 8 sites:**
- `infra/database/kvstore/in_memory_raw_json_store.go:104`: the in-memory store's ordered query
- `domain/course/course_structure.go:76` and `:302`: a course's levels and chapters
- `domain/course/unit_content.go:65`: one field of a unit's content document
- `domain/enrollment/unit_supplier.go:60` and `:69`: a unit's lesson cards and dialogues
- `domain/enrollment/tag_stats_repository.go:113`: a kid's tag stats, sorted
- `domain/enrollment/lexicon_stats_repository.go:111`: a kid's word stats, sorted

**Hand-written `NewSimpleStream` state machines with the same job, 3 sites:**
- `infra/util/streamutil/sorted.go:17`: collect, sort, re-emit
- `domain/course/lexicon.go:362`: collect, read an order map, sort, re-emit
- `domain/activity/user_activity_manager.go:102`: load a feed page, re-emit

All eleven sites share the flaw `FromSliceProvider` was written to avoid: the stream works only
once. Probe results, collecting the same stream value twice:

```
FlatMap(FromLazy(NewLazy),FromSlice)     first=[1 2 3]  second=[]
FromSliceProvider                        first=[1 2 3]  second=[1 2 3]
```

`FromSliceProvider` fetches in `Open`, with the context the stream is consumed with, and fetches
again on each consumption. That is exactly what these sites need, with none of their per-site
state.

## Not in scope

shpankids has two more `FromLazy` sites that load a value and then produce a *stream*, not a slice:
- `infra/database/kvstore/in_memory_raw_json_store.go:385`
- `internal/api/oapi_shpankids_api_impl.go:74`

`FromSliceProvider` doesn't fit them. The operator re-consumption fix, a separate request, covers
them instead.

## Caveat: panics in `Open`

`FromSliceProvider` calls the user's fetch function from `Open`, and a panic in `Open` skips
lifecycle cleanup and `DoFinally` (`docs/open-stage-panic-leak.md`: confirmed there, not fixed).

This does **not** make shpankids' current code any worse. Probe results with a panicking fetch:

```
FromSliceProvider panic: err=stream recovered error value: fetch panicked DoFinally fired=false
FlatMap(FromLazy) panic:  err=stream recovered error value: fetch panicked DoFinally fired=false
```

Both forms turn the panic into an error, and neither fires `DoFinally`, so switching is neutral on
this point. If the open-stage fix is small, it would be good to ship it in the same release, since
`FromSliceProvider` makes user code in `Open` the normal case.

## What shpankids does once it lands

1. Bump the pin: `go.mod` together with the strict-interface template URL in `tools/oapi-conf.yaml`.
2. Replace the eleven sites with `stream.FromSliceProvider(...)`.
3. Rewrite `streamutil.Sorted` in terms of it:
   `FromSliceProvider(func(ctx) { s, err := src.Collect(ctx); slices.SortFunc(s, cmp); return s, err })`.
4. Delete the per-site `collected` / `idx` / `loaded` state.

## Workaround today

None. Each site keeps its single-use state machine, or shpankids copies `sliceProviderStream` into
its own code.

## Size and priority

- Size: a tag and a release. No code.
- Priority: highest of the shpankids requests, because it unblocks the most call sites for the
  least effort.
