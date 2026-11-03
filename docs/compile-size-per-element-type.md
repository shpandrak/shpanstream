# Ask: a Stream element type costs ~220 KB of machine code and ~0.5 s of compile time

Status: open. Reproduction: `tools/compilesize/measure.sh`. Source of the
finding: the shpankids application (its spec
`docs/superpowers/specs/2026-11-03-check-gate-performance.md`).

## The symptom, in an application

A one-line change in a shpankids domain package costs 48 s of `go build`,
because three 7–8 k-line packages take 18 s, 11 s and 10.5 s to compile and
sit on one serial chain. A compiler CPU profile is pure SSA backend and GC:
code volume. `go tool nm -size` on `domain/course` (7,238 lines): 7.4 MB of
text, 5.9 MB of it (76 % of the functions) generic instantiations of this
library, over 73 distinct element types. The production binary carries 8 MB
of them.

## The reproduction here, with nothing from that application

`tools/compilesize/gen.sh N` writes a package with N distinct struct types,
each used the way an application repository uses a Stream: a slice-backed
source, `Map` to an `Entry[string, T]`, `Filter`, `Collect`, `FindFirst`.
Nothing concurrent. `measure.sh N` compiles it against the working copy
(`replace … => ../..`) and reports the instantiated code.

Against `bbfdacf` (v0.4.58), Go 1.25, 4 cores:

| Types | Compile | Shape instantiations | Per type |
|---|---|---|---|
| 1 | 0.7 s | 303 KB (1,016 symbols) | 303 KB |
| 10 | 5.2 s | 2.3 MB (7,361) | 231 KB |
| 50 | **25.8 s** | **10.9 MB (35,561)** | 218 KB |

Linear in the number of types. Where the 10.9 MB goes, by family:

| Bytes | Symbols | Family |
|---|---|---|
| 3.89 MB | 10,908 | `stream.Stream` value methods |
| 2.07 MB | 6,464 | `stream.(*Stream)` — the compiler-generated pointer wrappers, inlining the above |
| 1.22 MB | 2,008 | `stream.(*concurrentStreamMapperProvider)` |
| 0.78 MB | 3,737 | `lazy.Lazy` |
| 0.77 MB | 3,333 | `lazy.(*Lazy)` |
| 0.51 MB | 1,757 | `stream.mapStreamConcurrently` |
| 0.41 MB | 1,255 | `stream.MapWithErrAndCtx` |
| 0.30 MB | 1,506 | `shpanstream.Mapper` and its pointer wrapper |

## Why each type costs this much

1. **A generic type instantiates its whole method set.** Every
   `Stream[shape]` carries all 29 methods whether the caller uses them or not
   (`Consume`, `MustConsume`, `ConsumeWithErr`, `ConsumeWithErrAndCtx`,
   `FindFirst`, `FindLast`, `Collect`, `MustCollect`, `Filter`,
   `FilterWithErr`, `FilterWithErAndCtx`, `Count`, `MustCount`, `IsEmpty`,
   `WithAdditionalLifecycle`, `Peek`, `DoFinally`, `DoFirst`, `Limit`, `Skip`,
   `Page`, `CollectRandomSample`, `RandomSample`, `WithLockWhileMaterializing`,
   `Iterator`, `IndexedIterator`, `Untyped`, `consumeConcurrently`,
   `doConsumeConcurrently`). A generic *function* is instantiated only when
   called. shpankids calls at most 12 of the 29.
2. **The pointer wrappers double it.** `(*Stream).Filter` is as large as
   `Stream.Filter` (about 100 KB each in `domain/course`): the wrapper inlines
   the body.
3. **The concurrent machinery is built for every `Map`.** `MapWithErrAndCtx`
   calls `mapStreamConcurrently` behind a runtime option check, so every
   `Map`/`MapWithErr`/`FlatMap` pair instantiates the goroutine provider's
   open/emit/close and their closures (one `mapStreamConcurrently` per Map
   pair, measured 142 = 142 in `domain/course`). `ConsumeWithErrAndCtx`
   references `consumeConcurrently` the same way. shpankids never calls
   `WithConcurrent*`.
4. **`FindFirst` returns `Lazy[T]`**, so every Stream shape also builds Lazy's
   ~23 methods, `MarshalJSON`/`UnmarshalJSON` included.
5. **`Untyped()` is a method whose body is `Map(s, func(T) any)`**, so every T
   drags in `MapWithErrAndCtx[T,any]`, `mapStreamConcurrently[T,any]` and the
   provider for `any` — one `[T, any]` pair per Stream shape.
6. **Each importing package stencils its own copy**; the linker dedupes them
   later, so the compile cost is paid once per package, the binary cost once.

## The ask, ranked by bytes saved per API risk

1. **Concurrent paths in non-generic code over `any`.** Keep
   `mapStreamConcurrently` and `consumeConcurrently` as thin generic adapters
   that box the provider and mapper as `func(ctx) (any, error)` and unbox the
   result; the boxing cost is paid only when the concurrent option is set.
   No API change. About 1.7 MB of the 10.9 MB here (families 3, 6).
2. **`Untyped()` without `Map`**: `newStream` and a one-line closure. No API
   change. About 0.4 MB on its own, less after 1.
3. **Thin method bodies over non-generic helpers**: the open/close lifecycle,
   the panic guard, defers, EOF handling, the `Limit`/`Skip`/`Page` counters,
   the `DoFirst`/`DoFinally` hooks into a non-generic `consumeLoop(ctx,
   lifecycles, step func(ctx) error)` where only `step` is generic. The
   pointer wrappers shrink with the bodies, so this counts twice. No API
   change; one indirect call per element. Estimated 30–50 % of families 1–2.
4. **`Lazy` as a thin generic shell over a non-generic core** (fetcher, empty
   error, JSON on `any`). Low API risk. Up to 1.5 MB here.
5. **Rarely used methods as free generic functions** (`Page`, `Skip`,
   `RandomSample`, `CollectRandomSample`, `WithLockWhileMaterializing`,
   `DoFirst`, `FindLast`, `Iterator`, `IndexedIterator`, `Must*`, `Untyped`),
   built only when called. **A source break for callers** — keep for a major
   version, or not at all; items 1–4 should be enough.

Acceptance: `tools/compilesize/measure.sh 50` under 12 s and under 5 MB of
shape instantiations, `make check` green, and the library's own benchmarks
(if any) unchanged on the non-concurrent path. Then shpankids bumps the
version and re-measures `go build ./...` after a one-line change in
`domain/exercise` (baseline 48 s).

Bytes are measured; compile-time gains are estimates until measured.
