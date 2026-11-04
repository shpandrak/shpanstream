# Ask: a Stream element type costs ~220 KB of machine code and ~0.5 s of compile time

Status: items 1–4 done on branch `claude/compile-size-ask` (results at the end);
item 5 not done. Reproduction: `tools/compilesize/measure.sh`. Source of the
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

## Results (items 1–4, Go 1.24.7, 4 cores)

`tools/compilesize/measure.sh 50`, one commit per item. Bytes are exact; the
compile seconds are single runs on a shared VM whose speed drifted by ±20 %
during the session, so read them as a trend (the interleaved series below is
the fair comparison).

| Step | Compile | Shape instantiations | Per type |
|---|---|---|---|
| baseline (`27ae006`) | 28.4 s | 10,881,584 B (35,561 symbols) | 217,631 B |
| 1. concurrent fan-out over `any` | 28.3 s | 9,400,605 B (33,598) | 188,012 B |
| 2. `Untyped()` without `Map` | 26.1 s | 8,918,305 B (30,997) | 178,366 B |
| 3. thin `Stream[T]` bodies over a non-generic core | 13.9 s | 3,904,722 B (17,062) | 78,094 B |
| 4. `Lazy[T]` over a non-generic core | 11.9 s | 3,707,300 B (16,557) | 74,146 B |

Interleaved baseline/final runs, same machine, same minutes:

| Run | Baseline | Final |
|---|---|---|
| 1 | 36.9 s | 12.9 s |
| 2 | 32.9 s | 13.8 s |
| 3 | 31.0 s | 11.5 s |

By family, final: `stream.Stream` 1.48 MB (was 3.89), `stream.(*Stream)`
0.42 MB (was 2.07), `lazy.Lazy` + `lazy.(*Lazy)` 0.93 MB (was 1.55),
`concurrentStreamMapperProvider` 0.03 MB (was 1.22), `mapStreamConcurrently`
0.07 MB (was 0.51), `MapWithErrAndCtx` 0.05 MB (was 0.41).

What the remaining 3.7 MB is: every `Stream[shape]` still carries all 27
methods (29 before item 1 moved the two concurrent-consume methods out of the
type) and `Lazy[shape]` all 21 (the thin bodies, about 34 KB per shape in
total), plus the compiler-generated pointer-receiver wrappers, which are
call-through stubs whose size comes from moving `T` by value through the
shape dictionary (so `//go:noinline` cannot shrink them). The probe's other
2.1 MB of text are the per-concrete-type dictionary wrappers Go emits for
each method. Only item 5 (fewer methods) reduces either further.

Where the bytes came from, mechanically: besides the four items, the single
biggest lever was that the compiler inlines a method that creates a closure
into every call site and into its pointer wrapper, emitting a fresh copy of
the closure body each time (nine copies of the Filter closure per element
type in the baseline). Closure-creating generic methods and helpers are now
marked `//go:noinline`; they run at pipeline construction, never per element.

Hot path (`stream/hot_path_bench_test.go`, 1024 struct elements): no
sequential benchmark got slower. The deterministic part is the allocation
count per drain: Consume 6 → 4, Map+Filter+Collect 16 → 14, Limit+Skip
10 → 8, DoFinally 6 → 4 (FindFirst stays at 9, +24 B for the Lazy fetcher
adapter), because the consume step now calls the provider and the consumer
directly instead of through two adapter closures. In ns/op, Consume is
reliably faster (about 7 % on interleaved baseline/final runs, `-count=3`);
the other benchmarks move within the VM's run-to-run noise (±15 %), so the
single-run speedups seen during the session are not claimed here. Boxing
into `any` happens only on the opt-in concurrent paths and in `Untyped()`,
where it always did.

## Follow-up: Stream without Lazy (item 4)

`FindFirst`/`FindLast` now return `(T, bool, error)` and package `stream` no longer imports
`lazy` (see [migration-stream-without-lazy.md](migration-stream-without-lazy.md)). `measure.sh 50`,
three interleaved runs each: shape instantiations 3,707,300 → 2,809,445 bytes (74,146 → 56,188 per
type), text 5,794,965 → 4,139,647 bytes, symbols 28,170 → 18,474, median compile 17.2 s → 10.3 s.
No `lazy.` symbol is left in the probe.
