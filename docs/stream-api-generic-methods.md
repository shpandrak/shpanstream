# Why `Stream` has no plain methods

`stream.Stream[T]` has no plain methods. The methods that take a function (`Filter`, `Map`,
`Consume`, `Peek`, ...) are Go 1.27 generic methods; everything else (`Collect`, `FindFirst`,
`Limit`, `MustValues`, ...) is a package function. This note explains why: what a Stream element type
costs the compiler, and how the API is shaped to keep that cost to what a program calls.

## What a generic type costs

Go compiles every plain method of a generic type for every element type it is instantiated with,
whether the program calls the method or not. For each method it emits four copies: the shape body,
the pointer-receiver wrapper, and a per-concrete-type dictionary wrapper for each of those two. A
generic *function*, by contrast, is compiled only for the instantiations that are called. Every
importing package compiles its own copies; the linker deduplicates them and drops the unused ones,
so most of the cost shows up as compile time and build memory, less as binary size.

For a library type like `Stream[T]`, which applications instantiate with tens of element types,
this dominates the build. In the application that prompted this work (shpankids), a one-line
change cost a 48 s rebuild: three 7–8 k-line packages sat on one serial chain, the compiler profile
was pure SSA backend and GC, and in one of those packages 5.9 MB of its 7.4 MB of machine code were
instantiations of this library over 73 element types. A design where `Stream[T]` carried 27 plain
methods paid for all 27 in every one of those types and packages.

## Keeping each instantiation small

Two rules keep the per-type cost low.

**Thin generic shells over a non-generic core.** Everything that does not touch `T` is compiled
once, and the generic code only adapts `T` to it:

- The sequential drain (opening and closing the lifecycle elements, the drain loop, the panic
  guards) lives in `stream/stream_core.go`. The generic code supplies one step that pulls an element
  and hands it to the consumer, at the cost of one indirect call per element.
- The concurrent fan-out (`WithConcurrentMapOption`, `WithConcurrentConsumeOption`) runs over boxed
  `any` values in `stream/concurrent_core.go`, so its goroutine machinery is compiled once, not per
  element type; the generic entry points only box and unbox. Boxing allocates, but only on these
  opt-in paths.
- `Untyped` is a one-line boxing provider, not `Map(s, func(T) any)`, which would instantiate the
  whole Map machinery for a `[T, any]` pair per element type.
- `lazy.Lazy[T]` is likewise a shell over the non-generic `lazyCore`.
- A generic method or function that creates a closure is `//go:noinline`. Otherwise the compiler
  inlines it into every call site (and into every pointer-receiver wrapper of a plain method) and
  emits a fresh copy of the closure body each time; nine copies of the `Filter` closure per element
  type were measured that way. These calls build pipelines and never run per element, so the lost
  inlining costs nothing on the hot path.
- Package `stream` does not import package `lazy`. `FindFirst` and `FindLast` return
  `(T, bool, error)`; the `Lazy`-returning bridges live in package `lazy` (`lazy.FindFirst(s)`,
  `lazy.Reduce(s, ...)`, ...), so only code that uses them pays for `Lazy[T]`.
  `stream/no_lazy_import_test.go` guards this.

**No plain methods.** The shells make each method small, but with plain methods every element type
still pays for all of them, and for their wrappers. Only removing plain methods removes that.

## The Go 1.27 fact this rests on

Go 1.27 added methods with their own type parameters. Tested on 1.27.3: such a method is compiled
only for the instantiations that are called, with no pointer wrapper and no per-type dictionary
wrapper. It costs the same as a package function. This is structural, not an accident of the
implementation: a generic method cannot satisfy an interface, so the compiler never has to compile
it ahead of a call.

A method that takes a function argument can carry that function's type as its type parameter. The
parameter is inferred, so call sites read like plain method calls:

```go
func (s Stream[T]) Filter[P ~func(T) bool](predicate P) Stream[T]

s.Filter(func(p Person) bool { return p.Age > 18 })
s.Filter(isAdult) // a named shpanstream.Predicate[T], or any func(T) bool
```

`stream/method_inference_test.go` pins this inference for func literals, named function types
and top-level functions.

## The split rule

- **A method takes a function: it is a generic method on `Stream[T]`.** `Consume`, `MustConsume`,
  `ConsumeWithErr`, `ConsumeWithErrAndCtx`, `Filter`, `FilterWithErr`, `FilterWithErrAndCtx`,
  `Peek`, `DoFirst` and `DoFinally` take the function's type as a type parameter (`F ~func(T)`).
  Each is a thin shell that converts its argument to the plain function type and calls the shared
  `//go:noinline` implementation, so a new `F` adds only a tiny shell. `Map`, `MapWithErr`,
  `MapWithErrAndCtx` and `FlatMap` take the target type (`Map[U any]`), so a chain can change
  element type without leaving it, and a plain function type (`mapper func(T) U`), so `U` is
  inferred from a value of a named function type such as `type ToName func(Person) string` too.
- **Anything else is a package function.** Terminal operations take `ctx` first, then the stream:
  `stream.Collect(ctx, s)`, `stream.Count(ctx, s)`, `stream.IsEmpty(ctx, s)`,
  `stream.FindFirst(ctx, s)`, `stream.FindLast(ctx, s)`, `stream.CollectRandomSample(ctx, s, n)`,
  like `Reduce`, `CollectToMap` and `Max`. Their `Must` forms take only the stream:
  `stream.MustCollect(s)`, `stream.MustCount(s)`. Transforms take the stream first:
  `stream.Limit(s, n)`, `stream.Skip(s, n)`, `stream.Page(s, page, size)`,
  `stream.RandomSample(s, n)`, `stream.WithAdditionalLifecycle(s, l)`,
  `stream.WithLockWhileMaterializing(s, mu)`, `stream.Untyped(s)`. `stream.MustValues(s)` returns an
  `iter.Seq[T]` and `stream.MustAll(s)` an `iter.Seq2[int, T]`; both panic if the stream fails.

The package functions `stream.Map`, `MapWithErr`, `MapWithErrAndCtx` and `FlatMap` exist alongside
the methods; they are the implementation the methods delegate to, and cost nothing unless called.

A typical pipeline:

```go
names, err := stream.Collect(ctx,
    repo.Stream().
        Filter(isAdult).
        Map(func(p Person) string { return p.Name }))
```

What the rule costs callers: an untyped `nil` cannot be passed for a function argument (`F` cannot
be inferred from it); a method value needs its type argument spelled (`s.Filter[func(int) bool]`) or
a closure; and an interface listing `Filter`, `Consume` and so on is never satisfied by `Stream[T]`.
Every module that depends on shpanstream requires `go 1.27` (`tsquery-parser-codegen` does not
depend on it and stays on `go 1.25.3`).

## Alternatives considered

Per-type cost measured with `tools/compilesize/measure.sh 50` on Go 1.27.3:

| Design | Per type | Min Go | Call sites |
|---|---|---|---|
| Plain methods over the non-generic core | 45 KB | 1.23 | fluent |
| Every operation a package function, no methods | 12.3 KB | 1.23 | no chaining at all |
| Generic methods for the func-taking methods, the rest plain methods | 35 KB | 1.27 | fluent |
| **Generic methods for the func-taking methods, package functions for the rest** | **11.7 KB** | **1.27** | fluent up to the terminal operation |

- **Dummy inferable type parameters** on the methods without a function argument (for example
  `Collect[C context.Context](ctx C)`) measured the same as this design. Rejected: the parameter
  exists only to work around the compiler, and makes godoc misleading.
- **Two APIs**, methods plus a functions-only package: both styles would need tests and docs, and
  prior art (go-functional's `itx`, pie) shows the second always ends up incomplete. This design
  keeps the fluent part already.
- **The same recipe for `lazy.Lazy[T]`**: measured on Go 1.27.3 with 50 types, each doing `NewLazy`,
  `Get`, `Filter` + `OrElse` and a JSON field. Generic methods for the func-taking methods and
  package functions for `OrElse`, `Or`, `IsEmpty` and the rest cut 9.1 KB per type to 3.4 KB. The
  5.7 KB saved is small next to Stream's 33 KB; only types actually wrapped in a `Lazy` pay it; and
  3.4 KB of it needs a hard break of the most common calls (`OrElse`, `Or`, `IsEmpty`). `Lazy` keeps
  its plain methods.

## Measurements

`tools/compilesize` generates N distinct struct element types, each used the way an application
uses a Stream (a slice source, `Map` to an `Entry`, `Filter`, `Collect`, `FindFirst`, nothing
concurrent), compiles them against the working copy and reports compile time and the bytes of
generic instantiations (`go.shape` symbols). Run `measure.sh N` before and after a change, with the
same N.

This design against the same code with plain methods, Go 1.27.3, Apple M4 Pro, medians of three
interleaved runs:

| `measure.sh` probe | Plain methods | This design |
|---|---|---|
| N=50: compile | 1.6 s | 0.4 s |
| N=50: text / shape bytes | 3,249,216 / 2,249,536 | 813,488 / 585,536 |
| N=50: shape / all text symbols | 11,102 / 18,472 | 2,752 / 3,854 |
| N=50: **shape per type** | **44,990 B** | **11,710 B** |
| N=200: compile | 6.4 s | 1.5 s |
| N=200: text / shape bytes | 13,664,992 / 9,536,288 | 3,515,440 / 2,562,016 |
| N=200: shape / all text symbols | 44,102 / 73,372 | 11,002 / 15,404 |
| N=200: **shape per type** | **47,681 B** | **12,810 B** |
| N=50 with `Stream[*T]` elements: compile, per type (1 run) | 1.2 s, 21,088 B | 0.4 s, 6,615 B |
| Binary of a `main` calling all 50 probes: plain / `-s -w` | 8,015,826 / 5,594,002 B | 6,963,954 / 4,776,274 B |

Sizes are identical across runs. The hot path does not change: `go test -run '^$' -bench .
-benchmem ./stream/` (`stream/hot_path_bench_test.go`, 1024 struct elements, 6 samples per tree,
benchstat) puts time between −1.25 % (`DoFinally`) and +1.72 % (`MustValuesFull`), geomean +0.54 %,
with B/op and allocs/op identical for all 8 benchmarks.

What each lever is worth, per element type at N=50. The first rows were measured on Go 1.24–1.25
on a shared 4-core VM, the last two on Go 1.27.3, so compare within a group:

| Design | Per type |
|---|---|
| Plain methods with full generic bodies | 217.6 KB |
| + concurrent fan-out over `any` | 188.0 KB |
| + `Untyped` without `Map` | 178.4 KB |
| + thin `Stream[T]` bodies over a non-generic core | 78.1 KB |
| + `Lazy[T]` over a non-generic core | 74.1 KB |
| + `stream` not depending on `lazy` | 56.2 KB |
| Same design, Go 1.27.3 | 45.0 KB |
| + no plain methods (this design) | 11.7 KB |

What is left per type is the code a program actually calls: the thin shells and the closures that
touch `T`.
