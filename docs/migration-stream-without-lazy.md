# Migration: package `stream` without package `lazy`

Since v0.5.0, package `stream` no longer imports package `lazy`. This is a breaking change.

**Why.** Go compiles every method of a generic type for every element type it is instantiated
with, called or not. `Stream[T].FindFirst()` returned `lazy.Lazy[T]`, so every `Stream[T]` in a
program also compiled all of `Lazy[T]`'s methods. Removing that edge cuts the generic code a Stream
element type costs by about a quarter (`tools/compilesize/measure.sh 50`: 74 → 56 KB per element
type), with matching gains in compile time and binary size for applications with many element types
(see [compile-size-per-element-type.md](compile-size-per-element-type.md)).

Absence is now a `bool`, not an error: `FindFirst`/`FindLast` return `(T, bool, error)` and an
empty stream gives `found == false, err == nil`. Where you relied on `Get` failing for an empty
stream, check `found` yourself.

## Old call → new call

| Before | After (no `lazy`) | Lazy bridge, if you still want a `Lazy` |
|---|---|---|
| `s.FindFirst().Get(ctx)` | `v, found, err := s.FindFirst(ctx)` | `lazy.FindFirst(s).Get(ctx)` |
| `s.FindFirst().GetOptional(ctx)` / `MustGetOptional()` | `v, found, err := s.FindFirst(ctx)` | `lazy.FindFirst(s).GetOptional(ctx)` |
| `s.FindFirst().IsEmpty(ctx)` | `s.IsEmpty(ctx)` | |
| `s.FindLast().Get(ctx)` | `v, found, err := s.FindLast(ctx)` | `lazy.FindLast(s).Get(ctx)` |
| `stream.FindFirstAndLast(s).Get(ctx)` | `fl, found, err := stream.FindFirstAndLast(ctx, s)` | `lazy.FindFirstAndLast(s)` |
| `stream.FromLazy(l)` | one of the deferred sources below | `lazy.ToStream(l)` |
| `stream.ReduceLazy(s, init, f)` (and `…WithErr`, `…WithErrAndCtx`) | `func(ctx context.Context) (R, error) { return stream.Reduce(ctx, s, init, f) }` | `lazy.Reduce(s, init, f)` (and `lazy.ReduceWithErr`, `lazy.ReduceWithErrAndCtx`) |
| `stream.MaxLazy(s)` / `stream.MinLazy(s)` | `stream.Max(ctx, s)` / `stream.Min(ctx, s)` | `lazy.Max(s)` / `lazy.Min(s)` |
| `type stream.Reducer[S, T] func(Stream[S]) lazy.Lazy[T]` | `type stream.Reducer[S, T] func(ctx, Stream[S]) (T, error)` (the shape of `stream.Max`) | |

The bridges live in package `lazy` (which now imports `stream`), keep the old semantics, including
the "no \"first element\" in an empty stream" error from `Get`, and cost the `Lazy` instantiation only
where they are used.

## Deferred sources

`stream.FromLazy` wrapped whichever deferred shape the `Lazy` held. Each shape now has its own
constructor in `stream`, named after `FromSliceProvider`. The function runs when the stream is
materialized, once per materialization, with the materialization context.

| Deferred shape | Before | After |
|---|---|---|
| slice | `stream.FromSliceProvider(f)` | unchanged: `stream.FromSliceProvider(func(ctx) ([]T, error))` |
| single value | `stream.FromLazy(lazy.NewLazy(f))` | `stream.FromValueProvider(func(ctx) (T, error))` |
| optional value | `stream.FromLazy(lazy.NewLazyOptional(f))` with `f` returning `(*T, error)` | `stream.FromOptionalProvider(func(ctx) (T, bool, error))`; `FromOptionalProvider(s.FindFirst)` streams `s`'s first element |
| stream | `stream.Concat(stream.FromLazy(lazy.NewLazy(f)))` | `stream.FromStreamProvider(func(ctx) (Stream[T], error))` |

One behavioural difference: `FromLazy` evaluated its `Lazy` on the first materialization only and
streamed nothing afterwards; the new sources (and `lazy.ToStream`) evaluate again on every
materialization, like `FromSliceProvider`. `FromStreamProvider` runs its function when the stream is
opened, and its error reaches the consumer wrapped (match with `errors.Is`); `FromValueProvider`
and `FromOptionalProvider` return the function's error as is.

A smaller one: `FindFirst` returns as soon as it has read the first element, so a context cancelled
after that element was read no longer turns the result into `context.Canceled`.
