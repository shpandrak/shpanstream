# Migrating to a newer shpanstream

Each section covers one breaking release. Coming from v0.4, do the v0.5 section first, by hand
(it is small), then the v0.6 one, which is mostly automated. Code samples use the current call
forms.

## v0.5 to v0.6: `Stream` without plain methods

**Why.** Go compiles every plain method of a generic type for every element type it is instantiated
with, called or not, and `Stream[T]` had 27 of them. In v0.6 it has none: the methods that take a
function are Go 1.27 generic methods, compiled only when called, and the rest are package
functions. On the `tools/compilesize` probe (Go 1.27.3, 50 element types) compile time drops from
1.6 s to 0.4 s and the generic code per element type from 45 KB to 11.7 KB; the design and the full
measurements are in [stream-api-generic-methods.md](stream-api-generic-methods.md).

### What changes in your code

The methods that take a function are called exactly as before: `Consume`, `MustConsume`,
`ConsumeWithErr`, `ConsumeWithErrAndCtx`, `Filter`, `FilterWithErr`, `Peek`, `DoFirst`,
`DoFinally`. New `Map`, `MapWithErr`, `MapWithErrAndCtx` and `FlatMap` methods let a chain map
without leaving it (`s.Filter(f).Map(g)`); the package functions `stream.Map` and friends are
unchanged. Everything else moves:

| v0.5 | v0.6 |
|---|---|
| `s.Collect(ctx)` | `stream.Collect(ctx, s)` |
| `s.MustCollect()` | `stream.MustCollect(s)` |
| `s.Count(ctx)` | `stream.Count(ctx, s)` |
| `s.MustCount()` | `stream.MustCount(s)` |
| `s.IsEmpty(ctx)` | `stream.IsEmpty(ctx, s)` |
| `s.FindFirst(ctx)` | `stream.FindFirst(ctx, s)` |
| `s.FindLast(ctx)` | `stream.FindLast(ctx, s)` |
| `s.CollectRandomSample(ctx, n)` | `stream.CollectRandomSample(ctx, s, n)` |
| `s.Limit(n)` | `stream.Limit(s, n)` |
| `s.Skip(n)` | `stream.Skip(s, n)` |
| `s.Page(p, size)` | `stream.Page(s, p, size)` |
| `s.RandomSample(n)` | `stream.RandomSample(s, n)` |
| `s.WithAdditionalLifecycle(l)` | `stream.WithAdditionalLifecycle(s, l)` |
| `s.WithLockWhileMaterializing(mu)` | `stream.WithLockWhileMaterializing(s, mu)` |
| `s.Untyped()` | `stream.Untyped(s)` |
| `for v := range s.Iterator` | `for v := range stream.MustValues(s)` (an `iter.Seq[T]`; panics if the stream fails, as before) |
| `for i, v := range s.IndexedIterator` | `for i, v := range stream.MustAll(s)` (an `iter.Seq2[int, T]`) |
| `s.FilterWithErAndCtx(p)` | `s.FilterWithErrAndCtx(p)` (spelling fixed) |

Terminal operations take `ctx` first, then the stream; transforms take the stream first.

### Go 1.27 and your tooling

- shpanstream v0.6 requires Go 1.27 (`go 1.27` in its `go.mod`), so `go get` raises your module's
  `go` directive to at least 1.27.
- Every tool that parses or type-checks your code must understand generic methods. That includes
  `gofmt` and `go vet` from older Go installs, `gopls`, linters (`golangci-lint`, `staticcheck`) and
  code generators. Tools built on `golang.org/x/tools` need v0.52 or later. An older tool fails with
  a parse or type error on the shpanstream sources or on your call sites, not with a lint finding.

### What no longer compiles, and what to write instead

- **An untyped `nil` for a function argument**, such as `s.Filter(nil)`: the function's type is a
  type parameter and cannot be inferred from `nil`. Pass a function, or drop the call.
- **Method values and method expressions of the generic methods**, such as `f := s.Filter` or
  `Stream[int].Filter`: write `s.Filter[func(int) bool]`, or a closure.
- **Interfaces that list Stream methods.** A generic method never satisfies an interface, so
  `Stream[T]` no longer implements an interface with `Filter`, `Consume` and so on. Wrap the stream
  in your own type, or pass it as a `stream.Stream[T]`.
- **Methods promoted through an embedded `Stream`** (`type People struct{ stream.Stream[Person] }`,
  then `people.Collect(ctx)`): call the function on the field, `stream.Collect(ctx, people.Stream)`.

### The rewriter: `tools/streamrewrite`

`tools/streamrewrite` rewrites a module's call sites using type information. It covers the table
above, including method values of the moved methods (`s.Collect` becomes a function literal) and the
`FilterWithErAndCtx` rename, adds the `stream` import where a file needs it, and respects your import
name for the package. It does not touch the four cases in the previous section, except that it
reports promoted methods.

Run it from the root of the module to migrate, while that module still requires v0.5, so that
every call site type-checks:

```sh
gofmt -l -w .   # the tool gofmt-formats each file it edits; format first so the diff is only the migration
go run github.com/shpandrak/shpanstream/tools/streamrewrite@v0.6.0 ./...
go get github.com/shpandrak/shpanstream@v0.6.0
go build ./... && go vet ./... && go test ./...
```

The `go run …@v0.6.0` form needs the `tools/streamrewrite/v0.6.0` tag. Until that tag exists, build
the tool from a shpanstream checkout and run the binary in your module (`go run` with a path outside
your module fails):

```sh
go -C /path/to/shpanstream/tools/streamrewrite build -o /tmp/streamrewrite .
/tmp/streamrewrite ./...
```

Run it once, before upgrading, while `go.mod` still requires v0.5.x. If the module already requires
v0.6, the tool changes nothing and exits 2: revert the version, run the tool, then upgrade.

Flags: `-n` prints the same report as a real run without writing any file; `-tags t1,t2` loads files
with those build tags; `-v` also prints the type errors of the loaded packages. Packages default to
`./...`, and test files are included. It prints how many call sites it rewrote, by method, then:

- `note:` lines for rewrites worth a look: a method value that became a function literal (the
  receiver is bound when the value is taken, as before), a generated file it rewrote (update its
  generator too, or the next generation undoes the edit), and `//go:build ignore` files it skipped.
- `needs a hand edit:` lines for sites it leaves alone because a rewrite could change what the code
  does, or it cannot express one:
  - a terminal call where neither the stream nor the arguments that now come before it are
    provably free of side effects, such as `newStream().Collect(newCtx())` or `s().Collect(<-ctxs)`:
    the new argument order would run them in a different order. Assign one to a variable and rerun.
  - an interface, or a type assertion or switch to one, that a v0.5 `Stream` satisfied through a
    moved method (for example `interface{ Count(context.Context) (int, error) }`). A v0.6 `Stream`
    no longer satisfies it, at compile time or at run time.
  - multi-value arguments (`s.Page(paging())`), method expressions (`Stream[T].Collect`), methods
    promoted through an embedded `Stream`, local names shadowing the `stream` package, and a
    `FilterWithErAndCtx` method value.
  - receivers that don't type-check, and every file in the module that imports `stream` but was not
    migrated: excluded by build constraints (rerun with `-tags`, or `GOOS`/`GOARCH`), cgo, parse
    errors, outside the given patterns, or in a nested `vendor` directory.
  - `FindFirst()` or `FindLast()` called without a context: v0.4 code, which the v0.5 section below
    migrates by hand.

Exit status: 0 when nothing is left, 1 when `needs a hand edit:` lines remain, 2 on a usage or load
error. `go fix` cannot do this migration: its `//go:fix inline` analyzer skips methods of generic
types. If you group imports, run `goimports` afterwards, since the import it adds is not placed in a
group.

## v0.4 to v0.5: package `stream` without package `lazy`

**Why.** `Stream[T].FindFirst()` returned a `lazy.Lazy[T]`, so every `Stream[T]` in a program also
compiled all of `Lazy[T]`'s methods. Since v0.5, package `stream` does not import `lazy`, which cuts
the generic code a Stream element type costs by about a quarter (74 KB to 56 KB per type on the
`tools/compilesize` probe).

Absence is a `bool`, not an error: `FindFirst` and `FindLast` return `(T, bool, error)`, and an
empty stream gives `found == false, err == nil`. Where you relied on `Get` failing for an empty
stream, check `found` yourself. `tools/streamrewrite` does not do this part.

The "after" column uses the v0.6 call forms. On v0.5 itself the terminal operations are still
methods: `s.FindFirst(ctx)`, `s.IsEmpty(ctx)`.

| v0.4 | Without `lazy` | Lazy bridge, if you still want a `Lazy` |
|---|---|---|
| `s.FindFirst().Get(ctx)` | `v, found, err := stream.FindFirst(ctx, s)` | `lazy.FindFirst(s).Get(ctx)` |
| `s.FindFirst().GetOptional(ctx)` / `MustGetOptional()` | `v, found, err := stream.FindFirst(ctx, s)` | `lazy.FindFirst(s).GetOptional(ctx)` |
| `s.FindFirst().IsEmpty(ctx)` | `stream.IsEmpty(ctx, s)` | |
| `s.FindLast().Get(ctx)` | `v, found, err := stream.FindLast(ctx, s)` | `lazy.FindLast(s).Get(ctx)` |
| `stream.FindFirstAndLast(s).Get(ctx)` | `fl, found, err := stream.FindFirstAndLast(ctx, s)` | `lazy.FindFirstAndLast(s)` |
| `stream.FromLazy(l)` | one of the deferred sources below | `lazy.ToStream(l)` |
| `stream.ReduceLazy(s, init, f)` (and `…WithErr`, `…WithErrAndCtx`) | `func(ctx context.Context) (R, error) { return stream.Reduce(ctx, s, init, f) }` | `lazy.Reduce(s, init, f)` (and `lazy.ReduceWithErr`, `lazy.ReduceWithErrAndCtx`) |
| `stream.MaxLazy(s)` / `stream.MinLazy(s)` | `stream.Max(ctx, s)` / `stream.Min(ctx, s)` | `lazy.Max(s)` / `lazy.Min(s)` |
| `type stream.Reducer[S, T] func(Stream[S]) lazy.Lazy[T]` | `type stream.Reducer[S, T] func(ctx, Stream[S]) (T, error)` (the shape of `stream.Max`) | |

The bridges live in package `lazy` (which imports `stream`), keep the v0.4 semantics, including the
`no "first element" in an empty stream` error from `Get`, and cost the `Lazy` instantiation only
where they are used.

### Deferred sources

`stream.FromLazy` wrapped whichever deferred shape the `Lazy` held. Each shape has its own
constructor in `stream`, named after `FromSliceProvider`. The function runs when the stream is
materialized, once per materialization, with the materialization context.

| Deferred shape | v0.4 | v0.5 and later |
|---|---|---|
| slice | `stream.FromSliceProvider(f)` | unchanged: `stream.FromSliceProvider(func(ctx) ([]T, error))` |
| single value | `stream.FromLazy(lazy.NewLazy(f))` | `stream.FromValueProvider(func(ctx) (T, error))` |
| optional value | `stream.FromLazy(lazy.NewLazyOptional(f))` with `f` returning `(*T, error)` | `stream.FromOptionalProvider(func(ctx) (T, bool, error))` |
| stream | `stream.Concat(stream.FromLazy(lazy.NewLazy(f)))` | `stream.FromStreamProvider(func(ctx) (Stream[T], error))` |

`FromOptionalProvider` takes the shape `FindFirst` returns, so this is a stream of `s`'s first
element:

```go
first := stream.FromOptionalProvider(func(ctx context.Context) (Person, bool, error) {
    return stream.FindFirst(ctx, s)
})
```

Two behavioural differences:

- `FromLazy` evaluated its `Lazy` on the first materialization only and streamed nothing afterwards.
  The new sources (and `lazy.ToStream`) evaluate again on every materialization, like
  `FromSliceProvider`. `FromStreamProvider` runs its function when the stream is opened, and its
  error reaches the consumer wrapped (match it with `errors.Is`); `FromValueProvider` and
  `FromOptionalProvider` return the function's error as is.
- `FindFirst` returns as soon as it has read the first element, so a context cancelled after that
  element was read does not turn the result into `context.Canceled`.
