# compilesize

Measures what a Stream element type costs the compiler: `measure.sh [N]`
generates N distinct struct types used the way an application repository uses
a Stream (slice source, `Map` to an `Entry`, `Filter`, `Collect`,
`FindFirst`), compiles the package against the working copy (`go.mod`
replaces the module with `../..`) and prints compile seconds, the bytes of
generic instantiations (`go.shape` symbols) and the per-type cost, then the
bytes by family. Run it with the same N before and after a change. The
finding it reproduces: `docs/compile-size-per-element-type.md`.

It is its own module so `go build ./...` at the repo root never compiles the
probe; `probe/probe_gen.go` is generated and gitignored.
