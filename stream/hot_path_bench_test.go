package stream

import (
	"context"
	"testing"
)

// The hot-path benchmarks pin the per-element cost of the sequential pipeline, so a refactor of the
// Stream method bodies (see docs/compile-size-per-element-type.md) can be checked not to slow it down.
// Each element type is a struct, so any accidental boxing into `any` would show up as an allocation.

type benchElem struct {
	Id   string
	Rank int
	Tags []string // never set; it keeps the element a multi-word struct rather than something register-sized
}

func benchSource(n int) []benchElem {
	src := make([]benchElem, n)
	for i := range src {
		src[i] = benchElem{Id: "x", Rank: i}
	}
	return src
}

const benchN = 1024

func BenchmarkConsume(b *testing.B) {
	s := FromSlice(benchSource(benchN))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sum := 0
		if err := s.Consume(context.Background(), func(e benchElem) { sum += e.Rank }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMapFilterCollect(b *testing.B) {
	s := Map(FromSlice(benchSource(benchN)), func(e benchElem) int { return e.Rank }).
		Filter(func(r int) bool { return r%2 == 0 })
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out, err := s.Collect(context.Background())
		if err != nil || len(out) != benchN/2 {
			b.Fatal(err, len(out))
		}
	}
}

func BenchmarkLimitSkip(b *testing.B) {
	src := FromSlice(benchSource(benchN))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Limit and Skip keep their counters in the closure, so the chain is rebuilt per iteration.
		n, err := src.Skip(8).Limit(benchN - 16).Count(context.Background())
		if err != nil || n != benchN-16 {
			b.Fatal(err, n)
		}
	}
}

func BenchmarkDoFinally(b *testing.B) {
	s := FromSlice(benchSource(benchN)).DoFinally(func(error) {})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		n, err := s.Count(context.Background())
		if err != nil || n != benchN {
			b.Fatal(err, n)
		}
	}
}

func BenchmarkFindFirst(b *testing.B) {
	s := FromSlice(benchSource(benchN))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := s.FindFirst().Get(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
