package user

import (
	"context"
	"slices"
	"sync"

	"github.com/shpandrak/shpanstream/stream"
)

type Person struct {
	Name string
	Age  int
}

func people() stream.Stream[Person] {
	return stream.FromSlice([]Person{{"a", 1}})
}

// Chained calls, with the moved methods in the middle and at the end.
func Chain(ctx context.Context) ([]Person, error) {
	return stream.Collect(ctx, stream.Limit(stream.Skip(people().Filter(func(p Person) bool { return p.Age > 0 }), 1), 10))
}

// Comments between the links of a chain survive.
func Commented(ctx context.Context, mu *sync.Mutex) (int, error) {
	return stream.Count(ctx, stream.WithLockWhileMaterializing(stream.Limit(people().
		// only adults
		Filter(func(p Person) bool { return p.Age >= 18 }), 5), // at most five
		mu))
}

func Terminals(ctx context.Context) {
	s := people()
	_, _, _ = stream.FindFirst(ctx, s)
	_, _, _ = stream.FindLast(ctx, s)
	_, _ = stream.IsEmpty(ctx, s)
	_, _ = stream.CollectRandomSample(ctx, s, 3)
	_ = stream.MustCount(s)
	_ = stream.MustCollect(s)
	_ = stream.MustCollect(stream.Untyped(stream.RandomSample(stream.Page(s, 2, 10), 4)))
}

func Pointer(ctx context.Context, p *stream.Stream[Person]) []Person {
	return stream.MustCollect(stream.Limit(*p, 1))
}

func Ranges() (n int) {
	for p := range stream.MustValues(people()) {
		n += p.Age
	}
	for i, p := range stream.MustAll(people()) {
		n += i + p.Age
	}
	all := slices.Collect(stream.MustValues(people()))
	stream.MustValues(people())(func(Person) bool { return false })
	return n + len(all)
}

func Rename() stream.Stream[Person] {
	return people().FilterWithErrAndCtx(func(ctx context.Context, p Person) (bool, error) { return true, nil })
}

// Method values become function literals.
func MethodValues(ctx context.Context) {
	s := people()
	first := func(ctx context.Context) (Person, bool, error) { return stream.FindFirst(ctx, s) }
	_, _, _ = first(ctx)
	run(func() []Person { return stream.MustCollect(s) })
}

func run(f func() []Person) { f() }

// A method value binds its receiver when it is taken: the literal must too, unless the
// receiver is a local variable that is never reassigned.
func MethodValuesBound(ctx context.Context, p *stream.Stream[Person]) {
	s := people()
	count := func() func(ctx context.Context) (int, error) {
		r := s
		return func(ctx context.Context) (int, error) { return stream.Count(ctx, r) }
	}()
	s = stream.FromSlice([]Person{})
	_, _ = count(ctx)
	_ = s
	byPointer := func() func() int {
		r := *p
		return func() int { return stream.MustCount(r) }
	}()
	fromCall := func() func() []Person {
		r := stream.Limit(people(), 1)
		return func() []Person { return stream.MustCollect(r) }
	}()
	_, _ = byPointer(), fromCall()
}

func mkctx() context.Context { return context.Background() }

// The context now comes before the stream. That is safe when either one is known to
// have no side effects.
func ArgOrder(ctx context.Context, ctxs map[string]context.Context) {
	s := people()
	_, _ = stream.Collect(mkctx(), s)
	_, _ = stream.Collect(ctx, people())
	_, _ = stream.Count(ctxs["a"], people())
	_, _ = stream.Count(context.Background(), people())
	_, _ = stream.CollectRandomSample(context.TODO(), stream.Limit(people(), 2), 1)
}
