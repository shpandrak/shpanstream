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
	return people().Filter(func(p Person) bool { return p.Age > 0 }).Skip(1).Limit(10).Collect(ctx)
}

// Comments between the links of a chain survive.
func Commented(ctx context.Context, mu *sync.Mutex) (int, error) {
	return people().
		// only adults
		Filter(func(p Person) bool { return p.Age >= 18 }).
		Limit(5). // at most five
		WithLockWhileMaterializing(mu).
		Count(ctx)
}

func Terminals(ctx context.Context) {
	s := people()
	_, _, _ = s.FindFirst(ctx)
	_, _, _ = s.FindLast(ctx)
	_, _ = s.IsEmpty(ctx)
	_, _ = s.CollectRandomSample(ctx, 3)
	_ = s.MustCount()
	_ = s.MustCollect()
	_ = s.Page(2, 10).RandomSample(4).Untyped().MustCollect()
}

func Pointer(ctx context.Context, p *stream.Stream[Person]) []Person {
	return p.Limit(1).MustCollect()
}

func Ranges() (n int) {
	for p := range people().Iterator {
		n += p.Age
	}
	for i, p := range people().IndexedIterator {
		n += i + p.Age
	}
	all := slices.Collect(people().Iterator)
	people().Iterator(func(Person) bool { return false })
	return n + len(all)
}

func Rename() stream.Stream[Person] {
	return people().FilterWithErAndCtx(func(ctx context.Context, p Person) (bool, error) { return true, nil })
}

// Method values become function literals.
func MethodValues(ctx context.Context) {
	s := people()
	first := s.FindFirst
	_, _, _ = first(ctx)
	run(s.MustCollect)
}

func run(f func() []Person) { f() }

// A method value binds its receiver when it is taken: the literal must too, unless the
// receiver is a local variable that is never reassigned.
func MethodValuesBound(ctx context.Context, p *stream.Stream[Person]) {
	s := people()
	count := s.Count
	s = stream.FromSlice([]Person{})
	_, _ = count(ctx)
	_ = s
	byPointer := p.MustCount
	fromCall := people().Limit(1).MustCollect
	_, _ = byPointer(), fromCall()
}

func mkctx() context.Context { return context.Background() }

// The context now comes before the stream. That is safe when either one is known to
// have no side effects.
func ArgOrder(ctx context.Context, ctxs map[string]context.Context) {
	s := people()
	_, _ = s.Collect(mkctx())
	_, _ = people().Collect(ctx)
	_, _ = people().Count(ctxs["a"])
	_, _ = people().Count(context.Background())
	_, _ = people().Limit(2).CollectRandomSample(context.TODO(), 1)
}
