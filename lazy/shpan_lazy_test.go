package lazy

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// Behavioural pins for the Lazy API, written before Lazy became a thin generic shell over a
// non-generic core (lazy_core.go): the same tests must pass on both.

type lazyElem struct {
	Id   string `json:"id"`
	Rank int    `json:"rank"`
}

var ctx = context.Background()

func TestLazy_JustAndGet(t *testing.T) {
	l := Just(lazyElem{"a", 1})
	v, err := l.Get(ctx)
	require.NoError(t, err)
	require.Equal(t, lazyElem{"a", 1}, v)
	require.Equal(t, lazyElem{"a", 1}, l.MustGet())

	p, err := l.GetOptional(ctx)
	require.NoError(t, err)
	require.Equal(t, lazyElem{"a", 1}, *p)
	require.Equal(t, lazyElem{"a", 1}, *l.MustGetOptional())

	empty, err := l.IsEmpty(ctx)
	require.NoError(t, err)
	require.False(t, empty)
	require.False(t, l.MustIsEmpty())
}

func TestLazy_EmptyAndOptional(t *testing.T) {
	e := Empty[lazyElem]()
	_, err := e.Get(ctx)
	require.EqualError(t, err, "lazy value is empty")
	p, err := e.GetOptional(ctx)
	require.NoError(t, err)
	require.Nil(t, p)
	require.Nil(t, e.MustGetOptional())
	require.True(t, e.MustIsEmpty())
	require.Panics(t, func() { e.MustGet() })

	// JustOptional with a nil pointer is empty; with a value it is present.
	require.True(t, JustOptional[int](nil).MustIsEmpty())
	one := 1
	require.Equal(t, 1, JustOptional(&one).MustGet())

	// OrElseThrow replaces the empty error.
	custom := errors.New("custom empty")
	_, err = e.OrElseThrow(func() error { return custom }).Get(ctx)
	require.ErrorIs(t, err, custom)
	_, err = JustOptionalOrElseThrow[int](nil, func() error { return custom }).Get(ctx)
	require.ErrorIs(t, err, custom)
	_, err = NewLazyOptionalOrElseThrow(func(ctx context.Context) (*int, error) { return nil, nil }, func() error { return custom }).Get(ctx)
	require.ErrorIs(t, err, custom)
}

func TestLazy_Errors(t *testing.T) {
	boom := errors.New("boom")
	l := Error[int](boom)
	_, err := l.Get(ctx)
	require.ErrorIs(t, err, boom)
	_, err = l.GetOptional(ctx)
	require.ErrorIs(t, err, boom)
	_, err = l.IsEmpty(ctx)
	require.ErrorIs(t, err, boom)
	_, err = l.OrElse(ctx, 5)
	require.ErrorIs(t, err, boom)
	_, err = l.OrElseGet(ctx, func() int { return 5 })
	require.ErrorIs(t, err, boom)
	require.Panics(t, func() { l.MustGet() })
	require.Panics(t, func() { l.MustGetOptional() })
	require.Panics(t, func() { l.MustOrElse(1) })
	require.Panics(t, func() { l.MustOrElseGet(func() int { return 1 }) })
	require.Panics(t, func() { l.MustIsEmpty() })
	require.Panics(t, func() { l.MustConsume(func(int) {}) })

	// JustWithErr carries both a value and an error: the error wins for Get, both come out of GetOptional.
	jw := JustWithErr(7, boom)
	_, err = jw.Get(ctx)
	require.ErrorIs(t, err, boom)
	p, err := jw.GetOptional(ctx)
	require.ErrorIs(t, err, boom)
	require.NotNil(t, p)
	require.Equal(t, 7, *p)

	seven := 7
	p, err = JustOptionalWithErr(&seven, boom).GetOptional(ctx)
	require.ErrorIs(t, err, boom)
	require.Equal(t, 7, *p)
	p, err = JustOptionalWithErr[int](nil, boom).GetOptional(ctx)
	require.ErrorIs(t, err, boom)
	require.Nil(t, p)

	// NewLazy: the fetcher's error is passed through, a value is wrapped as present.
	_, err = NewLazy(func(ctx context.Context) (int, error) { return 0, boom }).Get(ctx)
	require.ErrorIs(t, err, boom)
	require.Equal(t, 3, NewLazy(func(ctx context.Context) (int, error) { return 3, nil }).MustGet())
}

func TestLazy_IsDeferredAndRefetched(t *testing.T) {
	calls := 0
	l := NewLazy(func(ctx context.Context) (int, error) { calls++; return calls, nil })
	require.Equal(t, 0, calls)
	require.Equal(t, 1, l.MustGet())
	require.Equal(t, 2, l.MustGet())
	require.Equal(t, 2, calls)
}

func TestLazy_OrElseFamily(t *testing.T) {
	v, err := Empty[int]().OrElse(ctx, 9)
	require.NoError(t, err)
	require.Equal(t, 9, v)
	v, err = Just(1).OrElse(ctx, 9)
	require.NoError(t, err)
	require.Equal(t, 1, v)
	require.Equal(t, 9, Empty[int]().MustOrElse(9))
	require.Equal(t, 1, Just(1).MustOrElse(9))

	v, err = Empty[int]().OrElseGet(ctx, func() int { return 8 })
	require.NoError(t, err)
	require.Equal(t, 8, v)
	require.Equal(t, 1, Just(1).MustOrElseGet(func() int { return 8 }))
	require.Equal(t, 8, Empty[int]().MustOrElseGet(func() int { return 8 }))

	// Or: the first present value wins; an error in the first short-circuits; the alternative's
	// empty error supplier applies.
	require.Equal(t, 1, Just(1).Or(Just(2)).MustGet())
	require.Equal(t, 2, Empty[int]().Or(Just(2)).MustGet())
	boom := errors.New("boom")
	_, err = Error[int](boom).Or(Just(2)).Get(ctx)
	require.ErrorIs(t, err, boom)
	custom := errors.New("custom")
	_, err = Empty[int]().Or(Empty[int]().OrElseThrow(func() error { return custom })).Get(ctx)
	require.ErrorIs(t, err, custom)
}

func TestLazy_Filter(t *testing.T) {
	l := Just(lazyElem{"a", 3})
	require.Equal(t, 3, l.Filter(func(e lazyElem) bool { return e.Rank > 2 }).MustGet().Rank)
	require.True(t, l.Filter(func(e lazyElem) bool { return e.Rank > 5 }).MustIsEmpty())

	// Empty is never filtered (the predicate must not run), the empty error supplier is kept.
	custom := errors.New("custom")
	calls := 0
	_, err := Empty[lazyElem]().OrElseThrow(func() error { return custom }).
		Filter(func(lazyElem) bool { calls++; return true }).Get(ctx)
	require.ErrorIs(t, err, custom)
	require.Equal(t, 0, calls)

	boom := errors.New("boom")
	_, err = l.FilterWithErr(func(lazyElem) (bool, error) { return false, boom }).Get(ctx)
	require.ErrorIs(t, err, boom)
	ok := l.FilterWithErrAndCtx(func(ctx context.Context, e lazyElem) (bool, error) { return e.Id == "a", nil })
	require.Equal(t, "a", ok.MustGet().Id)
}

func TestLazy_MapFamily(t *testing.T) {
	l := Just(lazyElem{"a", 3})
	require.Equal(t, "a:3", Map(l, func(e lazyElem) string { return e.Id + ":3" }).MustGet())
	require.True(t, Map(Empty[lazyElem](), func(e lazyElem) string { return e.Id }).MustIsEmpty())

	boom := errors.New("boom")
	_, err := MapWithErr(l, func(e lazyElem) (string, error) { return "", boom }).Get(ctx)
	require.ErrorIs(t, err, boom)
	_, err = Map(Error[lazyElem](boom), func(e lazyElem) string { return e.Id }).Get(ctx)
	require.ErrorIs(t, err, boom)
	require.Equal(t, 3, MapWithErrAndCtx(l, func(ctx context.Context, e lazyElem) (int, error) { return e.Rank, nil }).MustGet())

	// The empty error supplier is inherited from the source.
	custom := errors.New("custom")
	_, err = Map(Empty[int]().OrElseThrow(func() error { return custom }), func(int) int { return 1 }).Get(ctx)
	require.ErrorIs(t, err, custom)

	// MapWhileFiltering: nil filters out, a zero-valued target is kept.
	require.True(t, MapWhileFiltering(l, func(lazyElem) *int { return nil }).MustIsEmpty())
	zero := 0
	require.Equal(t, 0, MapWhileFiltering(l, func(lazyElem) *int { return &zero }).MustGet())
	_, err = MapWhileFilteringWithErr(l, func(lazyElem) (*int, error) { return nil, boom }).Get(ctx)
	require.ErrorIs(t, err, boom)
	require.Equal(t, 5, MapWhileFilteringWithErrAndCtx(l, func(ctx context.Context, e lazyElem) (*int, error) {
		five := 5
		return &five, nil
	}).MustGet())

	// FlatMap flattens an inner Lazy, empty stays empty.
	require.Equal(t, 30, FlatMap(l, func(e lazyElem) Lazy[int] { return Just(e.Rank * 10) }).MustGet())
	require.True(t, FlatMap(l, func(e lazyElem) Lazy[int] { return Empty[int]() }).MustIsEmpty())
	require.True(t, FlatMap(Empty[lazyElem](), func(e lazyElem) Lazy[int] { return Just(1) }).MustIsEmpty())
	_, err = FlatMap(l, func(e lazyElem) Lazy[int] { return Error[int](boom) }).Get(ctx)
	require.ErrorIs(t, err, boom)
}

func TestLazy_Consume(t *testing.T) {
	var got []lazyElem
	require.NoError(t, Just(lazyElem{"a", 1}).Consume(ctx, func(e lazyElem) { got = append(got, e) }))
	require.Equal(t, []lazyElem{{"a", 1}}, got)
	Just(lazyElem{"b", 2}).MustConsume(func(e lazyElem) { got = append(got, e) })
	require.Len(t, got, 2)

	calls := 0
	require.NoError(t, Empty[lazyElem]().Consume(ctx, func(lazyElem) { calls++ }))
	require.Equal(t, 0, calls)

	boom := errors.New("boom")
	require.ErrorIs(t, Just(1).ConsumeWithErr(ctx, func(int) error { return boom }), boom)
	require.ErrorIs(t, Error[int](boom).ConsumeWithErr(ctx, func(int) error { return nil }), boom)
	require.NoError(t, Just(1).ConsumeWithErrAndCtx(ctx, func(ctx context.Context, v int) error {
		require.Equal(t, 1, v)
		return nil
	}))
}

func TestLazy_JSON(t *testing.T) {
	data, err := json.Marshal(Just(lazyElem{"a", 1}))
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"a","rank":1}`, string(data))

	data, err = json.Marshal(Empty[lazyElem]())
	require.NoError(t, err)
	require.Equal(t, "null", string(data))

	boom := errors.New("boom")
	_, err = json.Marshal(Error[lazyElem](boom))
	require.ErrorIs(t, err, boom)

	// Marshalling as a struct field goes through the value method as well.
	type holder struct {
		L Lazy[lazyElem] `json:"l"`
	}
	data, err = json.Marshal(holder{L: Just(lazyElem{"b", 2})})
	require.NoError(t, err)
	require.JSONEq(t, `{"l":{"id":"b","rank":2}}`, string(data))

	// UnmarshalJSON is declared on the value receiver, so (as before) it validates the payload but
	// the receiver copy it updates is discarded by the caller.
	var l Lazy[lazyElem]
	require.NoError(t, l.UnmarshalJSON([]byte(`{"id":"c","rank":3}`)))
	require.NoError(t, l.UnmarshalJSON([]byte(`null`)))
	require.Error(t, l.UnmarshalJSON([]byte(`{"rank":"not a number"}`)))
}
