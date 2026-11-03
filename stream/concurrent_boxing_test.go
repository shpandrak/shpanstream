package stream

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// The concurrent map and consume paths run over boxed `any` values (concurrent_core.go). These
// tests pin the boxing round trip for the element kinds where `any` is lossy if handled carelessly:
// interface elements holding nil, typed-nil pointers, struct values and zero-valued results.

type boxedElem struct {
	Id   string
	Rank int
}

func TestConcurrentMap_StructElementsRoundTrip(t *testing.T) {
	src := []boxedElem{{"a", 1}, {"b", 2}, {"c", 3}, {"d", 4}, {"e", 5}}
	out, err := Map(
		FromSlice(src),
		func(e boxedElem) boxedElem { return boxedElem{Id: e.Id + e.Id, Rank: e.Rank * 10} },
		WithConcurrentMapOption(3),
	).Collect(context.Background())
	require.NoError(t, err)
	sort.Slice(out, func(i, j int) bool { return out[i].Rank < out[j].Rank })
	require.Equal(t, []boxedElem{{"aa", 10}, {"bb", 20}, {"cc", 30}, {"dd", 40}, {"ee", 50}}, out)
}

func TestConcurrentMap_InterfaceElementsIncludingNil(t *testing.T) {
	// A nil `any` element must survive boxing as a nil element, not get dropped or panic on unbox.
	src := []any{nil, 1, "two", nil, 3.0}
	out, err := Map(
		FromSlice(src),
		func(v any) any { return v },
		WithConcurrentMapOption(2),
	).Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, out, len(src))
	nils := 0
	for _, v := range out {
		if v == nil {
			nils++
		}
	}
	require.Equal(t, 2, nils)
}

func TestConcurrentMap_TypedNilPointerElements(t *testing.T) {
	one := 1
	src := []*int{&one, nil, &one, nil}
	out, err := Map(
		FromSlice(src),
		func(p *int) *int { return p },
		WithConcurrentMapOption(2),
	).Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, out, 4)
	nils, ones := 0, 0
	for _, p := range out {
		if p == nil {
			nils++
		} else {
			require.Equal(t, 1, *p)
			ones++
		}
	}
	require.Equal(t, 2, nils)
	require.Equal(t, 2, ones)
}

func TestConcurrentMap_ZeroValuedResultsAreKept(t *testing.T) {
	out, err := Map(
		Just(1, 2, 3, 4),
		func(int) int { return 0 },
		WithConcurrentMapOption(2),
	).Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{0, 0, 0, 0}, out)
}

func TestConcurrentMap_SourceErrorSurfacedAndStreamClosed(t *testing.T) {
	boom := errors.New("source boom")
	var closed atomic.Int32
	src := NewSimpleStream(
		func(ctx context.Context) (int, error) { return 0, boom },
		WithCloseFuncOption(func() { closed.Add(1) }),
	)
	_, err := Map(src, func(v int) int { return v }, WithConcurrentMapOption(2)).Collect(context.Background())
	require.ErrorIs(t, err, boom)
	require.Equal(t, int32(1), closed.Load(), "the source must be closed exactly once after a source error")
}

func TestConcurrentMap_OpenFailureSurfaced(t *testing.T) {
	boom := errors.New("open boom")
	_, err := Map(Error[int](boom), func(v int) int { return v }, WithConcurrentMapOption(2)).Collect(context.Background())
	require.ErrorIs(t, err, boom)
}

func TestConcurrentMap_RejectsUnsupportedOption(t *testing.T) {
	_, err := MapWithErrAndCtx(Just(1), func(_ context.Context, v int) (int, error) { return v, nil }, unsupportedMapOption{}).
		Collect(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported map stream option type")
}

type unsupportedMapOption struct{}

func (unsupportedMapOption) mapStreamOptionName() string { return "unsupported" }

func TestConcurrentConsume_StructAndNilElementsRoundTrip(t *testing.T) {
	var sum atomic.Int64
	err := FromSlice([]boxedElem{{"a", 1}, {"b", 2}, {"c", 3}}).
		Consume(context.Background(), func(e boxedElem) { sum.Add(int64(e.Rank)) }, WithConcurrentConsumeOption(2))
	require.NoError(t, err)
	require.Equal(t, int64(6), sum.Load())

	var nils atomic.Int32
	err = FromSlice([]any{nil, "x", nil}).
		Consume(context.Background(), func(v any) {
			if v == nil {
				nils.Add(1)
			}
		}, WithConcurrentConsumeOption(2))
	require.NoError(t, err)
	require.Equal(t, int32(2), nils.Load())
}

func TestConcurrentConsume_SourceErrorSurfacedAndStreamClosed(t *testing.T) {
	boom := errors.New("source boom")
	var closed atomic.Int32
	emitted := 0
	src := NewSimpleStream(
		func(ctx context.Context) (int, error) {
			emitted++
			if emitted > 3 {
				return 0, boom
			}
			return emitted, nil
		},
		WithCloseFuncOption(func() { closed.Add(1) }),
	)
	err := src.Consume(context.Background(), func(int) {}, WithConcurrentConsumeOption(2))
	require.ErrorIs(t, err, boom)
	require.Equal(t, int32(1), closed.Load())
}

func TestConcurrentConsume_ReConsumable(t *testing.T) {
	s := Just(1, 2, 3, 4, 5)
	for i := 0; i < 3; i++ {
		var sum atomic.Int64
		err := s.Consume(context.Background(), func(v int) { sum.Add(int64(v)) }, WithConcurrentConsumeOption(3))
		require.NoError(t, err)
		require.Equal(t, int64(15), sum.Load())
	}
}

// A consumer panic on the concurrent consume path is recovered at the consumer boundary and
// reported as the returned error (the guard sits outside the fan-out, so this holds for panics
// unwinding out of doConsumeConcurrently itself; worker-goroutine panics are documented as not yet
// recovered in docs/shpanstream-concurrent-panic-followup.md).
func TestConcurrentConsume_OpenPanicRecoveredAsError(t *testing.T) {
	s := NewSimpleStream(
		func(ctx context.Context) (int, error) { return 1, nil },
		WithOpenFuncOption(func(ctx context.Context) error { panic("open panic") }),
	)
	err := s.Consume(context.Background(), func(int) {}, WithConcurrentConsumeOption(2))
	require.Error(t, err)
	require.Contains(t, err.Error(), "open panic")
}
