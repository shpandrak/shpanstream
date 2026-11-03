package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUntyped_BoxesElementsAndKeepsLifecycle(t *testing.T) {
	var order []string
	s := Just(1, 2, 3).
		WithAdditionalLifecycle(NewLifecycle(
			func(ctx context.Context) error { order = append(order, "open"); return nil },
			func() { order = append(order, "close") },
		)).
		Untyped()

	out, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []any{1, 2, 3}, out)
	require.Equal(t, []string{"open", "close"}, order)

	// Re-consumable, like the typed stream it wraps.
	out, err = s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []any{1, 2, 3}, out)
}

func TestUntyped_NilAndErrorPassThrough(t *testing.T) {
	var p *int
	out, err := Just(p, p).Untyped().Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, out, 2)
	for _, v := range out {
		require.Equal(t, p, v)
	}

	boom := errors.New("boom")
	_, err = Error[int](boom).Untyped().Collect(context.Background())
	require.ErrorIs(t, err, boom)

	var seen []any
	err = MapWithErr(Just(1, 2, 3), func(v int) (int, error) {
		if v == 3 {
			return 0, boom
		}
		return v, nil
	}).Untyped().Consume(context.Background(), func(v any) { seen = append(seen, v) })
	require.ErrorIs(t, err, boom)
	require.Equal(t, []any{1, 2}, seen)
}
