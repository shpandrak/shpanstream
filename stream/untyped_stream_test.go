package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUntyped_BoxesElementsAndKeepsLifecycle(t *testing.T) {
	var order []string
	s := Untyped(WithAdditionalLifecycle(Just(1, 2, 3), NewLifecycle(
		func(ctx context.Context) error { order = append(order, "open"); return nil },
		func() { order = append(order, "close") },
	)))

	out, err := Collect(context.Background(), s)
	require.NoError(t, err)
	require.Equal(t, []any{1, 2, 3}, out)
	require.Equal(t, []string{"open", "close"}, order)

	// Re-consumable, like the typed stream it wraps.
	out, err = Collect(context.Background(), s)
	require.NoError(t, err)
	require.Equal(t, []any{1, 2, 3}, out)
}

func TestUntyped_NilAndErrorPassThrough(t *testing.T) {
	var p *int
	out, err := Collect(context.Background(), Untyped(Just(p, p)))
	require.NoError(t, err)
	require.Len(t, out, 2)
	for _, v := range out {
		require.Equal(t, p, v)
	}

	boom := errors.New("boom")
	_, err = Collect(context.Background(), Untyped(Error[int](boom)))
	require.ErrorIs(t, err, boom)

	var seen []any
	err = Untyped(MapWithErr(Just(1, 2, 3), func(v int) (int, error) {
		if v == 3 {
			return 0, boom
		}
		return v, nil
	})).Consume(context.Background(), func(v any) { seen = append(seen, v) })
	require.ErrorIs(t, err, boom)
	require.Equal(t, []any{1, 2}, seen)
}
