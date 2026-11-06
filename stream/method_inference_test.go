package stream

import (
	"context"
	"fmt"
	"testing"

	"github.com/shpandrak/shpanstream"
	"github.com/stretchr/testify/require"
)

// These tests pin that the generic methods infer their function type parameter from every common
// kind of argument, so call sites read like plain method calls: no type argument is ever spelled.

func isEvenTopLevel(v int) bool { return v%2 == 0 }

var collectedByTopLevel []int

func collectTopLevel(v int) { collectedByTopLevel = append(collectedByTopLevel, v) }

func TestFilter_InfersFunctionType(t *testing.T) {
	src := Just(1, 2, 3, 4)

	// Func literal.
	require.Equal(t, []int{2, 4}, MustCollect(src.Filter(func(v int) bool { return v%2 == 0 })))

	// Named shpanstream.Predicate[T] variable.
	var isEven shpanstream.Predicate[int] = func(v int) bool { return v%2 == 0 }
	require.Equal(t, []int{2, 4}, MustCollect(src.Filter(isEven)))

	// Top-level func.
	require.Equal(t, []int{2, 4}, MustCollect(src.Filter(isEvenTopLevel)))
}

func TestConsume_InfersFunctionType(t *testing.T) {
	ctx := context.Background()
	src := Just(1, 2, 3)

	// Func literal.
	var got []int
	require.NoError(t, src.Consume(ctx, func(v int) { got = append(got, v) }))
	require.Equal(t, []int{1, 2, 3}, got)

	// Named function-typed variable.
	got = nil
	type consumer func(int)
	var c consumer = func(v int) { got = append(got, v) }
	require.NoError(t, src.Consume(ctx, c))
	require.Equal(t, []int{1, 2, 3}, got)

	// Top-level func.
	collectedByTopLevel = nil
	require.NoError(t, src.Consume(ctx, collectTopLevel))
	require.Equal(t, []int{1, 2, 3}, collectedByTopLevel)
}

func TestMapMethods_AcceptNamedFuncTypes(t *testing.T) {
	ctx := context.Background()
	src := Just(1, 2)

	type toStr func(int) string
	var f toStr = func(v int) string { return fmt.Sprint(v) }
	require.Equal(t, []string{"1", "2"}, MustCollect(src.Map(f)))

	type toStrWithErr func(int) (string, error)
	var fe toStrWithErr = func(v int) (string, error) { return fmt.Sprint(v), nil }
	require.Equal(t, []string{"1", "2"}, MustCollect(src.MapWithErr(fe)))

	type toStrWithErrAndCtx func(context.Context, int) (string, error)
	var fc toStrWithErrAndCtx = func(_ context.Context, v int) (string, error) { return fmt.Sprint(v), nil }
	res, err := Collect(ctx, src.MapWithErrAndCtx(fc))
	require.NoError(t, err)
	require.Equal(t, []string{"1", "2"}, res)

	type toStream func(int) Stream[string]
	var fs toStream = func(v int) Stream[string] { return Just(fmt.Sprint(v), fmt.Sprint(v)) }
	require.Equal(t, []string{"1", "1", "2", "2"}, MustCollect(src.FlatMap(fs)))
}
