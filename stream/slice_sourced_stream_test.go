package stream

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJust_EmptySlice(t *testing.T) {
	s := Just[int]()
	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestJust_SingleElement(t *testing.T) {
	s := Just(42)
	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{42}, result)
}

func TestJust_MultipleElements(t *testing.T) {
	s := Just(1, 2, 3, 4, 5)
	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5}, result)
}

func TestFromSlice_EmptySlice(t *testing.T) {
	s := FromSlice([]int{})
	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestFromSlice_NilSlice(t *testing.T) {
	s := FromSlice([]int(nil))
	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestFromSlice_SingleElement(t *testing.T) {
	s := FromSlice([]int{42})
	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{42}, result)
}

func TestFromSlice_MultipleElements(t *testing.T) {
	s := FromSlice([]int{1, 2, 3, 4, 5})
	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5}, result)
}

// Test that the original slice is not modified
func TestFromSlice_DoesNotModifyOriginalSlice(t *testing.T) {
	original := []int{1, 2, 3, 4, 5}
	s := FromSlice(original)

	// Collect the stream
	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5}, result)

	// The original slice should be unchanged
	require.Equal(t, []int{1, 2, 3, 4, 5}, original)
}

// Test that modifying the original slice after creating the stream doesn't affect the stream
func TestFromSlice_IsolatedFromOriginalSliceChanges(t *testing.T) {
	original := []int{1, 2, 3, 4, 5}
	s := FromSlice(original)

	// Modify the original slice
	original[0] = 999
	original[4] = 888

	// Stream should still have the original values
	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5}, result)
}

// Test that the stream can be collected multiple times
func TestFromSlice_MultipleCollections(t *testing.T) {
	s := FromSlice([]int{1, 2, 3, 4, 5})

	// First collection
	result1, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5}, result1)

	// The second collection should work
	result2, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5}, result2)

	// The third collection should work
	result3, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5}, result3)
}

// Test that Just can be collected multiple times
func TestJust_MultipleCollections(t *testing.T) {
	s := Just(1, 2, 3, 4, 5)

	// First collection
	result1, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5}, result1)

	// The second collection should work
	result2, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5}, result2)
}

// Test context cancellation
func TestFromSlice_ContextCancellation(t *testing.T) {
	s := FromSlice([]int{1, 2, 3, 4, 5})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := s.Collect(ctx)
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
}

// Test that streams created with different slices are independent
func TestFromSlice_Independence(t *testing.T) {
	s1 := FromSlice([]int{1, 2, 3})
	s2 := FromSlice([]int{4, 5, 6})

	result1, err := s1.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, result1)

	result2, err := s2.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{4, 5, 6}, result2)

	// Collect s1 again to ensure it's still independent
	result1Again, err := s1.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, result1Again)
}

// Test with different data types
func TestJust_StringType(t *testing.T) {
	s := Just("hello", "world", "test")
	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"hello", "world", "test"}, result)
}

// Test with a struct type
func TestFromSlice_StructType(t *testing.T) {
	type testStruct struct {
		ID   int
		Name string
	}

	original := []testStruct{
		{ID: 1, Name: "Alice"},
		{ID: 2, Name: "Bob"},
	}

	s := FromSlice(original)
	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, original, result)

	// Verify independence
	original[0].Name = "Changed"
	result2, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Alice", result2[0].Name) // Should still be Alice
}

// Test with a pointer type
func TestFromSlice_PointerType(t *testing.T) {
	val1, val2, val3 := 1, 2, 3
	original := []*int{&val1, &val2, &val3}

	s := FromSlice(original)
	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 3)
	require.Equal(t, 1, *result[0])
	require.Equal(t, 2, *result[1])
	require.Equal(t, 3, *result[2])

	// The pointers should be the same (slice is cloned, but pointers within are shared)
	require.Same(t, original[0], result[0])
}

// Test that the provider function is invoked only when the stream is materialized, once per materialization
func TestFromSliceProvider_LazyAndReMaterializable(t *testing.T) {
	calls := 0
	s := FromSliceProvider(func(ctx context.Context) ([]int, error) {
		calls++
		return []int{1, 2, 3}, nil
	})
	require.Zero(t, calls, "provider should not be invoked before the stream is materialized")

	result, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, result)
	require.Equal(t, 1, calls)

	// The second collection should re-invoke the provider
	result, err = s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, result)
	require.Equal(t, 2, calls)
}

func TestFromSliceProvider_EmptyAndNilSlice(t *testing.T) {
	result, err := FromSliceProvider(func(ctx context.Context) ([]int, error) {
		return []int{}, nil
	}).Collect(context.Background())
	require.NoError(t, err)
	require.Empty(t, result)

	result, err = FromSliceProvider(func(ctx context.Context) ([]int, error) {
		return nil, nil
	}).Collect(context.Background())
	require.NoError(t, err)
	require.Empty(t, result)
}

// Test that an error returned by the provider is propagated to the consumer
func TestFromSliceProvider_ProviderError(t *testing.T) {
	providerErr := errors.New("provider failed")
	_, err := FromSliceProvider(func(ctx context.Context) ([]int, error) {
		return nil, providerErr
	}).Collect(context.Background())
	require.ErrorIs(t, err, providerErr)
}

// Test that the provider is actually invoked with the materialization context
func TestFromSliceProvider_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	var providerCtx context.Context
	_, err := FromSliceProvider(func(ctx context.Context) ([]int, error) {
		providerCtx = ctx
		return []int{1, 2, 3}, ctx.Err()
	}).Collect(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, providerCtx, "provider should have been invoked")
	require.ErrorIs(t, providerCtx.Err(), context.Canceled, "provider should receive the cancelled materialization context")
}

// Test that a partially consumed stream is re-materialized from the beginning
func TestFromSliceProvider_PartialConsumption(t *testing.T) {
	calls := 0
	s := FromSliceProvider(func(ctx context.Context) ([]int, error) {
		calls++
		return []int{1, 2, 3}, nil
	})

	result, err := s.Limit(2).Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, result)

	// The stream should start over, not resume where the partial consumption stopped
	result, err = s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, result)
	require.Equal(t, 2, calls)
}
