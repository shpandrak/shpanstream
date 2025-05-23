package stream

import (
	"context"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestStreamPage(t *testing.T) {
	// Helper function to create a stream of integers from 1 to n
	createNumberStream := func(n int) Stream[int] {
		count := 0
		return newStream[int](func(ctx context.Context) (int, error) {
			if count >= n {
				return 0, io.EOF
			}
			count++
			return count, nil
		}, nil)
	}

	tests := []struct {
		name       string
		streamSize int
		pageNum    int
		pageSize   int
		expected   []int
	}{
		{
			name:       "Page 0 (first page) with valid size",
			streamSize: 10,
			pageNum:    0,
			pageSize:   3,
			expected:   []int{1, 2, 3},
		},
		{
			name:       "Page 1 (second page) with valid size",
			streamSize: 10,
			pageNum:    1,
			pageSize:   3,
			expected:   []int{4, 5, 6},
		},
		{
			name:       "Page 2 (third page) with valid size",
			streamSize: 10,
			pageNum:    2,
			pageSize:   3,
			expected:   []int{7, 8, 9},
		},
		{
			name:       "Last partial page",
			streamSize: 10,
			pageNum:    3,
			pageSize:   3,
			expected:   []int{10},
		},
		{
			name:       "Page beyond available data",
			streamSize: 5,
			pageNum:    10,
			pageSize:   3,
			expected:   []int{},
		},
		{
			name:       "Negative page number",
			streamSize: 10,
			pageNum:    -1,
			pageSize:   3,
			expected:   []int{},
		},
		{
			name:       "Zero page size",
			streamSize: 10,
			pageNum:    0,
			pageSize:   0,
			expected:   []int{},
		},
		{
			name:       "Negative page size",
			streamSize: 10,
			pageNum:    0,
			pageSize:   -5,
			expected:   []int{},
		},
		{
			name:       "Large page size",
			streamSize: 5,
			pageNum:    0,
			pageSize:   100,
			expected:   []int{1, 2, 3, 4, 5},
		},
		{
			name:       "Empty stream pagination",
			streamSize: 0,
			pageNum:    0,
			pageSize:   5,
			expected:   []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := createNumberStream(tt.streamSize).Page(tt.pageNum, tt.pageSize).MustCollect()
			if len(tt.expected) == 0 {
				require.Empty(t, result)
			} else {
				require.EqualValues(t, tt.expected, result)
			}
		})
	}

	// Single element pages test (kept separate as it has a loop)
	t.Run("Single element pages", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			result := createNumberStream(5).Page(i, 1).MustCollect()
			require.EqualValues(t, []int{i + 1}, result)
		}
	})
}
