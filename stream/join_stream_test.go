package stream

import (
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestJoinSortedStreams(t *testing.T) {

	type testCase struct {
		name        string
		leftStream  Stream[int]
		rightStream Stream[int]
		want        Stream[int]
	}
	tests := []testCase{
		{
			name:        "empty streams",
			leftStream:  Empty[int](),
			rightStream: Empty[int](),
			want:        Empty[int](),
		},
		{
			name:        "left stream empty",
			leftStream:  Empty[int](),
			rightStream: Just(1, 2, 3),
			want:        Empty[int](),
		},
		{
			name:        "right stream empty",
			leftStream:  Just(1, 2, 3),
			rightStream: Empty[int](),
			want:        Empty[int](),
		},
		{
			name:        "no common elements",
			leftStream:  Just(1, 2, 3),
			rightStream: Just(4, 5, 6),
			want:        Empty[int](),
		},
		{
			name:        "common elements",
			leftStream:  Just(1, 2, 3),
			rightStream: Just(2, 3, 4),
			want:        Just(2, 3),
		},
		{
			name:        "common elements first starts higher",
			leftStream:  Just(2, 3, 4),
			rightStream: Just(1, 2, 3),
			want:        Just(2, 3),
		},
		{
			name:        "first and last match",
			leftStream:  Just(1, 2, 3),
			rightStream: Just(3, 4, 5),
			want:        Just(3),
		},
		{
			name:        "first and last match 2",
			leftStream:  Just(3, 4, 5),
			rightStream: Just(1, 2, 3),
			want:        Just(3),
		},
		{
			name:        "full match",
			leftStream:  Just(1, 2, 3),
			rightStream: Just(1, 2, 3),
			want:        Just(1, 2, 3),
		},
		{
			name:        "full match right stream",
			leftStream:  Just(0, 1, 2, 3, 4),
			rightStream: Just(1, 2, 3),
			want:        Just(1, 2, 3),
		},
		{
			name:        "full match left stream",
			leftStream:  Just(1, 2, 3),
			rightStream: Just(0, 1, 2, 3, 4),
			want:        Just(1, 2, 3),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(
				t,
				tt.want.MustCollect(),
				Map(
					JoinSortedStreams(
						tt.leftStream,
						tt.rightStream,
						util.Identity[int](),
						util.Identity[int](),
						shpanstream.ComparatorForOrdered[int](),
					).
						// Make sure the left and right streams are equal
						Peek(func(v shpanstream.Tuple2[int, int]) {
							require.Equal(t, v.A, v.B)
						}),
					func(t shpanstream.Tuple2[int, int]) int {
						return t.A
					},
				).MustCollect(),
			)
		})
	}
}
