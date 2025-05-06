package stream

import (
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/lazy"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestLeftJoinSortedStreams(t *testing.T) {

	type testCase struct {
		name        string
		leftStream  Stream[int]
		rightStream Stream[int]
		want        Stream[shpanstream.Tuple2[int, *int]]
	}
	tests := []testCase{
		{
			name:        "empty streams",
			leftStream:  Empty[int](),
			rightStream: Empty[int](),
			want:        Empty[shpanstream.Tuple2[int, *int]](),
		},
		{
			name:        "left stream empty",
			leftStream:  Empty[int](),
			rightStream: Just(1, 2, 3),
			want:        Empty[shpanstream.Tuple2[int, *int]](),
		},
		{
			name:        "right stream empty",
			leftStream:  Just(1, 2, 3),
			rightStream: Empty[int](),
			want: Just(
				shpanstream.Tuple2[int, *int]{
					A: 1,
					B: nil,
				},
				shpanstream.Tuple2[int, *int]{
					A: 2,
					B: nil,
				},
				shpanstream.Tuple2[int, *int]{
					A: 3,
					B: nil,
				},
			),
		},
		{
			name:        "no common elements",
			leftStream:  Just(1, 2, 3),
			rightStream: Just(4, 5, 6),
			want: Just(
				shpanstream.Tuple2[int, *int]{
					A: 1,
					B: nil,
				},
				shpanstream.Tuple2[int, *int]{
					A: 2,
					B: nil,
				},
				shpanstream.Tuple2[int, *int]{
					A: 3,
					B: nil,
				},
			),
		},
		{
			name:        "common elements",
			leftStream:  Just(1, 2, 3),
			rightStream: Just(2, 3, 4),
			want: Just(
				shpanstream.Tuple2[int, *int]{
					A: 1,
					B: nil,
				},
				shpanstream.Tuple2[int, *int]{
					A: 2,
					B: util.Pointer(2),
				},
				shpanstream.Tuple2[int, *int]{
					A: 3,
					B: util.Pointer(3),
				},
			),
		},
		{
			name:        "common elements first starts higher",
			leftStream:  Just(2, 3, 4),
			rightStream: Just(1, 2, 3),
			want: Just[shpanstream.Tuple2[int, *int]](
				shpanstream.Tuple2[int, *int]{
					A: 2,
					B: util.Pointer(2),
				},
				shpanstream.Tuple2[int, *int]{
					A: 3,
					B: util.Pointer(3),
				},
				shpanstream.Tuple2[int, *int]{
					A: 4,
					B: nil,
				},
			),
		},
		{
			name:        "first and last match",
			leftStream:  Just(1, 2, 3),
			rightStream: Just(3, 4, 5),
			want: Just(
				shpanstream.Tuple2[int, *int]{
					A: 1,
					B: nil,
				},
				shpanstream.Tuple2[int, *int]{
					A: 2,
					B: nil,
				},
				shpanstream.Tuple2[int, *int]{
					A: 3,
					B: util.Pointer(3),
				},
			),
		},
		{
			name:        "first and last match 2",
			leftStream:  Just(3, 4, 5),
			rightStream: Just(1, 2, 3),
			want: Just(
				shpanstream.Tuple2[int, *int]{
					A: 3,
					B: util.Pointer(3),
				},
				shpanstream.Tuple2[int, *int]{
					A: 4,
					B: nil,
				},
				shpanstream.Tuple2[int, *int]{
					A: 5,
					B: nil,
				},
			),
		},
		{
			name:        "full match",
			leftStream:  Just(1, 2, 3),
			rightStream: Just(1, 2, 3),
			want: Just(
				shpanstream.Tuple2[int, *int]{
					A: 1,
					B: util.Pointer(1),
				},
				shpanstream.Tuple2[int, *int]{
					A: 2,
					B: util.Pointer(2),
				},
				shpanstream.Tuple2[int, *int]{
					A: 3,
					B: util.Pointer(3),
				},
			),
		},
		{
			name:        "full match right stream",
			leftStream:  Just(0, 1, 2, 3, 4),
			rightStream: Just(1, 2, 3),
			want: Just(
				shpanstream.Tuple2[int, *int]{
					A: 0,
					B: nil,
				},
				shpanstream.Tuple2[int, *int]{
					A: 1,
					B: util.Pointer(1),
				},
				shpanstream.Tuple2[int, *int]{
					A: 2,
					B: util.Pointer(2),
				},
				shpanstream.Tuple2[int, *int]{
					A: 3,
					B: util.Pointer(3),
				},
				shpanstream.Tuple2[int, *int]{
					A: 4,
					B: nil,
				},
			),
		},
		{
			name:        "full match left stream",
			leftStream:  Just(1, 2, 3),
			rightStream: Just(0, 1, 2, 3, 4),
			want: Just(
				shpanstream.Tuple2[int, *int]{
					A: 1,
					B: util.Pointer(1),
				},
				shpanstream.Tuple2[int, *int]{
					A: 2,
					B: util.Pointer(2),
				},
				shpanstream.Tuple2[int, *int]{
					A: 3,
					B: util.Pointer(3),
				},
			),
		},
	}

	mapperWithMinusOneForNil := func(v shpanstream.Tuple2[int, *int]) shpanstream.Tuple2[int, int] {
		return shpanstream.Tuple2[int, int]{
			A: v.A,
			B: lazy.JustOptional(v.B).MustOrElse(-1),
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			require.Equal(t, Map(
				tt.want,
				mapperWithMinusOneForNil,
			).MustCollect(), Map(
				LeftJoinSortedStreams(
					tt.leftStream,
					tt.rightStream,
					util.Identity[int](),
					util.Identity[int](),
					shpanstream.ComparatorForOrdered[int](),
				).
					// Make sure the left and right streams are equal or b is nil
					Peek(func(v shpanstream.Tuple2[int, *int]) {
						if v.B != nil {
							require.Equal(t, v.A, *v.B)
						}
					}),
				mapperWithMinusOneForNil,
			).MustCollect())

		})
	}

}
