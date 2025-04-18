package shpanstream

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBasicStreamsYey(t *testing.T) {
	collect, err :=
		Just(1, 2, 3, 4, 5).
			Filter(func(i int) bool {
				return i > 2
			}).
			Collect(context.Background())

	require.NoError(t, err)
	require.Equal(t, 3, len(collect))

	collect, err =
		MapStream(Just(1, 2, 3, 4, 5), func(i int) int {
			return i + 1
		}).
			Filter(func(i int) bool {
				return i > 2
			}).
			Collect(context.Background())

	require.NoError(t, err)
	require.Equal(t, 4, len(collect))

}
