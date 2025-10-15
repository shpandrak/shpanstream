package stream

import (
	"github.com/stretchr/testify/require"
	"slices"
	"testing"
)

func TestFromIterator(t *testing.T) {
	slc := []int{1, 2, 3}
	values := slices.Values(slc)
	require.Equal(t, slc, FromIterator[int](values).MustCollect())
}
