package stream

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

// Regression: Result.Unpack / UnpackResult used to drop the error, which made Buffered append a
// spurious trailing zero (from the io.EOF sentinel mapped to (zero, nil)) and swallow upstream
// errors.
func TestBuffered_NoSpuriousTrailingValue(t *testing.T) {
	out, err := Buffered(Just(1, 2, 3, 4, 5), 3).Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4, 5}, out)
}

func TestBuffered_SurfacesUpstreamError(t *testing.T) {
	boom := errors.New("boom")
	_, err := Buffered(
		MapWithErr(Just(1, 2, 3, 4), func(i int) (int, error) {
			if i == 3 {
				return 0, boom
			}
			return i, nil
		}),
		2,
	).Collect(context.Background())
	require.ErrorIs(t, err, boom)
}

func TestBuffered_EmptyStream(t *testing.T) {
	out, err := Buffered(Empty[int](), 3).Collect(context.Background())
	require.NoError(t, err)
	require.Empty(t, out)
}

func TestStream_Buffered(t *testing.T) {
	iterationCounter := atomic.Int32{}
	actuallyReadCounter := atomic.Int32{}

	sourceChannel := Just(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).Peek(func(v int) {
		// Counting how many items are actually read from the source channel
		actuallyReadCounter.Add(1)
	})

	buffered := Buffered(sourceChannel, 3).Peek(func(v int) {
		// Counting how many items are read from the buffered channel
		iterationCounter.Add(1)
	})

	for idx := range buffered.IndexedIterator {
		// Allowing for the buffer to be filled
		time.Sleep(time.Millisecond * 1)
		require.EqualValues(t, idx+1, iterationCounter.Load())
		require.EqualValues(t, min(idx+4, 10), actuallyReadCounter.Load())
	}
}
