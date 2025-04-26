package shpanstream

import (
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

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
