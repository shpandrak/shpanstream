package shpanstream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
	"sync"
)

type bufferedStreamProvider[T any] struct {
	src        Stream[T]
	bufferChan chan Entry[T, error]
	bufferSize int
	wg         sync.WaitGroup
}

// Buffer creates a buffered stream from the source stream with a given buffer size.
func (s Stream[T]) Buffer(size int) Stream[T] {
	return NewStream(&bufferedStreamProvider[T]{
		src:        s,
		bufferSize: size,
	})
}

func (b *bufferedStreamProvider[T]) Open(ctx context.Context) error {
	b.wg.Add(1)
	b.bufferChan = make(chan Entry[T, error], b.bufferSize)
	// Start Reading from the source stream
	go func() {
		defer b.wg.Done()
		err := b.src.Consume(ctx, func(v T) {
			// Write to the buffer channel
			select {
			case b.bufferChan <- Entry[T, error]{Key: v}:
			case <-ctx.Done():
			}
		})
		if err != nil {
			// Handle error by putting it in the buffer channel
			select {
			case b.bufferChan <- Entry[T, error]{Value: err}:
			case <-ctx.Done():
			}
		} else {
			// This means the source stream has finished, put EOF in the buffer channel
			select {
			case b.bufferChan <- Entry[T, error]{Value: io.EOF}:
			case <-ctx.Done():
			}
		}
	}()
	return nil

}

func (b *bufferedStreamProvider[T]) Close() {
	b.wg.Wait()
	close(b.bufferChan)
}

func (b *bufferedStreamProvider[T]) Emit(ctx context.Context) (T, error) {
	select {
	case <-ctx.Done():
		return util.DefaultValue[T](), ctx.Err()
	case entry, ok := <-b.bufferChan:
		if !ok {
			return util.DefaultValue[T](), fmt.Errorf("an attempt to read from streamed buffer, after it has already been closed")
		}
		return entry.Key, entry.Value
	}
}
