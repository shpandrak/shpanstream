package stream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"io"
)

// Buffered creates a buffered stream from the source stream with a given buffer size.
func Buffered[T any](s Stream[T], size int) Stream[T] {
	if size <= 0 {
		return ErrorStream[T](fmt.Errorf("buffer size must be greater than 0"))
	}
	if size == 1 {
		return s
	}

	// Create a buffered channel of type Result size-1
	// (-1 since one item will block while trying to write to the channel)
	// Result will either be T or an upstream error
	bufferChan := make(chan shpanstream.Result[T], size-1)

	return MapStreamWithErr(
		// Create a new stream with the buffer channel as the source
		StreamFromChannel(bufferChan),

		// Unpack the result from the buffer channel to the original type or error
		shpanstream.UnpackResult[T],
	).
		// Attach handler to the Open func of the stream lifecycle to trigger the buffering goroutine
		WithAdditionalStreamLifecycle(NewStreamLifecycle(
			func(ctx context.Context) error {

				// Start Reading from the source stream and populate the buffer channel
				go func() {
					// Make sure to close the buffer channel when either the source stream is done, or the context is cancelled
					defer close(bufferChan)

					err := s.Consume(ctx, func(v T) {
						// Write to the buffer channel
						select {
						case bufferChan <- shpanstream.Result[T]{Value: v}:
						case <-ctx.Done():
						}
					})

					// If an upstream error occurs, we need to send it to the buffer channel
					if err != nil {
						select {
						case bufferChan <- shpanstream.Result[T]{Err: err}:
						case <-ctx.Done():
						}
					} else {
						// If we are here, it means processing the source stream finished successfully,
						// "celebrating" it by putting an EOF in the buffer channel
						select {
						case bufferChan <- shpanstream.Result[T]{Err: io.EOF}:
						case <-ctx.Done():
						}
					}
				}()
				return nil
			},
			func() {
			},
		))
}
