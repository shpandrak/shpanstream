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
		return Error[T](fmt.Errorf("buffer size must be greater than 0"))
	}
	if size == 1 {
		return s
	}

	// Create a buffered channel of type Result size-1
	// (-1 since one item will block while trying to write to the channel)
	// Result will either be T or an upstream error
	bufferChan := make(chan shpanstream.Result[T], size-1)

	// internalCtx bounds the buffering goroutine so close can cancel and join it. The join must be
	// driven by our own cancel: the consumer's teardown runs lifecycle Close funcs BEFORE cancelling
	// the consume ctx, so waiting on the outer ctx here would deadlock.
	var internalCancel context.CancelFunc
	bufferingDone := make(chan struct{})

	return MapWithErr(
		// Create a new stream with the buffer channel as the source
		FromChannel(bufferChan),

		// Unpack the result from the buffer channel to the original type or error
		shpanstream.UnpackResult[T],
	).
		// Attach handler to the Open func of the stream lifecycle to trigger the buffering goroutine
		WithAdditionalLifecycle(NewLifecycle(
			func(ctx context.Context) error {
				internalCtx, cancel := context.WithCancel(ctx)
				internalCancel = cancel

				// Start Reading from the source stream and populate the buffer channel
				go func() {
					// bufferingDone is closed last (LIFO), after the source's Consume has fully torn
					// down, so a close() blocked on <-bufferingDone is released only once the source
					// is closed.
					defer close(bufferingDone)
					// Make sure to close the buffer channel when either the source stream is done, or the context is cancelled
					defer close(bufferChan)

					err := s.Consume(internalCtx, func(v T) {
						// Write to the buffer channel
						select {
						case bufferChan <- shpanstream.Result[T]{Value: v}:
						case <-internalCtx.Done():
						}
					})

					// If an upstream error occurs, we need to send it to the buffer channel
					if err != nil {
						select {
						case bufferChan <- shpanstream.Result[T]{Err: err}:
						case <-internalCtx.Done():
						}
					} else {
						// If we are here, it means processing the source stream finished successfully,
						// "celebrating" it by putting an EOF in the buffer channel
						select {
						case bufferChan <- shpanstream.Result[T]{Err: io.EOF}:
						case <-internalCtx.Done():
						}
					}
				}()
				return nil
			},
			func() {
				// Cancel the buffering goroutine and join it, so the source is fully closed (its
				// Consume returned) by the time the stream reports closed. Without the join the
				// goroutine outlives Consume and the source teardown races with the caller.
				if internalCancel != nil {
					internalCancel()
					<-bufferingDone
				}
			},
		))
}
