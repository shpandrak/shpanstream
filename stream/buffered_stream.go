package stream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

// Buffered creates a buffered stream from the source stream with a given buffer size.
// The resulting stream is re-consumable sequentially, but (like all streams) must not be consumed
// concurrently.
func Buffered[T any](s Stream[T], size int) Stream[T] {
	if size <= 0 {
		return Error[T](fmt.Errorf("buffer size must be greater than 0"))
	}
	if size == 1 {
		return s
	}
	b := &bufferedStreamProvider[T]{src: s, size: size}
	return NewSimpleStream(b.emit, WithOpenFuncOption(b.open), WithCloseFuncOption(b.close))
}

// bufferedStreamProvider drains the source on a background goroutine into a buffered channel. All
// channel state is created per Open (not once), so the stream is re-consumable (double collection).
type bufferedStreamProvider[T any] struct {
	src  Stream[T]
	size int

	// Per-consumption state, (re)initialised in open and torn down in close.
	bufferChan     chan shpanstream.Result[T]
	internalCancel context.CancelFunc
	bufferingDone  chan struct{}
	eofCtx         context.Context
	eofCancel      context.CancelFunc
}

func (b *bufferedStreamProvider[T]) open(ctx context.Context) error {
	// Buffer holds size-1 items (one more item blocks in the send while the buffer is full).
	bufferChan := make(chan shpanstream.Result[T], b.size-1)

	// internalCtx bounds the buffering goroutine so close can cancel and join it. The join must be
	// driven by our own cancel: the consumer's teardown runs lifecycle Close funcs BEFORE cancelling
	// the consume ctx, so waiting on the outer ctx here would deadlock.
	internalCtx, cancel := context.WithCancel(ctx)
	bufferingDone := make(chan struct{})

	// eofCtx lets emit distinguish "channel closed because the source reached EOF" from "closed on
	// cancel" (mirrors the concurrent map provider).
	eofCtx, eofCancel := context.WithCancel(context.Background())

	b.bufferChan = bufferChan
	b.internalCancel = cancel
	b.bufferingDone = bufferingDone
	b.eofCtx = eofCtx
	b.eofCancel = eofCancel

	go func() {
		// bufferingDone is closed last (LIFO), after the source's Consume has fully torn down, so a
		// close() blocked on <-bufferingDone is released only once the source is closed.
		defer close(bufferingDone)
		// Closing bufferChan signals normal completion (io.EOF) to the consumer side.
		defer close(bufferChan)

		err := b.src.Consume(internalCtx, func(v T) {
			select {
			case bufferChan <- shpanstream.Result[T]{Value: v}:
			case <-internalCtx.Done():
			}
		})
		// A real upstream error is forwarded through the buffer; normal completion (nil) is signalled
		// by cancelling eofCtx before the deferred close of bufferChan.
		if err != nil {
			select {
			case bufferChan <- shpanstream.Result[T]{Err: err}:
			case <-internalCtx.Done():
			}
		} else {
			eofCancel()
		}
	}()
	return nil
}

func (b *bufferedStreamProvider[T]) emit(ctx context.Context) (T, error) {
	select {
	case <-ctx.Done():
		return util.DefaultValue[T](), ctx.Err()
	case r, stillGood := <-b.bufferChan:
		if !stillGood {
			// The buffering goroutine closed the channel; eofCtx tells apart normal completion from
			// a close driven by cancellation (where the pending error may have been dropped).
			if b.eofCtx.Err() != nil {
				return util.DefaultValue[T](), io.EOF
			}
			if ctx.Err() != nil {
				return util.DefaultValue[T](), ctx.Err()
			}
			// Should never happen
			return util.DefaultValue[T](), fmt.Errorf("buffered stream channel closed prematurely")
		}
		return r.Unpack()
	}
}

func (b *bufferedStreamProvider[T]) close() {
	// Cancel the buffering goroutine and join it, so the source is fully closed (its Consume
	// returned) by the time the stream reports closed. Without the join the goroutine outlives
	// Consume and the source teardown races with the caller.
	if b.internalCancel != nil {
		b.internalCancel()
		<-b.bufferingDone
	}
	// On early termination the goroutine never signals EOF and eofCancel was never called; release
	// the eofCtx here (calling it twice is safe).
	if b.eofCancel != nil {
		b.eofCancel()
	}
}
