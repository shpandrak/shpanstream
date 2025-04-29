package stream

import (
	"context"
	"fmt"
)

type unsafeProviderBuilder struct {
	// by index of the stream, nil means that the stream is already opened
	orderedUntypedStreams []any
	// by index of the stream, nil means that the stream is not open (either not opened or already closed)
	orderedCloseFunctionsForStreamsThatAreOpen []func()

	// store the index of the streams that were opened in the order they were opened so that we can close them in reverse order
	streamOpenOrder []int
}

func addStreamUnsafe[T any](b *unsafeProviderBuilder, s Stream[T]) int {
	b.orderedUntypedStreams = append(b.orderedUntypedStreams, s)
	b.orderedCloseFunctionsForStreamsThatAreOpen = append(b.orderedCloseFunctionsForStreamsThatAreOpen, nil)
	return len(b.orderedUntypedStreams) - 1
}

func openSubStreamUnsafe[T any](ctx context.Context, b *unsafeProviderBuilder, idx int) (ProviderFunc[T], error) {
	if idx < 0 || idx >= len(b.orderedUntypedStreams) {
		return nil, fmt.Errorf("stream index out of range: %d;len=%d", idx, len(b.orderedUntypedStreams))
	}
	if b.orderedUntypedStreams[idx] == nil {
		return nil, fmt.Errorf("stream at index %d is already opened", idx)
	}
	stream, ok := b.orderedUntypedStreams[idx].(Stream[T])
	if !ok {
		return nil, fmt.Errorf("stream at index %d is not of type Stream[%T]", idx, b.orderedUntypedStreams[idx])
	}
	cancelFuncForOpenSubStream, err := doOpenStream[T](ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("error opening stream at index %d: %w", idx, err)
	}

	b.orderedCloseFunctionsForStreamsThatAreOpen[idx] = func() {
		doCloseSubStream(stream)
		cancelFuncForOpenSubStream()
	}

	// Marking that we have opened this stream, so that we don't open it again
	b.orderedUntypedStreams[idx] = nil
	// Store the index of the stream that was opened
	b.streamOpenOrder = append(b.streamOpenOrder, idx)

	return stream.provider, nil
}

func closeSubStreamUnsafe(b *unsafeProviderBuilder, idx int) error {
	if idx < 0 || idx >= len(b.orderedCloseFunctionsForStreamsThatAreOpen) {
		return fmt.Errorf("stream index out of range: %d;len=%d", idx, len(b.orderedCloseFunctionsForStreamsThatAreOpen))
	}
	if b.orderedUntypedStreams[idx] != nil {
		return fmt.Errorf("stream at index %d is not opened", idx)
	}

	// Close the stream, if already closed, do nothing, this is ok
	if b.orderedCloseFunctionsForStreamsThatAreOpen[idx] != nil {
		b.orderedCloseFunctionsForStreamsThatAreOpen[idx]()
		b.orderedCloseFunctionsForStreamsThatAreOpen[idx] = nil
	}
	return nil
}

func newUnsafeStream[T any](
	b *unsafeProviderBuilder,
	optOpenFunc func(ctx context.Context, b *unsafeProviderBuilder) error,
	emitFunc func(ctx context.Context, b *unsafeProviderBuilder) (T, error),
	optCloseFunc func(),
) Stream[T] {
	closeFunc := func() {
		// Close all streams that were left open (reversed order)
		for i := len(b.streamOpenOrder) - 1; i >= 0; i-- {
			cf := b.orderedCloseFunctionsForStreamsThatAreOpen[b.streamOpenOrder[i]]
			if cf != nil {
				cf()
			}
		}
		// Close the unsafe stream itself using the closeFunc
		if optCloseFunc != nil {
			optCloseFunc()
		}
	}
	return newStream(
		func(ctx context.Context) (T, error) {
			return emitFunc(ctx, b)
		},
		[]Lifecycle{NewLifecycle(func(ctx context.Context) error {
			if optOpenFunc != nil {
				err := optOpenFunc(ctx, b)

				// If we fail to open a lifecycle, we need to call the closeFunc, so that we don't leak resources
				// open func in unsafe provider might include opening sub streams
				if err != nil {
					closeFunc()
				}
				return err
			}
			return nil
		},
			closeFunc,
		)},
	)
}
